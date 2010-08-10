/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 NorthScale, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#include "config.h"

#include <stdio.h>
#include <unistd.h>
#include <iostream>
#include <vector>
#include <map>
#include <list>
#include <cstdlib>
#include <event.h>
#include <memcached/vbucket.h>

#include "sockstream.h"
#include "binarymessagepipe.h"
#include <libvbucket/vbucket.h>

using namespace std;

static uint8_t verbosity(0);
static unsigned int timeout = 0;

static void usage(std::string binary) {
    ssize_t idx = binary.find_last_of("/\\");
    if (idx != -1) {
        binary = binary.substr(idx + 1);
    }

    cerr << "Usage: " << binary
         << " -h host:port -b # [-m mapfile|-d desthost:destport]" << endl
         << "\t-h host:port Connect to host:port" << endl
         << "\t-A           Use TAP acks" << endl
         << "\t-t           Move buckets from a server to another server"<< endl
         << "\t-b #         Operate on bucket number #" << endl
         << "\t-m mapfile   The destination bucket map" << endl
         << "\t-a auth      Try to authenticate <auth>" << endl
         << "\t             (Password should be provided on standard input)" << endl
         << "\t-d host:port Send all vbuckets to this server" << endl
         << "\t-v           Increase verbosity" << endl
         << "\t-N name      Use a tap stream named \"name\"" << endl
         << "\t-T timeout   Terminate if nothing happend for timeout seconds" << endl;
    exit(EX_USAGE);
}

static uint16_t str2bucketid(const char *str)
{
    uint32_t val = atoi(str);
    if ((val & 0xffff0000) != 0) {
        std::string message = "Invalid bucket id: ";
        message.append(str);
        throw std::runtime_error(message);
    }

    return static_cast<uint16_t>(val);
}

class DownstreamBinaryMessagePipeCallback : public BinaryMessagePipeCallback {
public:
    void messageReceived(BinaryMessage *msg) {
        upstream->sendMessage(msg);
        if (verbosity > 1) {
            std::cout << "Received message from downstream server: "
                      << msg->toString() << std::endl;
        }
    }

    void setUpstream(BinaryMessagePipe &up) {
        upstream = &up;
    }

    void messageSent(BinaryMessage *msg) {
        if (msg->data.req->request.opcode == PROTOCOL_BINARY_CMD_TAP_VBUCKET_SET) {
            // test the state thing..
            uint16_t v = msg->data.vs->message.body.tap.flags;
            vbucket_state_t state = static_cast<vbucket_state_t>(ntohs(v));
            if (state == pending) {
                cout << "Starting to move bucket "
                     << msg->getVBucketId()
                     << endl;
                cout.flush();
            } else if (state == active) {
                cout << "Bucket "
                     << msg->getVBucketId()
                     << " moved to the next server" << endl;
                cout.flush();
            }
        }
    }

    void abort() {
        // send a message upstream that we're aborting this tap stream
    }

private:
    BinaryMessagePipe *upstream;
};

class UpstreamBinaryMessagePipeCallback : public BinaryMessagePipeCallback {
public:
    void messageReceived(BinaryMessage *msg) {
        if (verbosity > 1) {
            std::cout << "Received message from upstream server: "
                      << msg->toString() << std::endl;
        }

        map<uint16_t, list<BinaryMessagePipe*> >::iterator bucketIter;
        if (msg->data.req->request.opcode == PROTOCOL_BINARY_CMD_NOOP) {
            // Ignore NOOPs
            delete msg;
            return;
        }
        bucketIter = bucketMap->find(msg->getVBucketId());
        if (bucketIter == bucketMap->end()) {
            std::cerr << "Internal server error!!" << std::endl
                      << "Received a message for a bucket I didn't request:"
                      << msg->toString()
                      << std::endl;
            delete msg;
        } else {
            list<BinaryMessagePipe*>::iterator iter;
            for (iter = bucketIter->second.begin();
                 iter != bucketIter->second.end();
                 iter++) {
                (*iter)->sendMessage(msg);
            }
        }
    }

    void setBucketmap(map<uint16_t, list<BinaryMessagePipe*> > &m) {
        bucketMap = &m;
    }

    void abort() {
        map<uint16_t, list<BinaryMessagePipe*> >::iterator bucketIter;
        for (bucketIter = bucketMap->begin(); bucketIter != bucketMap->end(); ++bucketIter) {
            list<BinaryMessagePipe*>::iterator iter;
            for (iter = bucketIter->second.begin(); iter != bucketIter->second.end(); ++iter) {
                (*iter)->abort();
            }
        }
    }

    void shutdown() {
        map<uint16_t, list<BinaryMessagePipe*> >::iterator bucketIter;
        for (bucketIter = bucketMap->begin(); bucketIter != bucketMap->end(); ++bucketIter) {
            list<BinaryMessagePipe*>::iterator iter;
            for (iter = bucketIter->second.begin(); iter != bucketIter->second.end(); ++iter) {
                (*iter)->shutdownInput();
                (*iter)->updateEvent();
            }
        }
    }

private:
    map<uint16_t, list<BinaryMessagePipe*> > *bucketMap;
};

extern "C" {
    void event_handler(int fd, short which, void *arg) {
        (void)fd; (void)which;
        BinaryMessagePipe *pipe;
        pipe = reinterpret_cast<BinaryMessagePipe*>(arg);

        try {
            pipe->step(which);
        } catch (std::exception& e) {
            cerr << e.what() << std::endl;
            pipe->abort();
        }
        pipe->updateEvent();
        pipe->updateTimer(timeout);
    }

    void event_timeout_handler(int fd, short which, void *arg) {
        (void)fd; (void)which;
        BinaryMessagePipe *pipe;
        pipe = reinterpret_cast<BinaryMessagePipe*>(arg);

        std::cerr << "Timed out on " << pipe->toString() << std::endl;
        _Exit(EXIT_FAILURE);
    }
}

static BinaryMessagePipe *getServer(int serverindex,
                                    VBUCKET_CONFIG_HANDLE vbucket,
                                    const string &destination,
                                    uint16_t vbucketId,
                                    BinaryMessagePipeCallback &cb,
                                    struct event_base *b,
                                    const std::string &auth,
                                    const std::string &passwd)
{
    static map<int, BinaryMessagePipe*> servermap;
    BinaryMessagePipe* ret;

    map<int, BinaryMessagePipe*>::iterator server = servermap.find(serverindex);
    if (server == servermap.end()) {
        string host;
        if (vbucket != NULL) {
            host.assign(vbucket_config_get_server(vbucket, serverindex));
        } else {
            host.assign(destination);
        }
        Socket *sock = new Socket(host);
        if (verbosity) {
            cout << "Connecting to downstream " << *sock
                 << " for " << vbucketId << endl;
        }
        sock->connect();
        ret = new BinaryMessagePipe(*sock, cb, b);
        if (auth.length() > 0) {
            if (verbosity) {
                cout << "Authenticating towards: " << *sock << endl;
            }
            try {
                ret->authenticate(auth, passwd);
                if (verbosity) {
                    cout << "Authenticated towards: " << *sock << endl;
                }
            } catch (std::exception& e) {
                throw std::string(e.what());
            }
        }
        sock->setNonBlocking();
        ret->updateEvent();
        servermap[serverindex] = ret;
    } else {
        ret = server->second;
    }

    return ret;
}

#ifndef HAVE_GETPASS
static char *getpass(const char *prompt)
{
    static char buffer[1024];
    fprintf(stdout, "%s", prompt);
    fflush(stdout);

    if (fgets(buffer, sizeof(buffer), stdin) == NULL) {
        return NULL;
    }

    size_t len = strlen(buffer) - 1;
    while (buffer[len] == '\r' || buffer[len] == '\n') {
        buffer[len] = '\0';
        --len;
    }

    return buffer;
}
#endif

int main(int argc, char **argv)
{
    int cmd;
    vector<uint16_t> buckets;
    const char *mapfile = NULL;
    string host;
    string destination;
    bool takeover = false;
    bool tapAck = false;
    string auth;
    string passwd;
    string name;

    while ((cmd = getopt(argc, argv, "N:Aa:h:b:m:d:tvT:?")) != EOF) {
        switch (cmd) {
        case 'A':
            tapAck = true;
            break;
        case 'a':
#ifdef ENABLE_SASL
            if (sasl_client_init(NULL) != SASL_OK) {
                fprintf(stderr, "Failed to initialize sasl library!\n");
                return EX_OSERR;
            }
            atexit(sasl_done);
            auth.assign(optarg);
            if (isatty(fileno(stdin))) {
                char *pw = getpass("Enter password: ");
                if (pw == NULL) {
                    return EXIT_FAILURE;
                }
                passwd.assign(pw);
            } else {
                char buffer[1024];
                if (fgets(buffer, sizeof(buffer), stdin) == NULL) {
                    cout << "Missing password" << endl;
                    return EXIT_FAILURE;
                }
                passwd.assign(buffer);
                ssize_t p = passwd.find_first_of("\r\n");
                passwd.resize(p);
            }
            break;
#else
            fprintf(stderr, "Not built with SASL support\n");
            return EX_USAGE;
#endif
        case 'm':
            if (mapfile != NULL) {
                cerr << "Multiple mapfiles is not supported" << endl;
                return EX_USAGE;
            }
            break;
        case 'd':
            destination.assign(optarg);
            break;
        case 'h':
            host.assign(optarg);
            break;
        case 'b':
            try {
                buckets.push_back(str2bucketid(optarg));
            } catch (std::exception& e) {
                cerr << e.what() << std::endl;
                return EX_USAGE;
            }
            break;
        case 't':
            takeover = true;
            break;
        case 'v':
            ++verbosity;
            break;
        case 'N':
            name.assign(optarg);
            break;
        case 'T':
            timeout = atoi(optarg);
            break;
        case '?': /* FALLTHROUGH */
        default:
            usage(argv[0]);
        }
    }

    try {
        initialize_sockets();
    } catch (std::exception& e) {
        cerr << "Failed to initialize sockets: " << e.what() << std::endl;
        return EX_IOERR;
    }

    if (host.length() == 0) {
        cerr << "You need to specify the host to migrate data from"
             << endl;
        return EX_USAGE;
    }

    if (mapfile == NULL && destination.empty()) {
        cerr << "Can't perform bucket migration without a bucket map or destination host" << endl;
        return EX_USAGE;
    }

    if (buckets.empty()) {
        cerr << "Please specify the buckets to migrate by using -b" << endl;
        return EX_USAGE;
    }

    VBUCKET_CONFIG_HANDLE vbucket(NULL);
    if (mapfile != NULL) {
        if (!destination.empty()) {
            cerr << "Cannot specify both map and destination" << endl;
            return EX_USAGE;
        }
        vbucket = vbucket_config_parse_file(mapfile);
        if (vbucket == NULL) {
            const char *msg = vbucket_get_error();
            if (msg == NULL) {
                msg = "Unknown error";
            }
            cerr << "Failed to parse vbucket config: " << msg << endl;
            return EX_CONFIG;
        }
    }

    struct event_base *evbase = event_init();
    if (evbase == NULL) {
        cerr << "Failed to initialize libevent" << endl;
        return EX_IOERR;
    }

    UpstreamBinaryMessagePipeCallback upstream;
    DownstreamBinaryMessagePipeCallback downstream;

    map<int, BinaryMessagePipe*> servermap;
    map<uint16_t, list<BinaryMessagePipe*> > bucketMap;
    for (vector<uint16_t>::iterator iter = buckets.begin();
         iter != buckets.end();
         ++iter) {

        if (takeover) {
            int idx = 0;
            if (vbucket != NULL) {
                idx = vbucket_get_master(vbucket, *iter);
                if (idx == -1) {
                    cerr << "Failed to resolve bucket: " << *iter << endl;
                    return EX_CONFIG;
                }
            }
            BinaryMessagePipe* pipe;
            try {
                pipe = getServer(idx, vbucket, destination, *iter, downstream, evbase, auth, passwd);
            } catch (std::string& e) {
                cerr << "Failed to connect to host for bucket " << *iter
                     << ": " << e << std::endl;
                return EX_CONFIG;
            }
            bucketMap[*iter].push_back(pipe);
        } else if (vbucket != NULL) {
            int num  = vbucket_config_get_num_replicas(vbucket);
            for (int ii = 0; ii < num; ++ii) {
                int idx = 0;
                vbucket_get_replica(vbucket, *iter, ii);
                if (idx == -1) {
                    continue;
                }
                BinaryMessagePipe* pipe;
                try {
                    pipe = getServer(idx, vbucket, destination, *iter, downstream, evbase,
                                     auth, passwd);
                } catch (std::string& e) {
                    cerr << "Failed to connect to host for bucket " << *iter
                         << ": " << e << std::endl;
                    return EX_CONFIG;
                }
                bucketMap[*iter].push_back(pipe);
            }
        } else {
            BinaryMessagePipe* pipe;
            try {
                pipe = getServer(0, vbucket, destination, *iter, downstream, evbase, auth, passwd);
            } catch (std::string& e) {
                cerr << "Failed to connect to host for bucket " << *iter
                     << ": " << e << std::endl;
                return EX_CONFIG;
            }
            bucketMap[*iter].push_back(pipe);
        }
    }

    if (verbosity) {
        cout << "Connecting to source: " << host << endl;
    }

    if (vbucket) {
        vbucket_config_destroy(vbucket);
    }
    Socket sock(host);
    sock.connect();
    BinaryMessagePipe pipe(sock, upstream, evbase);
    if (auth.length() > 0) {
        if (verbosity) {
            cout << "Authenticating towards: " << sock << endl;
        }
        try {
            pipe.authenticate(auth, passwd);
            if (verbosity) {
                cout << "Authenticated towards: " << sock << endl;
            }
        } catch (std::exception &e) {
            cerr << "Failed to authenticate: " << e.what() << endl;
            return EX_CONFIG;
        }
    }
    sock.setNonBlocking();
    pipe.sendMessage(new TapRequestBinaryMessage(name, buckets, takeover, tapAck));
    pipe.updateEvent();
    upstream.setBucketmap(bucketMap);
    downstream.setUpstream(pipe);

    event_base_loop(evbase, 0);

    int ret = EX_OK;
    return ret;
}
