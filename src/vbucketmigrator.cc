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
#include <pthread.h>
#include <memcached/vbucket.h>

#include "sockstream.h"
#include "binarymessagepipe.h"
#include <libvbucket/vbucket.h>

#ifndef EX_SOFTWARE
#define EX_SOFTWARE 70
#endif

using namespace std;

static uint8_t verbosity(0);
static unsigned int timeout = 0;
static int exit_code = EX_OK;
static struct event timerev;
static bool evtimer_active(false);
static size_t moved = 0;

static size_t packets(0);

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
         << "\t-d host:port Send all vbuckets to this server" << endl
         << "\t-v           Increase verbosity" << endl
         << "\t-N name      Use a tap stream named \"name\"" << endl
         << "\t-T timeout   Terminate if nothing happened for timeout seconds" << endl
         << "\t-e           Run as an Erlang port" << endl;
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

void BinaryMessagePipeCallback::markcomplete() {
    if (evtimer_active) {
        evtimer_active = false;
        evtimer_del(&timerev);
    }
}

const int PENDING_SEND_LO_WAT = 128;
const int PENDING_SEND_HI_WAT = 512;

class UpstreamController {
public:
    UpstreamController() :
        upstream(0), pendingSendCount(0),
        closed(false), inputPlugged(false) {}
    void sendUpstreamMessage(BinaryMessage *msg) {
        upstream->sendMessage(msg);
    }
    void incrementPendingDownstream() {
        if (!upstream)
            return;

        pendingSendCount++;
        if (!inputPlugged && pendingSendCount > PENDING_SEND_HI_WAT) {
            upstream->plugInput();
            inputPlugged = true;
        }
    }
    void decrementPendingDownstream() {
        if (!upstream)
            return;

        pendingSendCount--;
        if (inputPlugged && pendingSendCount < PENDING_SEND_LO_WAT && !closed) {
            upstream->unPlugInput();
            inputPlugged = false;
        }
    }
    void close() {
        closed = true;
    }
    void setUpstream(BinaryMessagePipe *_upstream) {
        upstream = _upstream;
    }

    int getPendingSendCount() {
        return pendingSendCount;
    }

private:
    BinaryMessagePipe *upstream;
    int pendingSendCount;
    bool closed;
    bool inputPlugged;
};

class DownstreamBinaryMessagePipeCallback : public BinaryMessagePipeCallback {
public:
    DownstreamBinaryMessagePipeCallback(UpstreamController *_upstream) :
        upstream(_upstream) {}

    void messageReceived(BinaryMessage *msg) {
        upstream->sendUpstreamMessage(msg);
        if (verbosity > 1) {
            std::cout << "Received message from downstream server: "
                      << msg->toString() << std::endl;
        }
    }

    void messageSent(BinaryMessage *msg) {
        upstream->decrementPendingDownstream();

        if (msg->data.req->request.opcode == PROTOCOL_BINARY_CMD_TAP_VBUCKET_SET && (msg->size - sizeof(msg->data.vs->bytes)) >= sizeof(vbucket_state_t)) {
            // test the state thing..
            vbucket_state_t state;
            memcpy(&state, msg->data.rawBytes + sizeof(msg->data.vs->bytes),
                   sizeof(state));
            state = static_cast<vbucket_state_t>(ntohl(state));
            if (state == pending) {
                cout << "Starting to move bucket "
                     << msg->getVBucketId()
                     << endl;
                cout.flush();
            } else if (state == active) {
                ++moved;
                cout << "Bucket "
                     << msg->getVBucketId()
                     << " moved to the next server" << endl;
                cout.flush();
            }
        }
    }

    void abort() {
        // send a message upstream that we're aborting this tap stream
        markcomplete();
    }

    void shutdown() {
        markcomplete();
    }
private:
    UpstreamController *upstream;
};

class UpstreamBinaryMessagePipeCallback : public BinaryMessagePipeCallback {
public:
    UpstreamBinaryMessagePipeCallback(UpstreamController *_controller) :
        BinaryMessagePipeCallback(),
        controller(_controller) {}

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
                controller->incrementPendingDownstream();
                (*iter)->sendMessage(msg);
            }
        }
    }

    void setBucketmap(map<uint16_t, list<BinaryMessagePipe*> > &m) {
        bucketMap = &m;
    }

    void completeMe() {
        markcomplete();
        controller->close();
    }

    void abort() {
        map<uint16_t, list<BinaryMessagePipe*> >::iterator bucketIter;
        for (bucketIter = bucketMap->begin(); bucketIter != bucketMap->end(); ++bucketIter) {
            list<BinaryMessagePipe*>::iterator iter;
            for (iter = bucketIter->second.begin(); iter != bucketIter->second.end(); ++iter) {
                (*iter)->abort();
            }
        }
        completeMe();
    }

    void shutdown() {
        map<uint16_t, list<BinaryMessagePipe*> >::iterator bucketIter;
        for (bucketIter = bucketMap->begin(); bucketIter != bucketMap->end(); ++bucketIter) {
            list<BinaryMessagePipe*>::iterator iter;
            for (iter = bucketIter->second.begin(); iter != bucketIter->second.end(); ++iter) {
                (*iter)->plugInput();
                (*iter)->updateEvent();
            }
        }
        completeMe();
    }

private:
    UpstreamController *controller;
    map<uint16_t, list<BinaryMessagePipe*> > *bucketMap;
};

extern "C" {
    void event_handler(evutil_socket_t fd, short which, void *arg) {
        (void)fd;
        BinaryMessagePipe *pipe;
        pipe = reinterpret_cast<BinaryMessagePipe*>(arg);

        if (which == EV_TIMEOUT) {
            std::cerr << "Timed out on " << pipe->toString() << std::endl;
            _Exit(EXIT_FAILURE);
        }

        ++packets;

        try {
            pipe->step(which);
        } catch (std::exception& e) {
            cerr << e.what() << std::endl;
            pipe->abort();
            exit_code = EX_IOERR;
        }
        pipe->updateEvent();
    }

    static void timer_handler(evutil_socket_t fd, short which, void *arg) {
        (void)fd;
        (void)which;
        (void)arg;

        // A hack follows.
        static size_t previousPackets(0);
        static unsigned int numSame(0);

        if (previousPackets == packets) {
            if (timeout > 0 && ++numSame > timeout + 3) {
                // numSame == number of seconds the packet counter
                // hasn't moved, so this is an alternate view on
                // timeouts.  Added 3 to the timeout just to make this
                // more of a safety net and less of a race against the
                // normal timeout.
                std::cerr << "Safety net timed out" << std::endl;
                _Exit(EXIT_FAILURE);
            }
        } else {
            numSame = 0;
            previousPackets = packets;
        }

        // reschedule
        struct timeval tv = {1, 0};
        int event_add_rv = event_add(&timerev, &tv);
        assert(event_add_rv != -1);
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
        ret = new BinaryMessagePipe(*sock, cb, b, timeout);
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

extern "C" {

static void* check_stdin_thread(void* arg)
{
    struct event_base *evbase = (struct event_base*)arg;

    while (!feof(stdin)) {
        getc(stdin);
    }

    fprintf(stderr, "EOF on stdin.  Exiting\n");
    exit_code = EX_OSERR;
    event_base_loopbreak(evbase);
    return NULL;
}

static void stdin_check(struct event_base *evbase) {
    pthread_t t;
    pthread_attr_t attr;

    // Ask for a periodic timer to fire so we *can* actually break out
    // if something happens.
    evtimer_set(&timerev, timer_handler, NULL);
    event_base_set(evbase, &timerev);
    struct timeval tv = {1, 0};
    int event_add_rv = event_add(&timerev, &tv);
    evtimer_active = true;
    assert(event_add_rv != -1);

    if (pthread_attr_init(&attr) != 0 ||
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED) != 0 ||
        pthread_create(&t, &attr, check_stdin_thread, (void*)evbase) != 0)
    {
        perror("couldn't create stdin checking thread.");
        exit(EX_OSERR);
    }
}

}

int main(int argc, char **argv)
{
    int cmd;
    vector<uint16_t> buckets;
    const char *mapfile = NULL;
    string host;
    string destination;
    bool takeover = false;
    bool tapAck = false;
    bool erlang = false;
    string auth;
    string passwd;
    string name;

    while ((cmd = getopt(argc, argv, "N:Aa:h:b:m:d:tvT:e?")) != EOF) {
        switch (cmd) {
        case 'A':
            tapAck = true;
            break;
        case 'a':
            auth.assign(optarg);
            break;
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
        case 'e':
            erlang = true;
            break;
        case '?': /* FALLTHROUGH */
        default:
            usage(argv[0]);
        }
    }

    if (!auth.empty()) {
#ifdef ENABLE_SASL
        if (sasl_client_init(NULL) != SASL_OK) {
            fprintf(stderr, "Failed to initialize sasl library!\n");
            return EX_OSERR;
        }
        atexit(sasl_done);
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
#else
        fprintf(stderr, "Not built with SASL support\n");
        return EX_USAGE;
#endif
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

    if (erlang) {
        stdin_check(evbase);
    }

    UpstreamController controller;
    UpstreamBinaryMessagePipeCallback upstream(&controller);
    DownstreamBinaryMessagePipeCallback downstream(&controller);

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
    BinaryMessagePipe pipe(sock, upstream, evbase, timeout);
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
    controller.setUpstream(&pipe);

    event_base_loop(evbase, 0);

    if (takeover && moved != buckets.size()) {
        cerr << "Did not move enough vbuckets in takeover: "
             << moved << "/" << buckets.size() << endl;
        exit_code = exit_code == 0 ? EX_SOFTWARE : exit_code;
    }

    if (controller.getPendingSendCount() != 0) {
        cerr << "Had " << controller.getPendingSendCount()
             << " pending messages at exit." << endl;
        exit_code = exit_code == 0 ? EX_SOFTWARE : exit_code;
    }

    return exit_code;
}
