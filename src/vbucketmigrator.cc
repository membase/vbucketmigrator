/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Copyright (C) 2010 NorthScale, Inc
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in this directory for full text.
 */
#include "config.h"

#include <stdio.h>
#include <unistd.h>
#include <iostream>
#include <vector>
#include <map>
#include <list>
#include <event.h>
#include <memcached/vbucket.h>

#include "sockstream.h"
#include "binarymessagepipe.h"
#include <libvbucket/vbucket.h>

using namespace std;

static uint8_t verbosity(0);

static void usage(std::string binary) {
    ssize_t idx = binary.find_last_of("/\\");
    if (idx != -1) {
        binary = binary.substr(idx + 1);
    }

    cerr << "Usage: " << binary << " -h host:port -b # -m mapfile" << endl
         << "\t-h host:port Connect to host:port" << endl
         << "\t-t           Move buckets from a server to another server"<< endl
         << "\t-b #         Operate on bucket number #" << endl
         << "\t-m mapfile   The destination bucket map" << endl
         << "\t-v           Increase verbosity" << endl;
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
        map<uint16_t, list<BinaryMessagePipe*> >::iterator bucketIter;
        bucketIter = bucketMap->find(msg->getVBucketId());
        if (bucketIter == bucketMap->end()) {
            std::cerr << "Internal server error!!"
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
    }
}

static BinaryMessagePipe *getServer(int serverindex,
                                    VBUCKET_CONFIG_HANDLE vbucket,
                                    uint16_t vbucketId,
                                    BinaryMessagePipeCallback &cb,
                                    struct event_base *b)
{
    static map<int, BinaryMessagePipe*> servermap;
    BinaryMessagePipe* ret;

    map<int, BinaryMessagePipe*>::iterator server = servermap.find(serverindex);
    if (server == servermap.end()) {
        Socket *sock = new Socket(vbucket_config_get_server(vbucket,
                                                            serverindex));
        sock->connect();
        sock->setNonBlocking();
        if (verbosity) {
            cout << "Connecting to downstream " << *sock
                 << " for " << vbucketId << endl;
        }
        ret = new BinaryMessagePipe(*sock, cb, b);
        servermap[serverindex] = ret;
    } else {
        ret = server->second;
    }

    return ret;
}

int main(int argc, char **argv)
{
    int cmd;
    vector<uint16_t> buckets;
    const char *mapfile = NULL;
    string host;
    bool takeover = false;

    while ((cmd = getopt(argc, argv, "h:b:m:tv?")) != EOF) {
        switch (cmd) {
        case 'm':
            if (mapfile != NULL) {
                cerr << "Multiple mapfiles is not supported" << endl;
                return EX_USAGE;
            }
            mapfile = optarg;
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

    if (mapfile == NULL) {
        cerr << "Can't perform bucket migration without a bucket map" << endl;
        return EX_USAGE;
    }

    if (buckets.empty()) {
        cerr << "Please specify the buckets to migrate by using -b" << endl;
        return EX_USAGE;
    }

    VBUCKET_CONFIG_HANDLE vbucket = vbucket_config_parse_file(mapfile);
    if (vbucket == NULL) {
        const char *msg = vbucket_get_error();
        if (msg == NULL) {
            msg = "Unknown error";
        }
        cerr << "Failed to parse vbucket config: " << msg << endl;
        return EX_CONFIG;
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
            int idx = vbucket_get_master(vbucket, *iter);
            if (idx == -1) {
                cerr << "Failed to resolve bucket: " << *iter << endl;
                return EX_CONFIG;
            }
            BinaryMessagePipe* pipe;
            try {
                pipe = getServer(idx, vbucket, *iter, downstream, evbase);
            } catch (std::string& e) {
                cerr << "Failed to connect to host for bucket " << *iter
                     << ": " << e << std::endl;
                return EX_CONFIG;
            }
            bucketMap[*iter].push_back(pipe);
        } else {
            int num  = vbucket_config_get_num_replicas(vbucket);
            for (int ii = 0; ii < num; ++ii) {
                int idx = vbucket_get_replica(vbucket, *iter, ii);
                if (idx == -1) {
                    continue;
                }
                BinaryMessagePipe* pipe;
                try {
                    pipe = getServer(idx, vbucket, *iter, downstream, evbase);
                } catch (std::string& e) {
                    cerr << "Failed to connect to host for bucket " << *iter
                         << ": " << e << std::endl;
                    return EX_CONFIG;
                }
                bucketMap[*iter].push_back(pipe);
            }
        }
    }

    if (verbosity) {
        cout << "Connecting to source: " << host << endl;
    }

    vbucket_config_destroy(vbucket);
    Socket sock(host);
    sock.connect();
    sock.setNonBlocking();

    BinaryMessagePipe pipe(sock, upstream, evbase);
    pipe.sendMessage(new TapRequestBinaryMessage(buckets, takeover));

    upstream.setBucketmap(bucketMap);
    downstream.setUpstream(pipe);

    event_base_loop(evbase, 0);

    int ret = EX_OK;
    return ret;
}
