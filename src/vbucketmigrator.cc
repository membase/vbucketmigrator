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
#include <algorithm>
#include <memcached/vbucket.h>

#include "sockstream.h"
#include "binarymessagepipe.h"
#include "buckets.h"

#ifndef EX_SOFTWARE
#define EX_SOFTWARE 70
#endif

using namespace std;

static uint8_t verbosity(0);
static unsigned int timeout = 0;
static int exit_code = EX_OK;
static struct event timerev;
static bool evtimer_active(false);

static size_t packets(0);

static void usage(std::string binary) {
    ssize_t idx = binary.find_last_of("/\\");
    if (idx != -1) {
        binary = binary.substr(idx + 1);
    }

    cerr << "Usage: " << binary
         << " -h host:port -b # -d desthost:destport" << endl
         << "\t-h host:port Connect to host:port" << endl
         << "\t-A           Use TAP acks" << endl
         << "\t-t           Move buckets from a server to another server"<< endl
         << "\t-b #         Operate on bucket number #" << endl
         << "\t-a auth      Try to authenticate <auth>" << endl
         << "\t-d host:port Send all vbuckets to this server" << endl
         << "\t-v           Increase verbosity" << endl
         << "\t-F           Flush all data on the destination" << endl
         << "\t-N name      Use a tap stream named \"name\"" << endl
         << "\t-T timeout   Terminate if nothing happened for timeout seconds" << endl
         << "\t-e           Run as an Erlang port" << endl
         << "\t-V           Validate bucket takeover" << endl
         << "\t-E expiry    Reset the expiry of all items to 'expiry'." << endl
         << "\t-f flag      Reset the flag of all items to 'flag'." << endl
         << "\t-r           Connect to the master as a registered TAP client" << endl;
    exit(EX_USAGE);
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
        closed(false), inputPlugged(false), aborting(false)
    {
        // Empty
    }

    void sendUpstreamMessage(BinaryMessage *msg) {
        upstream->sendMessage(msg);
    }
    void incrementPendingDownstream() {
        if (!upstream) {
            return;
        }

        pendingSendCount++;
        if (!inputPlugged && pendingSendCount > PENDING_SEND_HI_WAT) {
            upstream->plugInput();
            inputPlugged = true;
        }
    }
    void decrementPendingDownstream() {
        if (!upstream) {
            return;
        }

        pendingSendCount--;
        if (inputPlugged && pendingSendCount < PENDING_SEND_LO_WAT && !closed) {
            upstream->unPlugInput();
            inputPlugged = false;
        }
    }

    void abort() {
        if (!aborting) {
            cerr << "Downstream connection closed.. shutdown upstream" << endl;
            aborting = true;
            upstream->abort();
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

    void dumpMessages(std::ostream &out) {
        upstream->dumpMessages(out);
    }

private:
    BinaryMessagePipe *upstream;
    int pendingSendCount;
    bool closed;
    bool inputPlugged;
    bool aborting;
};

class DownstreamBinaryMessagePipeCallback : public BinaryMessagePipeCallback {
public:
    DownstreamBinaryMessagePipeCallback(UpstreamController *_upstream) :
        upstream(_upstream), aborting(false), moved(0)
    {
        // Empty
    }

    void messageReceived(BinaryMessage *msg) {
        if (msg->data.req->request.opcode == PROTOCOL_BINARY_CMD_NOOP) {
            // Ignore NOOP responses
            delete msg;
        } else {
            upstream->sendUpstreamMessage(msg);
            if (verbosity > 1) {
                std::cout << "Received message from downstream server: "
                          << msg->toString() << std::endl;
            }
        }
    }

    void messageSent(BinaryMessage *msg) {
        upstream->decrementPendingDownstream();

        uint8_t opcode = msg->data.req->request.opcode;
        if (opcode == PROTOCOL_BINARY_CMD_TAP_VBUCKET_SET) {
            if (msg->size - sizeof(msg->data.vs->bytes) == sizeof(vbucket_state_t)) {
                // test the state thing..
                vbucket_state_t state;
                memcpy(&state, msg->data.rawBytes + sizeof(msg->data.vs->bytes),
                       sizeof(state));
                state = static_cast<vbucket_state_t>(ntohl(state));
                if (state == vbucket_state_pending) {
                    cout << "Starting to move bucket "
                         << msg->getVBucketId()
                         << endl;
                    cout.flush();
                } else if (state == vbucket_state_active) {
                    ++moved;
                    cout << "Bucket "
                         << msg->getVBucketId()
                         << " moved to the next server" << endl;
                    cout.flush();
                } else if (!is_valid_vbucket_state_t(state)) {
                    cerr << "Illegal vbucket state received: "
                         << state << endl;
                }
            } else {
                cerr << "Incorrect message size for TAP_VBUCKET_SET: " <<
                    msg->size - sizeof(msg->data.vs->bytes) << endl;
            }
        }
    }

    void abort() {
        if (!aborting) {
            aborting = true;
            cerr << "An error occured on the downstream connection.." << endl;
            markcomplete();
            upstream->abort();
        }
    }

    void shutdown() {
        aborting = true;
        markcomplete();
        upstream->abort();
    }

    size_t getMoved() const {
        return moved;
    }

private:
    UpstreamController *upstream;
    bool aborting;
    size_t moved;
};

class UpstreamBinaryMessagePipeCallback : public BinaryMessagePipeCallback {
public:
    UpstreamBinaryMessagePipeCallback(UpstreamController *_controller,
                                      const vector<uint16_t> &_buckets) :
        BinaryMessagePipeCallback(), controller(_controller),
        buckets(_buckets), aborting(false),
        hasExpiry(false), hasFlags(false), expiry(0), flags(0)
    {
        // EMPTY
    }

    void messageSent(BinaryMessage *msg) {
        if (verbosity > 1) {
            std::cout << "Message from downstream sent upstream: "
                      << msg->toString() << std::endl;
        }
    }


    void messageReceived(BinaryMessage *msg) {
        if (verbosity > 1) {
            std::cout << "Received message from upstream server: "
                      << msg->toString() << std::endl;
        }

        // Some messages are connection bound and not vbucket bound..
        bool allow;
        switch (msg->data.req->request.opcode) {
        case PROTOCOL_BINARY_CMD_NOOP:
        case PROTOCOL_BINARY_CMD_TAP_OPAQUE:
            allow = true;
            break;
        default:
            allow = false;
        }
        if (!allow && !std::binary_search(buckets.begin(), buckets.end(),
                                msg->getVBucketId())) {
            std::cerr << "Internal server error!!" << std::endl
                      << "Received a message for a bucket I didn't request:"
                      << msg->toString()
                      << std::endl;
            delete msg;
        } else {
            controller->incrementPendingDownstream();
            fixMessage(msg);
            downstream->sendMessage(msg);
        }
    }

    void fixMessage(BinaryMessage *msg) {
        if (hasExpiry) {
            msg->setExpiry(expiry);
        }
        if (hasFlags) {
            msg->setFlags(flags);
        }
    }

    void resetExpiry(uint32_t e) {
        hasExpiry = true;
        expiry = e;
    }

    void resetFlags(uint32_t f) {
        hasFlags = true;
        flags = f;
    }

    void setDownstream(BinaryMessagePipe *_downstream) {
        downstream = _downstream;
    }

    void completeMe() {
        markcomplete();
        controller->close();
    }

    void abort() {
        if (!aborting) {
            aborting = true;
            downstream->abort();
            completeMe();
        }
    }

    void shutdown() {
        downstream->plugInput();
        downstream->updateEvent();
        completeMe();
    }

private:
    BinaryMessagePipe *downstream;
    UpstreamController *controller;
    vector<uint16_t> buckets;
    bool aborting;
    bool hasExpiry;
    bool hasFlags;
    uint32_t expiry;
    uint32_t flags;
};

extern "C" {
    void event_handler(evutil_socket_t fd, short which, void *arg) {
        (void)fd;
        BinaryMessagePipe *pipe;
        pipe = reinterpret_cast<BinaryMessagePipe*>(arg);

        if (which == EV_TIMEOUT) {
            std::cerr << "Timed out on " << pipe->toString() << std::endl;
            exit(EXIT_FAILURE);
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
                exit(EXIT_FAILURE);
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

static BinaryMessagePipe *getServer(const string &destination,
                                    BinaryMessagePipeCallback &cb,
                                    struct event_base *b,
                                    const std::string &auth,
                                    const std::string &passwd,
                                    bool flush)
{
    BinaryMessagePipe* ret(NULL);
    std::string msg;
    try {
        string host(destination);
        Socket *sock = new Socket(host);
        if (verbosity) {
            cout << "Connecting to " << *sock << endl;
        }
        sock->connect();
        sock->setKeepalive(true);
        ret = new BinaryMessagePipe(*sock, cb, b, timeout);
        if (auth.length() > 0) {
            if (verbosity) {
                cout << "Authenticating towards: " << *sock << endl;
            }
            ret->authenticate(auth, passwd);
            if (verbosity) {
                cout << "Authenticated towards: " << *sock << endl;
            }
        }
        sock->setNonBlocking();
        if (flush) {
            ret->sendMessage(new FlushBinaryMessage);
        }
        ret->updateEvent();

    } catch (std::string &e) {
        msg = e;
    } catch (std::exception &e) {
        msg = e.what();
    } catch (...) {
        msg.assign("Unhandled exception");
    }

    if (msg.length() > 0) {
        throw msg;
    }

    assert(ret);
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
    string host;
    string destination;
    bool takeover = false;
    bool tapAck = false;
    bool erlang = false;
    string auth;
    string passwd;
    string name;
    bool validate = false;
    bool flush = false;
    bool registeredTapClient = false;
    string expiryResetValue;
    string flagResetValue;

    while ((cmd = getopt(argc, argv, "N:Aa:h:b:d:tvFT:e?VE:rf:")) != EOF) {
        switch (cmd) {
        case 'E':
            expiryResetValue.assign(optarg);
            break;
        case 'f':
            flagResetValue.assign(optarg);
            break;
        case 'A':
            tapAck = true;
            break;
        case 'a':
            auth.assign(optarg);
            break;
        case 'd':
            destination.assign(optarg);
            break;
        case 'h':
            host.assign(optarg);
            break;
        case 'b':
            try {
                parseBuckets(buckets, optarg);
            } catch (string& e) {
                cerr << e.c_str() << endl;
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
        case 'F':
            flush = true;
            break;
        case 'T':
            timeout = atoi(optarg);
            break;
        case 'e':
            erlang = true;
            break;
        case 'V':
            validate = true;
            break;
        case 'r':
            registeredTapClient = true;
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

    if (destination.empty()) {
        cerr << "Can't perform bucket migration without a destination host" << endl;
        return EX_USAGE;
    }

    if (buckets.empty()) {
        cerr << "Please specify the buckets to migrate by using -b" << endl;
        return EX_USAGE;
    }

    sort(buckets.begin(), buckets.end());
    struct event_base *evbase = event_init();
    if (evbase == NULL) {
        cerr << "Failed to initialize libevent" << endl;
        return EX_IOERR;
    }

    if (erlang) {
        stdin_check(evbase);
    }

    UpstreamController controller;
    UpstreamBinaryMessagePipeCallback upstream(&controller, buckets);
    DownstreamBinaryMessagePipeCallback downstream(&controller);
    BinaryMessagePipe *downstreamPipe;
    BinaryMessagePipe *upstreamPipe;

    if (expiryResetValue.length() != 0) {
        uint32_t expiry = strtoul(expiryResetValue.c_str(), NULL, 10);
        upstream.resetExpiry(expiry);
    }

    if (flagResetValue.length() != 0) {
        uint32_t flags = strtoul(flagResetValue.c_str(), NULL, 10);
        upstream.resetFlags(flags);
    }

    try {
        downstreamPipe = getServer(destination, downstream, evbase,
                                   auth, passwd, flush);
        upstreamPipe = getServer(host, upstream, evbase,
                                 auth, passwd, false);
    } catch (std::string &e) {
        cerr << "Failed to connect to host: " << e.c_str() << endl;
        return EX_CONFIG;
    }

    upstreamPipe->sendMessage(new TapRequestBinaryMessage(name, buckets, takeover,
                                                          tapAck, registeredTapClient));
    upstreamPipe->updateEvent();
    upstream.setDownstream(downstreamPipe);
    controller.setUpstream(upstreamPipe);

    if (flush) {
        controller.incrementPendingDownstream();
    }

    event_base_loop(evbase, 0);

    if (takeover && downstream.getMoved() != buckets.size()) {
        cerr << "Did not move enough vbuckets in takeover: "
             << downstream.getMoved() << "/" << buckets.size() << endl;
        exit_code = exit_code == 0 ? EX_SOFTWARE : exit_code;
    }

    if (controller.getPendingSendCount() != 0) {
        cerr << "Had " << controller.getPendingSendCount()
             << " pending messages at exit." << endl;
        controller.dumpMessages(cerr);
        exit_code = exit_code == 0 ? EX_SOFTWARE : exit_code;
    }

    // Validate all takeovers..
    if (exit_code == 0 && takeover && validate) {
        if (verbosity) {
            cout << "Validate bucket states" << std::endl;
        }

        unsigned int numSuccess = 0;
        vector<uint16_t>::iterator iter;
        for (iter = buckets.begin(); iter != buckets.end(); ++iter) {

            if (downstreamPipe->isClosed()) {
                cerr << "\t" << *iter
                     << " Failed to verify, pipe to "
                     << downstreamPipe->toString() << " is closed!" << endl;
                continue ;
            }

            std::string msg;
            try {
                vbucket_state_t state = downstreamPipe->getVBucketState(*iter,
                                                                        timeout * 1000);
                if (state == vbucket_state_active) {
                    ++numSuccess;
                }
                if (state != vbucket_state_active) {
                    cerr << "Incorrect state for " << *iter
                         << " at "
                         << downstreamPipe->toString() << ": " << state << endl;
                } else if (verbosity) {
                    cout << "\t" << *iter << " ok" << endl;
                }
            } catch (std::string &e) {
                msg = e;
            } catch (std::exception &e) {
                msg = e.what();
            } catch (...) {
                msg.assign("Unhandled exception");
            }

            if (msg.length()) {
                cerr << "\t" << *iter << " Failed to verify: "
                     << msg.c_str() << endl;
            }
        }

        if (numSuccess != buckets.size() && exit_code == 0) {
            cerr << "Expected to move " << buckets.size()
                 << " buckets, but moved " << numSuccess << std::endl;
            exit_code = EX_SOFTWARE;
        }
    }

    if (exit_code == 0 && !takeover) {
        // It is only the takeover processes that should exit, so getting
        // here would be some sort of a failure..
        exit_code = EX_SOFTWARE;
    }

    return exit_code;
}
