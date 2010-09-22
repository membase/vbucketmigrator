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
#ifndef BINARYMESSAGEPIPE_H
#define BINARYMESSAGEPIPE_H 1

#include "config.h"
#include "binarymessage.h"
#include "mutex.h"
#include "sockstream.h"
#include <memcached/vbucket.h>
#include <string>
#include <queue>
#include <event.h>

#ifndef evutil_socket_t
#define evutil_socket_t int
#endif

class BinaryMessagePipeCallback {
public:
    virtual ~BinaryMessagePipeCallback() {}
    virtual void messageReceived(BinaryMessage *msg) = 0;
    virtual void messageSent(BinaryMessage *msg) { (void)msg; };
    virtual void abort() = 0;
    virtual void shutdown() {};
    void markcomplete();
};

extern "C" {
    void event_handler(evutil_socket_t fd, short which, void *arg);
}

class BinaryMessagePipe {
public:
    BinaryMessagePipe(Socket &s, BinaryMessagePipeCallback &cb, struct event_base *b,
                      int tmout) :
        sock(s), callback(cb), msg(NULL), avail(0), flags(0), base(b), timeout(tmout),
        sendptr(NULL), sendlen(0), closed(false), doRead(true)
    {
        updateEvent();
    }

    ~BinaryMessagePipe() {
    }

    void abort() {
        callback.abort();
        closed = true;
        // we need to delete event before closing fd
        updateEvent();
        sock.close();
    }

    void authenticate(const std::string &authname, const std::string &password);

    void plugInput(void) {
        doRead = false;
        updateEvent();
    }

    void unPlugInput(void) {
        doRead = true;
        updateEvent();
    }

    void step(short flags);

    /**
     * Send a message over this pipe to the other end.
     * This function transfer the ownership of the msg pointer
     *
     * @param msg the message to send. The message will be
     *        deleted by calling delete when the message is transferred
     */
    void sendMessage(BinaryMessage *message) {
        ++message->refcount;
        queue.push(message);
        updateEvent();
    }

    void updateEvent();

    std::string toString() const {
        std::stringstream ss;
        ss << "BinaryMessagePipe from " << sock.toString()
           << " on fd " << sock.getSocket();
        return ss.str();
    }

    vbucket_state_t getVBucketState(uint16_t bucket);

    bool isClosed() const { return closed; }

protected:

    /**
     * Read a message from the stream
     * @return true if a complete message is available in msg, false otherwise
     */
    bool readMessage();

    /**
     * Write as much as possible from the message queue to the socket
     * @return true if all messages in the queue are successfully sent, false otherwise
     */
    bool drainBuffers();

    /**
     * Try to read and dispatch as many messages from the input pipe
     */
    void fillBuffers();

    Socket &sock;
    BinaryMessagePipeCallback &callback;
    BinaryMessage *msg;
    size_t bufsz;
    protocol_binary_request_header header;
    size_t avail;
    short flags;
    struct event_base *base;
    struct event ev;
    int timeout;

    std::queue<BinaryMessage *> queue;
    uint8_t *sendptr;
    ssize_t sendlen;

    bool closed;
    bool doRead;
};

#endif
