/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Copyright (C) 2010 NorthScale, Inc
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in this directory for full text.
 */
#ifndef BINARYMESSAGEPIPE_H
#define BINARYMESSAGEPIPE_H 1

#include "config.h"
#include "binarymessage.h"
#include "mutex.h"
#include "sockstream.h"

#include <queue>
#include <event.h>

class BinaryMessagePipeCallback {
public:
    virtual ~BinaryMessagePipeCallback() {}
    virtual void messageReceived(BinaryMessage *msg) = 0;
    virtual void messageSent(BinaryMessage *msg) { (void)msg; };
    virtual void abort() = 0;
};

extern "C" {
    void event_handler(int fd, short which, void *arg);
}

class BinaryMessagePipe {
public:
    BinaryMessagePipe(Socket &s, BinaryMessagePipeCallback &cb, struct event_base *b) :
        sock(s), callback(cb), msg(NULL), avail(0), flags(0), base(b),
        sendptr(NULL), sendlen(0), closed(false)
    {
        updateEvent();
    }

    ~BinaryMessagePipe() {
    }

    void abort() {
        callback.abort();
        closed = true;
        sock.close();
    }

    void authenticate(const std::string &authname, const std::string &password);

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

    std::queue<BinaryMessage *> queue;
    uint8_t *sendptr;
    ssize_t sendlen;

    bool closed;
};

#endif
