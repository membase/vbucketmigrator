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
#include "messagequeue.h"
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
    BinaryMessagePipe(Socket &s, BinaryMessagePipeCallback &cb,
                      struct event_base *b) :
        sock(s), callback(cb), msg(NULL), avail(0), mq(), flags(0), base(b),
        closed(false)
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
        mq.push(message);
        updateEvent();
    }

protected:
    friend void event_handler(int fd, short which, void *arg);
    void updateEvent() {
        short new_flags = EV_READ | EV_PERSIST;
        if (!mq.empty()) {
            new_flags |= EV_WRITE;
        }

        if (closed) {
            assert(event_del(&ev) != -1);
            return;
        }

        if (new_flags != flags) {
            if (flags != 0) {
                 assert(event_del(&ev) != -1);
            }
            event_set(&ev, sock.getSocket(), new_flags, event_handler,
                      reinterpret_cast<void *>(this));
            event_base_set(base, &ev);
            assert(event_add(&ev, 0) != -1);
            flags = new_flags;
        }
    }

    Socket &sock;
    BinaryMessagePipeCallback &callback;
    BinaryMessage *msg;
    size_t bufsz;
    protocol_binary_request_header header;
    size_t avail;
    MessageQueue mq;
    short flags;
    struct event_base *base;
    struct event ev;
    bool closed;
};

#endif
