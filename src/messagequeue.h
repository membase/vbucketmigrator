/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Copyright (C) 2010 NorthScale, Inc
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in this directory for full text.
 */
#ifndef MESSAGEQUEUE_H
#define MESSAGEQUEUE_H 1

#include "config.h"
#include "binarymessage.h"
#include "mutex.h"

#include <queue>

class MessageQueue {
public:
    MessageQueue() : mutex(), queue() {}
    ~MessageQueue() {
        // @todo delete all messages
    }

    void push(BinaryMessage *msg) {
        mutex.acquire();
        queue.push(msg);
        mutex.release();
    }

    BinaryMessage* pop(void) {
        mutex.acquire();
        BinaryMessage *ret = NULL;
        if (!queue.empty()) {
            ret = queue.front();
            queue.pop();
        }
        mutex.release();
        return ret;
    }

    bool empty() {
        mutex.acquire();
        bool ret = queue.empty();
        mutex.release();
        return ret;
    }

    Mutex mutex;
    std::queue<BinaryMessage *> queue;
};

#endif
