/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Copyright (C) 2010 NorthScale, Inc
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in this directory for full text.
 */
#include "config.h"
#include "binarymessagepipe.h"

void BinaryMessagePipe::step(short mask) {
    if ((mask & EV_WRITE) == EV_WRITE) {
        // @todo this needs to be improved! check for errors etc..
        if (!mq.empty()) {
            BinaryMessage *next = mq.pop();
            send(sock.getSocket(), next->data.rawBytes, next->size, 0);
            callback.messageSent(next);
            delete next;
        }
    }

    if ((mask & EV_READ) == EV_READ) {
        bool moreData = true;
        do {
            char *dst;
            size_t nbytes;
            ssize_t nr;

            if (msg == NULL) {
                dst = reinterpret_cast<char*>(&header) + avail;
                nbytes = sizeof(header.bytes) - avail;
            } else {
                dst = msg->data.rawBytes + sizeof(header.bytes) + avail;
                nbytes = ntohl(header.request.bodylen) - avail;
            }

            if ((nr = recv(sock.getSocket(), dst, nbytes, 0)) == -1) {
                switch (errno) {
                case EINTR:
                    break;
                case EWOULDBLOCK:
                    return;
                default:
                    return;
                }
            } else if (nr == 0) {
                closed = true;
                return ;
            }

            avail += nr;
            if (nr == static_cast<ssize_t>(nbytes)) {
                // we got everything..
                avail = 0;
                if (msg == NULL) {
                    msg = new BinaryMessage(header);
                } else {
                    callback.messageReceived(msg);
                    msg = NULL;
                }
            }
        } while (moreData);
    }
}

