/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Copyright (C) 2010 NorthScale, Inc
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in this directory for full text.
 */
#ifndef BINARYMESSAGE_H
#define BINARYMESSAGE_H 1

#include "config.h"
#include <map>
#include <string>
#include <sstream>
#include <cstring>
#include <queue>
#include <assert.h>
#include <cerrno>
#include <stdexcept>

#include <memcached/protocol_binary.h>

class BinaryMessage {
public:
    BinaryMessage() : size(0), refcount(0) {
        data.rawBytes = NULL;
    }

    BinaryMessage(const protocol_binary_request_header &h) throw (std::runtime_error)
        : size(ntohl(h.request.bodylen) + sizeof(h.bytes)), refcount(0)
    {
        // verify the internal
        if (h.request.magic != PROTOCOL_BINARY_REQ &&
            h.request.magic != PROTOCOL_BINARY_RES) {
            throw std::runtime_error("Invalid package detected on the wire");
        }
        data.rawBytes = new char[size];
        memcpy(data.rawBytes, reinterpret_cast<const char*>(&h),
               sizeof(h.bytes));
    }

    virtual ~BinaryMessage() {
        delete []data.rawBytes;
    }

    uint16_t getVBucketId() const {
        assert(data.rawBytes != NULL);
        return ntohs(data.req->request.vbucket);
    }

    std::string getKey() const {
        std::string key;
        uint16_t keylen = ntohs(data.req->request.keylen);
        char *ptr = data.rawBytes + sizeof(*data.req);
        ptr += data.req->request.extlen;
        key.assign(ptr, keylen);

        return key;
    }

    std::string toString() const {
        std::stringstream ss;
        ss << "[ V: " << getVBucketId() << " k: <" << getKey() << ">]";
        return ss.str();
    }

    size_t size;
    int refcount;
    union {
        protocol_binary_request_header *req;
        protocol_binary_request_tap_connect *tap_connect;
        protocol_binary_request_tap_mutation *mutation;
        protocol_binary_request_tap_delete *remove;
        protocol_binary_request_tap_flush *flush;
        protocol_binary_request_tap_opaque *opaque;
        protocol_binary_request_tap_vbucket_set *vs;
        char *rawBytes;
    } data;
};

class TapRequestBinaryMessage : public BinaryMessage {
public:
    TapRequestBinaryMessage(std::vector<uint16_t> buckets, bool takeover) :
        BinaryMessage()
    {
        size = sizeof(data.tap_connect->bytes) + buckets.size() * 2 + 2;
        data.rawBytes = new char[size];
        data.req->request.magic = PROTOCOL_BINARY_REQ;
        data.req->request.opcode = PROTOCOL_BINARY_CMD_TAP_CONNECT;
        data.req->request.keylen = 0;
        data.req->request.extlen = 4;
        data.req->request.datatype = PROTOCOL_BINARY_RAW_BYTES;
        data.req->request.vbucket = 0;
        uint32_t bodylen = data.req->request.extlen + buckets.size() * 2 + 2;
        data.req->request.bodylen = htonl(bodylen);;
        data.req->request.opaque = 0xcafecafe;
        data.req->request.cas = 0;

        uint32_t flags = TAP_CONNECT_FLAG_LIST_VBUCKETS;
        if (takeover) {
            flags |= TAP_CONNECT_FLAG_TAKEOVER_VBUCKETS;
        }
        data.tap_connect->message.body.flags = htonl(flags);

        // To avoid alignment problems we have to do this the hard way..
        char *ptr = data.rawBytes + sizeof(data.tap_connect->bytes);
        uint16_t val = htons(static_cast<uint16_t>(buckets.size()));
        memcpy(ptr, &val, 2);
        ptr += 2;
        for (size_t i = 0; i < buckets.size(); ++i) {
            val = htons(buckets[i]);
            memcpy(ptr, &val, 2);
            ptr += 2;
        }
    }
};

#endif
