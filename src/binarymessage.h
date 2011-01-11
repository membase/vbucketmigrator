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
#ifndef BINARYMESSAGE_H
#define BINARYMESSAGE_H 1

#include "config.h"
#include <map>
#include <string>
#include <sstream>
#include <iomanip>
#include <cstring>
#include <queue>
#include <assert.h>
#include <cerrno>
#include <stdexcept>

#include <memcached/protocol_binary.h>
#include <memcached/vbucket.h>

class BinaryMessage {
public:
    BinaryMessage() : size(0) {
        data.rawBytes = NULL;
    }

    BinaryMessage(const protocol_binary_request_header &h) throw (std::runtime_error)
        : size(ntohl(h.request.bodylen) + sizeof(h.bytes))
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

    std::string getBody() const {
        std::string key;
        uint16_t bodylen = ntohl(data.req->request.bodylen);
        char *ptr = data.rawBytes + sizeof(*data.req);
        ptr += data.req->request.extlen + data.req->request.keylen;
        key.assign(ptr, bodylen);

        return key;
    }

    void setExpiry(uint32_t expiry) {
        if (data.mutation->message.header.request.opcode == PROTOCOL_BINARY_CMD_TAP_MUTATION) {
            data.mutation->message.body.item.expiration = htonl(expiry);
        }
    }

    void setFlags(uint32_t flags) {
        if (data.mutation->message.header.request.opcode == PROTOCOL_BINARY_CMD_TAP_MUTATION) {
            data.mutation->message.body.item.flags = htonl(flags);
        }
    }

    std::string getComCode() const {
        switch (data.req->request.opcode) {
        case PROTOCOL_BINARY_CMD_NOOP: return "NOOP";
        case PROTOCOL_BINARY_CMD_TAP_CONNECT: return "TCONNECT";
        case PROTOCOL_BINARY_CMD_TAP_MUTATION: return "TMUTATION";
        case PROTOCOL_BINARY_CMD_TAP_DELETE: return "TDELETE";
        case PROTOCOL_BINARY_CMD_TAP_FLUSH: return "TFLUSH";
        case PROTOCOL_BINARY_CMD_TAP_OPAQUE: return "TOPAQUE";
        case PROTOCOL_BINARY_CMD_TAP_VBUCKET_SET: return "TVBSET";
        default:
            {
                std::stringstream ss;
                ss << "0x" << std::setfill('0') << std::setw(2) << std::hex
                   << static_cast<int>(data.req->request.opcode);
                return ss.str();
            }
        }
    }

    std::string getMessageType() const {
        switch (data.req->request.magic) {
        case PROTOCOL_BINARY_REQ: return "REQ";
        case PROTOCOL_BINARY_RES: return "RES";
        default:
            {
                std::stringstream ss;
                ss << "0x" << std::setfill('0') << std::setw(2) << std::hex
                   << static_cast<int>(data.req->request.magic);
                return ss.str();
            }
        }
    }

    std::string toString() const {
        std::stringstream ss;
        ss << "[ " << getMessageType()
           << " V: " << getVBucketId()
           << " " << getComCode();

        if (data.req->request.opcode >= PROTOCOL_BINARY_CMD_TAP_MUTATION &&
            data.req->request.opcode <= PROTOCOL_BINARY_CMD_TAP_VBUCKET_SET) {
            ss << " (tap seqno: " << std::hex << ntohl(data.req->request.opaque);

            uint16_t flags = ntohs(data.mutation->message.body.tap.flags);
            if (flags & TAP_FLAG_ACK) {
                ss << " ACK request";
            }
            ss << ")";
        }

        if (data.req->request.opcode == PROTOCOL_BINARY_CMD_TAP_VBUCKET_SET) {
            if (ntohl(data.req->request.bodylen) >= sizeof(vbucket_state_t)) {
                vbucket_state_t state;
                memcpy(&state, data.rawBytes + sizeof(data.vs->bytes),
                       sizeof(state));
                state = static_cast<vbucket_state_t>(ntohl(state));

                ss << " (set vbucket state to \"";
                switch (state) {
                case vbucket_state_active:
                    ss << "active\"";
                    break;
                case vbucket_state_replica:
                    ss << "replica\"";
                    break;
                case vbucket_state_pending:
                    ss << "pending\"";
                    break;
                case vbucket_state_dead:
                    ss << "dead\"";
                    break;
                default:
                    ss << "unknown";
                }
                ss << ")";
            }
        }

        if (data.req->request.keylen != 0) {
            ss << " k: <" << getKey() << ">";
        }

        ss << "]";

        return ss.str();
    }

    size_t size;
    union {
        protocol_binary_request_header *req;
        protocol_binary_response_header *res;
        protocol_binary_request_tap_connect *tap_connect;
        protocol_binary_request_tap_mutation *mutation;
        protocol_binary_request_tap_delete *remove;
        protocol_binary_request_tap_flush *flush;
        protocol_binary_request_tap_opaque *opaque;
        protocol_binary_request_tap_vbucket_set *vs;
        protocol_binary_response_get_vbucket *vg;
        char *rawBytes;
    } data;
};

class TapRequestBinaryMessage : public BinaryMessage {
public:
    TapRequestBinaryMessage(const std::string &name, std::vector<uint16_t> buckets,
                            bool takeover, bool tapAck) :
        BinaryMessage()
    {
        size = sizeof(data.tap_connect->bytes) + buckets.size() * 2 + 2 + name.length();
        data.rawBytes = new char[size];
        data.req->request.magic = PROTOCOL_BINARY_REQ;
        data.req->request.opcode = PROTOCOL_BINARY_CMD_TAP_CONNECT;
        data.req->request.keylen = ntohs(static_cast<uint16_t>(name.length()));
        data.req->request.extlen = 4;
        data.req->request.datatype = PROTOCOL_BINARY_RAW_BYTES;
        data.req->request.vbucket = 0;
        uint32_t bodylen = data.req->request.extlen + buckets.size() * 2 + 2 +
            name.length();
        data.req->request.bodylen = htonl(bodylen);;
        data.req->request.opaque = 0xcafecafe;
        data.req->request.cas = 0;

        uint32_t flags = TAP_CONNECT_FLAG_LIST_VBUCKETS;
        if (takeover) {
            flags |= TAP_CONNECT_FLAG_TAKEOVER_VBUCKETS;
        }

        if (tapAck) {
            flags |= TAP_CONNECT_SUPPORT_ACK;
        }

        data.tap_connect->message.body.flags = htonl(flags);
        char *ptr = data.rawBytes + sizeof(data.tap_connect->bytes);

        if (name.length() > 0) {
            memcpy(ptr, name.c_str(), name.length());
            ptr += name.length();
        }

        // To avoid alignment problems we have to do this the hard way..
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

class SaslListMechsBinaryMessage : public BinaryMessage {
public:
    SaslListMechsBinaryMessage() : BinaryMessage()
    {
        size = sizeof(data.req->bytes);
        data.rawBytes = new char[size];
        data.req->request.magic = PROTOCOL_BINARY_REQ;
        data.req->request.opcode = PROTOCOL_BINARY_CMD_SASL_LIST_MECHS;
        data.req->request.keylen = 0;
        data.req->request.extlen = 0;
        data.req->request.datatype = PROTOCOL_BINARY_RAW_BYTES;
        data.req->request.vbucket = 0;
        data.req->request.bodylen = 0;
        data.req->request.opaque = 0xcafecafe;
        data.req->request.cas = 0;
    }
};

class SaslAuthBinaryMessage : public BinaryMessage {
public:
    SaslAuthBinaryMessage(size_t keylen, const char *key,
                          size_t bodylen, const char *body) :
        BinaryMessage()
    {
        size = sizeof(data.req->bytes) + keylen + bodylen;
        data.rawBytes = new char[size];
        data.req->request.magic = PROTOCOL_BINARY_REQ;
        data.req->request.opcode = PROTOCOL_BINARY_CMD_SASL_AUTH;
        data.req->request.keylen = htons((uint16_t)keylen);
        data.req->request.extlen = 0;
        data.req->request.datatype = PROTOCOL_BINARY_RAW_BYTES;
        data.req->request.vbucket = 0;
        data.req->request.bodylen = htonl((uint32_t)(keylen + bodylen));
        data.req->request.opaque = 0xcafecafe;
        data.req->request.cas = 0;
        memcpy(data.req->bytes + sizeof(data.req->bytes), key, keylen);
        memcpy(data.req->bytes + sizeof(data.req->bytes) + keylen, body, bodylen);
    }
};

class SaslStepBinaryMessage : public SaslAuthBinaryMessage {
public:
    SaslStepBinaryMessage(size_t keylen, const char *key,
                          size_t bodylen, const char *body) :
        SaslAuthBinaryMessage(keylen, key, bodylen, body)
    {
        data.req->request.opcode = PROTOCOL_BINARY_CMD_SASL_STEP;
    }
};

class GetVBucketStateBinaryMessage : public BinaryMessage {
public:
    GetVBucketStateBinaryMessage(uint16_t bucket) : BinaryMessage()
    {
        size = sizeof(data.req->bytes);
        data.rawBytes = new char[size];
        data.req->request.magic = PROTOCOL_BINARY_REQ;
        data.req->request.opcode = PROTOCOL_BINARY_CMD_GET_VBUCKET;
        data.req->request.keylen = 0;
        data.req->request.extlen = 0;
        data.req->request.datatype = PROTOCOL_BINARY_RAW_BYTES;
        data.req->request.vbucket = htons(bucket);
        data.req->request.bodylen = 0;
        data.req->request.opaque = 0xcafecafe;
        data.req->request.cas = 0;
    }
};

class FlushBinaryMessage : public BinaryMessage {
public:
    FlushBinaryMessage() : BinaryMessage() {
        size = sizeof(data.req->bytes);
        data.rawBytes = new char[size];
        data.req->request.magic = PROTOCOL_BINARY_REQ;
        data.req->request.opcode = PROTOCOL_BINARY_CMD_FLUSHQ;
        data.req->request.keylen = 0;
        data.req->request.extlen = 0;
        data.req->request.datatype = PROTOCOL_BINARY_RAW_BYTES;
        data.req->request.vbucket = 0;
        data.req->request.bodylen = 0;
        data.req->request.opaque = 0xdeaddead;
        data.req->request.cas = 0;
    }
};

#endif
