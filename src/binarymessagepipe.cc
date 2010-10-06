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
#include "binarymessagepipe.h"

void BinaryMessagePipe::step(short mask) {
    if ((mask & EV_WRITE) == EV_WRITE) {
        drainBuffers();
    }

    if ((mask & EV_READ) == EV_READ) {
        fillBuffers();
    }
}

bool BinaryMessagePipe::drainBuffers() {
    if (sendptr != NULL || !queue.empty()) {
        do {
            if (sendptr == NULL) {
                if (!queue.empty()) {
                    sendptr = queue.front()->data.req->bytes;
                    sendlen = (ssize_t)queue.front()->size;
                } else {
                    // no more data to send!
                    return true;
                }
            }

            ssize_t nw;
            nw = send(sock.getSocket(), (const char*)sendptr, sendlen, 0);
            if (nw == -1) {
                switch (get_socket_errno()) {
                case EINTR:
                    // retry
                    break;
                case EWOULDBLOCK:
                    // no more could be sent at this time...
                    return false;
                default:
                    {
                        std::stringstream err;
                        err << "Failed to write to stream: " << strerror(get_socket_errno());
                        throw std::runtime_error(err.str());
                    }
                }
            } else if (nw == sendlen) {
                BinaryMessage *next = queue.front();
                queue.pop();
                callback.messageSent(next);
                --next->refcount;
                if (next->refcount == 0) {
                    delete next;
                }
                sendptr = NULL;
            } else {
                sendptr += nw;
                sendlen -= nw;
            }

        } while (true);
    }
    return true;
}

bool BinaryMessagePipe::readMessage() {
    do {
        char *dst;
        size_t nbytes;
        ssize_t nr = 0;

        if (msg == NULL) {
            dst = reinterpret_cast<char*>(&header) + avail;
            nbytes = sizeof(header.bytes) - avail;
        } else {
            dst = msg->data.rawBytes + sizeof(header.bytes) + avail;
            nbytes = ntohl(header.request.bodylen) - avail;
        }

        if (nbytes > 0) {
            if ((nr = recv(sock.getSocket(), dst, nbytes, 0)) == -1) {
                switch (get_socket_errno()) {
                case EINTR:
                    break;
                case EWOULDBLOCK:
                    return false;
                default:
                    {
                        std::stringstream err;
                        err << "Failed to read from stream: "
                            << strerror(get_socket_errno());
                        throw std::runtime_error(err.str());
                    }
                }
            } else if (nr == 0) {
                closed = true;
                return false;
            }
        }

        avail += nr;
        if (nr == static_cast<ssize_t>(nbytes)) {
            // we got everything..
            avail = 0;
            if (msg == NULL) {
                msg = new BinaryMessage(header);
            } else {
                return true;
            }
        }
    } while (true);
}

void BinaryMessagePipe::fillBuffers() {
    while (readMessage()) {
        callback.messageReceived(msg);
        msg = NULL;
    }

    if (closed) {
        callback.shutdown();
    }
}

void BinaryMessagePipe::updateEvent() {
    short new_flags = EV_PERSIST;
    if (!queue.empty()) {
        new_flags |= EV_WRITE;
    }

    if (doRead) {
        new_flags |= EV_READ;
    }

    if (closed) {
        int event_del_rv = event_del(&ev);
        assert(event_del_rv != -1);
        return;
    }

    if (new_flags != flags) {
        if (flags != 0) {
            int event_del_rv = event_del(&ev);
            assert(event_del_rv != -1);
        }
        bool has_io = (new_flags & ~EV_PERSIST);
        event_set(&ev, sock.getSocket(), new_flags, event_handler,
                  reinterpret_cast<void *>(this));
        event_base_set(base, &ev);
        struct timeval tv = {timeout, 0};
        int event_add_rv = event_add(&ev, (has_io && timeout > 0) ? &tv : NULL);
        assert(event_add_rv != -1);
        flags = new_flags;
    }
}

#ifdef ENABLE_SASL
extern "C" {
    static int get_username(void *context, int id, const char **result,
                            unsigned int *len) {
        if (!context || !result || (id != SASL_CB_USER && id != SASL_CB_AUTHNAME)) {
            return SASL_BADPARAM;
        }

        *result = (char*)context;
        if (len) {
            *len = (unsigned int)strlen(*result);
        }

        return SASL_OK;
    }

    static int get_password(sasl_conn_t *conn, void *context, int id,
                            sasl_secret_t **psecret) {
        if (!conn || ! psecret || id != SASL_CB_PASS) {
            return SASL_BADPARAM;
        }

        *psecret = (sasl_secret_t*)context;
        return SASL_OK;
    }

    typedef int(*SASLFUNC)();
}
#endif

void BinaryMessagePipe::authenticate(const std::string &authname,
                                     const std::string &password) {
#ifndef ENABLE_SASL
    (void)authname;
    (void)password;
#else
    if (password.length() > 127) {
        throw std::runtime_error(std::string("Password too long"));
    }

    union {
        sasl_secret_t secret;
        char buffer[sizeof(sasl_secret_t) + 128];
    } secret;
    sasl_callback_t sasl_callbacks[4] = {
        { SASL_CB_USER, (SASLFUNC)&get_username, (void*)authname.c_str() },
        { SASL_CB_AUTHNAME, (SASLFUNC)&get_username, (void*)authname.c_str() },
        { SASL_CB_PASS, (SASLFUNC)&get_password, (void*)&secret.secret },
        { SASL_CB_LIST_END, NULL, NULL }
    };

    memset(secret.buffer, 0, sizeof(secret.buffer));
    secret.secret.len = password.length();
    memcpy(secret.secret.data, password.c_str(), password.length());

    BinaryMessage *message = new SaslListMechsBinaryMessage;
    ++message->refcount;
    queue.push(message);
    if (!drainBuffers()) {
        throw std::runtime_error(std::string("Failed to send auth data"));
    }

    if (!readMessage()) {
        throw std::runtime_error(std::string("Failed to receive auth data"));
    }

    if (msg->data.res->response.opcode != PROTOCOL_BINARY_CMD_SASL_LIST_MECHS) {
        throw std::runtime_error(std::string("Internal error, unexpected package received"));
    }

    if (ntohs(msg->data.res->response.status) != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        throw std::runtime_error(std::string("Failed to get sasl mechs"));
    }

    std::string mechs((char*)msg->data.res->bytes + sizeof(msg->data.res->bytes),
                      ntohl(msg->data.res->response.bodylen));
    delete msg;
    msg = NULL;

    sasl_conn_t *conn;
    int ret = sasl_client_new("memcached", sock.toString().c_str(),
                              sock.getLocalAddress().c_str(),
                              sock.getRemoteAddress().c_str(),
                              sasl_callbacks, 0, &conn);
    if (ret != SASL_OK) {
        throw std::runtime_error("Failed to create sasl client");
    }

    const char *data;
    const char *chosenmech;
    unsigned int len;
    ret = sasl_client_start(conn, mechs.c_str(), NULL, &data, &len, &chosenmech);

    if (ret != SASL_OK && ret != SASL_CONTINUE) {
        sasl_dispose(&conn);
        throw std::runtime_error(std::string("sasl_client_start failed"));
    }

    size_t clen = strlen(chosenmech);
    message = new SaslAuthBinaryMessage(clen, chosenmech, len, data);
    do {
        ++message->refcount;
        queue.push(message);
        if (!drainBuffers()) {
            sasl_dispose(&conn);
            throw std::runtime_error(std::string("Failed to send auth data"));
        }

        if (!readMessage()) {
            sasl_dispose(&conn);
            throw std::runtime_error(std::string("Failed to receive auth data"));
        }

        if (msg->data.res->response.opcode != PROTOCOL_BINARY_CMD_SASL_STEP &&
            msg->data.res->response.opcode != PROTOCOL_BINARY_CMD_SASL_AUTH) {
            sasl_dispose(&conn);
            throw std::runtime_error(std::string("Internal error unexpected pacakge received"));
        }

        uint16_t klen = ntohs(msg->data.res->response.keylen);
        uint32_t blen = ntohl(msg->data.res->response.bodylen);
        std::string bytes((char*)msg->data.res->bytes + sizeof(msg->data.res->bytes) +
                          klen + msg->data.res->response.extlen,
                          blen - klen - msg->data.res->response.extlen);

        uint16_t stat = ntohs(msg->data.res->response.status);
        delete msg;
        msg = NULL;

        switch (stat) {
        case PROTOCOL_BINARY_RESPONSE_SUCCESS:
            sasl_dispose(&conn);
            return;
        case PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE:
            break;
        case PROTOCOL_BINARY_RESPONSE_AUTH_ERROR:
            {
                sasl_dispose(&conn);
                std::stringstream ss;
                ss << "Authentication failed: " << bytes;
                throw std::runtime_error(ss.str());
            }
        default:
            {
                sasl_dispose(&conn);
                std::stringstream ss;
                ss << "Internal error: " << bytes;
                throw std::runtime_error(ss.str());
            }
        }

        ret = sasl_client_step(conn, bytes.c_str(), bytes.length(), NULL, &data, &len);
        if (ret != SASL_OK && ret != SASL_CONTINUE) {
            sasl_dispose(&conn);
            throw std::runtime_error(std::string("sasl_client_step failed"));
        }

        message = new SaslStepBinaryMessage(clen, chosenmech, len, data);
    } while (true);
#endif
}

vbucket_state_t BinaryMessagePipe::getVBucketState(uint16_t bucket, int tmout) {
    sock.setBlockingMode(true);
    if (tmout > 0) {
        sock.setTimeout(tmout);
    }
    BinaryMessage *message = new GetVBucketStateBinaryMessage(bucket);
    ++message->refcount;
    queue.push(message);
    if (!drainBuffers()) {
        throw std::runtime_error(std::string("Failed to send vbucket get state"));
    }

    if (!readMessage()) {
        throw std::runtime_error(std::string("Failed to receive vbucket state"));
    }

    if (msg->data.res->response.opcode != 0x84) {
        throw std::runtime_error(std::string("Internal error, unexpected pacage received"));
    }

    if (ntohs(msg->data.res->response.status) != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        std::stringstream ss;
        ss << "Failed to get vbucket state: "
           << ntohs(msg->data.res->response.status);
        throw std::runtime_error(ss.str());
    }

    std::string state = msg->getBody();
    delete msg;
    msg = NULL;
    vbucket_state_t ret;

    if (state.compare("active") == 0) {
        ret = active;
    } else if (state.compare("replica") == 0) {
        ret = replica;
    } else if (state.compare("pending") == 0) {
        ret = pending;
    } else if (state.compare("dead") == 0) {
        ret = dead;
    } else {
        std::stringstream ss;
        ss << "Internal error, received unknown vbucket state: ["
           << state.c_str() << "] " << state.length();
        throw std::runtime_error(ss.str());
    }

    return ret;
}
