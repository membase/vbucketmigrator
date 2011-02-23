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
#ifndef SOCKSTREAM_H
#define SOCKSTREAM_H

#include "config.h"
#include <string>
#include <sstream>
#include <iostream>
#include <sys/types.h>

class osockstream;
class isockstream;

class Socket {

public:
    Socket(SOCKET s) : sock(s), host(""), port(0),
                       in(NULL), out(NULL), ai(NULL)
    {
        // @todo use getpeername to lookup the peers name
    }

    Socket(const std::string &h) : sock(INVALID_SOCKET), host(h), port("11211"),
                                   in(NULL), out(NULL), ai(NULL) {
        ssize_t s = host.find(":");
        if (s != -1) {
            port = host.substr(s + 1);
            host = host.substr(0, s);
        }
    }

    Socket(const std::string &h, in_port_t p) : sock(INVALID_SOCKET),
                                                host(h), port(), in(NULL),
                                                out(NULL), ai(NULL) {
        std::stringstream ss;
        ss << p;
        port.assign(ss.str());
    }

    ~Socket() {
        close();
    }

    friend std::ostream& operator<< (std::ostream& o, const Socket &p) {
        return o << "{Sock " << p.host << ":" << p.port << "}";
    }

    std::string toString() const {
        std::stringstream ss;
        ss << host << ":" << port;
        return ss.str();
    }

    std::string getLocalAddress() const throw (std::string);
    std::string getRemoteAddress() const throw (std::string);

    void setBlockingMode(bool blocking) throw (std::string);
    void setBlocking() throw (std::string) { setBlockingMode(true); }
    void setNonBlocking()throw (std::string) { setBlockingMode(false); }
    void setTimeout(int millis);
    void setTimeout(int which, int millis);

    void setKeepalive(bool enable) throw (std::string);

    void resolve(void) throw (std::string);
    void connect(void) throw (std::string);
    void close(void);

    SOCKET getSocket() const { return sock; }
    std::ostream *getOutStream();
    std::istream *getInStream();

protected:
    SOCKET sock;
    std::string host;
    std::string port;
    isockstream *in;
    osockstream *out;
    struct addrinfo *ai;
};

#endif
