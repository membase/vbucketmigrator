/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Copyright (C) 2010 NorthScale, Inc
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in this directory for full text.
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

    void setNonBlocking() throw (std::string);
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
