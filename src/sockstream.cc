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

#include <sstream>
#include <fstream>
#include <sys/types.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <assert.h>
#include <cstring>

#include "sockstream.h"

using namespace std;

class sockstreambuf : public filebuf {
public:
    sockstreambuf(SOCKET s, size_t sz = 64*1024) : sock(s), size(sz),
                                                   buffer(NULL) {
        buffer = new char[size];
        setp(buffer, buffer + size - 1);
        setg(buffer, buffer, buffer);
    }

    virtual ~sockstreambuf() {
        sync();
        delete []buffer;
    }

    virtual int overflow(int c) {
        if (c != EOF) {
            *pptr() = c;
            pbump(1);
        }

        if (flushBuffer() == EOF) {
            fprintf(stderr, "RETURNING EOF\n");
            return EOF;
        }

        return c;
    }

    virtual int underflow() {
        ssize_t nr;

        while ((nr = recv(sock, buffer, size, 0)) == SOCKET_ERROR) {
            switch (get_socket_errno()) {
            case EINTR:
                // retry
                break;
            default:
                /* These should not happen. get a core file!! */
                abort();
            }
        }
        if (nr == 0) {
            return EOF;
        }
        setg(buffer, buffer, buffer + nr);
        return (unsigned char)buffer[0];
    }

    virtual int sync(){
        if (flushBuffer() == EOF) {
            return -1;
        }

        return 0;
    }

private:
    int flushBuffer() {
        ptrdiff_t nb = pptr() - pbase();

        if (nb > 0) {
            // @todo handle error code
            if (send(sock, buffer, nb, 0) != nb) {
                return EOF;
            }

            pbump(-nb);
        }

        return nb;
    }

    SOCKET sock;
    size_t size;
    char *buffer;
};

class osockstream : public ofstream {
public:
    osockstream(SOCKET sock) : buffer(sock) {
        ios::rdbuf(&buffer);
    }

private:
    sockstreambuf buffer;
};

class isockstream : public ifstream {
public:
    isockstream(SOCKET sock) : buffer(sock) {
        ios::rdbuf(&buffer);
    }

private:
    sockstreambuf buffer;
};

void Socket::resolve(void) throw (string)
{
    if (ai == NULL) {
        struct addrinfo hints;
        std::memset(&hints, 0, sizeof(hints));
        hints.ai_flags = AI_PASSIVE;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_family = AF_UNSPEC;

        int error = getaddrinfo(host.c_str(), port.c_str(), &hints, &ai);
        if (error != 0) {
            stringstream ss;
            ss << "getaddrinfo(): "
               << (error != EAI_SYSTEM) ? gai_strerror(error) : strerror(error);
            throw ss.str();
        }
    }
}

void Socket::connect(void) throw (string)
{
    if (sock != INVALID_SOCKET) {
        throw "Can't call connect() with an open Socket. Call close()!!";
    }

    resolve();

    for (struct addrinfo *next= ai; next; next= next->ai_next) {
        if ((sock = socket(ai->ai_family,
                           ai->ai_socktype,
                           ai->ai_protocol)) == INVALID_SOCKET) {
            continue;
        }

        if (::connect(sock, ai->ai_addr, ai->ai_addrlen) == SOCKET_ERROR) {
            closesocket(sock);
            sock = INVALID_SOCKET;
            continue;
        }

        break;
    }

    if (sock == INVALID_SOCKET) {
        stringstream msg;
        msg << "Failed to connect to [" << host << ":" << port << "]";
        throw msg.str();
    }
}

void Socket::close(void) {
    if (in) {
        delete in;
        in = NULL;
    }

    if (out) {
        delete out;
        out = NULL;
    }

    if (sock != INVALID_SOCKET) {
#if 0
        WSAIoctl(sock, SIO_FLUSH, NULL, 0, NULL, 0, NULL, NULL, NULL);
#endif
        shutdown(sock, SD_SEND); // disable sending of more data

        closesocket(sock);
        sock = INVALID_SOCKET;
    }

    if (ai != NULL) {
        freeaddrinfo(ai);
        ai = NULL;
    }
}

ostream *Socket::getOutStream()
{
    if (out == NULL) {
        out = new osockstream(sock);
    }
    return out;
}

istream *Socket::getInStream()
{
    if (in == NULL) {
        in = new isockstream(sock);
    }

    return in;
}

void Socket::setNonBlocking() throw (std::string)
{
#ifdef WIN32
    u_long arg = 1;
    if (ioctlsocket(sock, FIONBIO, &arg) == SOCKET_ERROR) {
        stringstream msg;
        msg << "Failed to enable nonblocking io: "
            << strerror(get_socket_errno());
        throw msg.str();
    }
#else
    int flags;
    if ((flags = fcntl(sock, F_GETFL, 0)) < 0) {
        stringstream msg;
        msg << "Failed to get current flags: " << strerror(get_socket_errno());
        throw msg.str();
    }

    if ((flags & O_NONBLOCK) != O_NONBLOCK) {
        if (fcntl(sock, F_SETFL, flags | O_NONBLOCK) < 0) {
            stringstream msg;
            msg << "Failed to enable O_NONBLOCK: " << strerror(get_socket_errno());
            throw msg.str();
        }
    }
#endif
}

std::string Socket::getLocalAddress() const throw (std::string) {
    char h[NI_MAXHOST];
    char p[NI_MAXSERV];
    struct sockaddr_storage saddr;
    socklen_t salen= sizeof(saddr);

    if ((getsockname(sock, (struct sockaddr *)&saddr, &salen) < 0) ||
        (getnameinfo((struct sockaddr *)&saddr, salen, h, sizeof(h),
                     p, sizeof(p), NI_NUMERICHOST | NI_NUMERICSERV) < 0))
    {
        stringstream msg;
        msg << "Failed to get local address: " << strerror(get_socket_errno());
        throw msg.str();
    }

    std::stringstream ss;
    ss << h << ";" << p;
    return ss.str();
}

std::string Socket::getRemoteAddress() const throw (std::string) {
    char h[NI_MAXHOST];
    char p[NI_MAXSERV];
    struct sockaddr_storage saddr;
    socklen_t salen= sizeof(saddr);

    if ((getpeername(sock, (struct sockaddr *)&saddr, &salen) < 0) ||
        (getnameinfo((struct sockaddr *)&saddr, salen, h, sizeof(h),
                     p, sizeof(p), NI_NUMERICHOST | NI_NUMERICSERV) < 0))
    {
        stringstream msg;
        msg << "Failed to get local address: " << strerror(get_socket_errno());
        throw msg.str();
    }

    std::stringstream ss;
    ss << h << ";" << p;
    return ss.str();
}
