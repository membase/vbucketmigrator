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
#ifndef CONFIG_HELPER_H
#define CONDIF_HELPER_H 1

// The intention of this file is to avoid cluttering the code with #ifdefs


#if ((defined (__SUNPRO_C) || defined(__SUNPRO_CC)) || defined __GNUC__)
#define EXPORT_FUNCTION __attribute__ ((visibility("default")))
#else
#define EXPORT_FUNCTION
#endif

#if defined(WIN32) || defined(__WIN32__)
#define _WIN32_WINNT    0x0501
typedef short in_port_t;
#define EADDRINUSE WSAEADDRINUSE
#define EWOULDBLOCK WSAEWOULDBLOCK
#define EINPROGRESS WSAEINPROGRESS
#define EALREADY WSAEALREADY
#define EISCONN WSAEISCONN
#define ENOTCONN WSAENOTCONN
#define ENOBUFS WSAENOBUFS
#define SHUT_RDWR SD_BOTH
#define EAI_SYSTEM -11
extern void initialize_sockets(void);
#define get_socket_errno() WSAGetLastError()

#else
#define initialize_sockets()
#define SOCKET int
#define INVALID_SOCKET -1
#define SOCKET_ERROR -1
#define SD_SEND 1
#ifdef __cplusplus
#define closesocket(a) ::close(a)
#else
#define closesocket(a) close(a)
#endif
#define get_socket_errno() errno
#endif

#ifdef HAVE_ARPA_INET_H
#include <arpa/inet.h>
#endif

#ifdef HAVE_SOCKET_H
#include <socket.h>
#endif

#ifdef HAVE_NETINET_IN_H
#include <netinet/in.h>
#endif

#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif

#ifdef HAVE_WINSOCK2_H
#include <winsock2.h>
#endif

#ifdef HAVE_WS2TCPIP_H
#include <ws2tcpip.h>
#endif

#ifdef __cplusplus
#include <cstdlib>
#else
#include <stdlib.h>
#endif

#ifdef HAVE_SYSEXITS_H
#include <sysexits.h>
#endif

#ifdef HAVE_SASL_SASL_H
#include <sasl/sasl.h>
#endif

#ifndef EX_USAGE
#define EX_USAGE EXIT_FAILURE
#endif
#ifndef EX_CONFIG
#define EX_CONFIG (EX_USAGE + 1)
#endif
#ifndef EX_IOERR
#define EX_IOERR (EX_CONFIG + 1)
#endif
#ifndef EX_OK
#define EX_OK EXIT_SUCCESS
#endif

#endif
