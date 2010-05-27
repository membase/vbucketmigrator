/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Copyright (C) 2010 NorthScale, Inc
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in this directory for full text.
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
#define EAI_SYSTEM -11
extern void initialize_sockets(void);
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
