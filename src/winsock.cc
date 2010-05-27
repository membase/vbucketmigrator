/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Copyright (C) 2010 NorthScale, Inc
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in this directory for full text.
 */
#include "config.h"

#include <stdexcept>

void initialize_sockets(void) {
   WSADATA wsaData;
   if (WSAStartup(MAKEWORD(2,0), &wsaData) != 0) {
      throw std::runtime_error("WSAStartup failed");
   }
}
