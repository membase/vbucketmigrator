/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Copyright (C) 2010 NorthScale, Inc
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in this directory for full text.
 */
#include "config.h"
#include "mutex.h"

#include <string>
#include <windows.h>

/**
 * Abstraction built on top of Windows mutexes
 */
Mutex::Mutex(void) throw (std::runtime_error) {
    if ((mutex = CreateMutex(NULL, FALSE, NULL)) == NULL) {
        throwError("MUTEX ERROR: Failed to initialize mutex: ");
    }
}

Mutex::~Mutex(void) throw (std::runtime_error) {
    CloseHandle(mutex);
}

void Mutex::acquire(void)  throw (std::runtime_error){
    DWORD error = WaitForSingleObject(mutex, INFINITE);
    if (error != WAIT_OBJECT_0) {
        throwError("MUTEX ERROR: Failed to acquire lock: ");
    }
}

void Mutex::release(void) throw (std::runtime_error) {
    if (!ReleaseMutex(mutex)) {
        throwError("MUTEX_ERROR: Failed to release lock: ");
    }
}

void Mutex::throwError(std::string msg) throw (std::runtime_error) {
    DWORD err = GetLastError();
    LPVOID error_msg;
    if (FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER |
                      FORMAT_MESSAGE_FROM_SYSTEM |
                      FORMAT_MESSAGE_IGNORE_INSERTS,
                      NULL, err, 0, (LPTSTR)&error_msg, 0, NULL) != 0) {
        msg.append(reinterpret_cast<const char*>(error_msg));
        LocalFree(error_msg);
    } else {
        msg.append("Failed to get error message");
    }
    throw std::runtime_error(msg);
}
