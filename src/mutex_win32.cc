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
