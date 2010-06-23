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
#ifndef MUTEX_H
#define MUTEX_H 1

#include "config.h"

#include <stdexcept>
#include <string.h>

#ifdef HAVE_WINDOWS_H
#include <windows.h>
#elif HAVE_PTHREAD_H
#include <pthread.h>
#else
#error "None of the supported interfaces available"
#endif

/**
 * A simple class to hide the platform dependencies for native synchronization
 * methods.
 * All functions will thrown std::runtime_exception if they encounter a problem
 */
class Mutex {
public:
    Mutex(void) throw (std::runtime_error);
    virtual ~Mutex(void) throw (std::runtime_error);
    void acquire(void) throw (std::runtime_error);
    void release(void) throw (std::runtime_error);

protected:
#ifdef HAVE_WINDOWS_H
    void throwError(std::string msg) throw (std::runtime_error);
    HANDLE mutex;
#elif HAVE_PTHREAD_H
    pthread_mutex_t mutex;
#else
#error "You might want to have a storage for your mutex.."
#endif
};

#endif
