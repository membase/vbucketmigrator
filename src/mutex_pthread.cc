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
#include <cstring>


/**
 * Abstraction built on top of pthread mutexes
 */
Mutex::Mutex(void) throw (std::runtime_error) {
    int error = pthread_mutex_init(&mutex, NULL);
    if (error != 0) {
        std::string message = "MUTEX ERROR: Failed to initialize mutex: ";
        message.append(std::strerror(error));
        throw std::runtime_error(message);
    }
}

Mutex::~Mutex(void) throw (std::runtime_error) {
    int error = pthread_mutex_destroy(&mutex);
    if (error != 0) {
        std::string message = "MUTEX ERROR: Failed to destroy mutex: ";
        message.append(std::strerror(error));
        throw std::runtime_error(message);
    }
}

void Mutex::acquire(void) throw (std::runtime_error) {
    int error = pthread_mutex_lock(&mutex);
    if (error != 0) {
        std::string message = "MUTEX ERROR: Failed to acquire lock: ";
        message.append(std::strerror(error));
        throw std::runtime_error(message);
    }
}

void Mutex::release(void) throw (std::runtime_error) {
    int error = pthread_mutex_unlock(&mutex);
    if (error != 0) {
        std::string message = "MUTEX_ERROR: Failed to release lock: ";
        message.append(std::strerror(error));
        throw std::runtime_error(message);
    }
}
