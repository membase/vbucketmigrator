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
