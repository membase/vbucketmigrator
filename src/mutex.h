/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Copyright (C) 2010 NorthScale, Inc
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in this directory for full text.
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
