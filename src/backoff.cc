/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Membase, Inc.
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
#include "backoff.h"
#include "sockstream.h"
#include <pthread.h>
#include <iostream>
#include <unistd.h>


pthread_mutex_t slowdown_mutex = PTHREAD_MUTEX_INITIALIZER;
volatile bool slowdown;

static int backoffTime = 500;
static long maxQueueSize = 100000;

static Socket *monitor;

extern "C" {
    static void *backoffThread(void *args);
}

void setBackoffLimits(std::string limits) {
    size_t pos;
    if ((pos = limits.find(",")) == std::string::npos) {
        std::cerr << "You need to specify both parameters to -M" << std::endl;
        exit(EXIT_FAILURE);
    }
    std::string delay = limits.substr(0, pos);
    if (delay != "-") {
        backoffTime = strtol(delay.c_str(), NULL, 10);
    }
    std::string thr = limits.substr(pos + 1);
    if (thr != "-") {
        maxQueueSize = strtol(thr.c_str(), NULL, 10);
    }

    if (backoffTime == 0 || maxQueueSize == 0) {
        std::cerr << "Invalid values specified for -M" << std::endl;
        exit(EXIT_FAILURE);
    }

    std::cout << "Using backoff delay " << backoffTime << "ms and max size "
              << maxQueueSize << std::endl;
}

static void *backoffThread(void *args) {
    (void)args;
    std::string msg;
    std::ostream &out = *(monitor->getOutStream());
    std::istream &in = *(monitor->getInStream());

    long size = maxQueueSize;

    try {
        do {
            out << "stats" << std::endl;
            out.flush();

            char buffer[256];
            long dirty = 0;
            while (in.getline(buffer, sizeof(buffer))) {
                char *p = strchr(buffer, '\r');
                if (p != NULL) {
                    *p = '\0';
                }
                if (strstr(buffer,"END") != NULL) {
                    break;
                } else if ((p = strstr(buffer, "ep_queue_size")) != NULL) {
                    dirty += strtol(p + 14, NULL, 10);
                } else if ((p = strstr(buffer, "ep_flusher_todo")) != NULL) {
                    dirty += strtol(p + 16, NULL, 10);
                }
            }

            bool newval = (dirty > size);
            pthread_mutex_lock(&slowdown_mutex);
            slowdown = newval;
            pthread_mutex_unlock(&slowdown_mutex);
            if (newval) {
                // Let the queue drain a bit before letting it
                // it back up..
                size = maxQueueSize / 3;
            } else {
                // Ok, so keep the system running again..
                size = maxQueueSize;
            }

            sleep(1);
        } while (true);
    } catch (std::string &e) {
        msg = e;
    } catch (std::exception &e) {
        msg = e.what();
    } catch (...) {
        msg.assign("Unhandled exception");
    }

    if (msg.length() > 0) {
        std::cerr << "FATAL: " << msg << std::endl;
        exit(EXIT_FAILURE);
    }

    // NOTREACHED
    return NULL;
}

void startBackoffMonitorThread(std::string host)
{
    std::string msg;
    try {
        monitor = new Socket(host);
        if (verbosity) {
            std::cout << "Connecting to " << *monitor << std::endl;
        }
        monitor->connect();
    } catch (std::string &e) {
        msg = e;
    } catch (std::exception &e) {
        msg = e.what();
    } catch (...) {
        msg.assign("Unhandled exception");
    }

    if (msg.length() > 0) {
        std::cerr << "Failed to connect monitor connection: " << std::endl
                  << msg << std::endl;
        exit(EXIT_FAILURE);
    }

    pthread_attr_t attr;
    if (pthread_attr_init(&attr) != 0 ||
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED) != 0) {
        std::cerr << "Error setting up thread attributes" << std::endl;
        exit(EXIT_FAILURE);
    }

    pthread_t tid;
    if (pthread_create(&tid, &attr, backoffThread, NULL) != 0) {
        std::cerr << "Error creating stats thread" << std::endl;
        exit(EXIT_FAILURE);
    }

    pthread_attr_destroy(&attr);
}

void backoff(void) {
   bool delay;
   useconds_t val = backoffTime;
   do {
      pthread_mutex_lock(&slowdown_mutex);
      delay = slowdown;
      pthread_mutex_unlock(&slowdown_mutex);
      if (delay) {
          if (verbosity) {
              std::cout << "Backing off for " << val << "ms"
                        << std::endl;
          }
          usleep(val);
          val <<= 2;
          if (val > 10000) {
              val = 10000;
          }
      }
   } while (delay);
}
