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
#include "buckets.h"

#include <string>
#include <sstream>
#include <iostream>

using namespace std;

static uint16_t str2bucketid(const char *&p) throw (std::string)
{
    const char *bid = p;
    uint32_t ret = 0;

    while (isdigit(*p)) {
        ret *= 10;
        ret += (*p - '0');
        ++p;
    }

    if ((ret & 0xffff0000) != 0) {
        string message = "Invalid bucket id: ";
        message.append(bid);
        throw message;
    }

    return static_cast<uint16_t>(ret);
}

static void skipWhite(const char *&p) {
    while (isspace(*p)) {
        ++p;
    }
}

static bool doParseBucketsRange(vector<uint16_t> &buckets, const char *arg) throw (std::string) {
    // syntax: [start,stop]
    const char *p = arg + 1;
    uint16_t start = str2bucketid(p);
    skipWhite(p);
    if (*p != ',') {
        return false;
    }
    ++p;
    skipWhite(p);
    uint16_t stop = str2bucketid(p);
    if (*p != ']') {
        return false;
    }
    skipWhite(p);
    ++p;
    skipWhite(p);
    if (*p != '\0') {
        return false;
    }

    while (start <= stop) {
        buckets.push_back(start);
        ++start;
    }
    return true;
}

static bool doParseBucketList(vector<uint16_t> &buckets, const char *arg) throw (std::string) {
    const char *p = arg;
    do {
        buckets.push_back(str2bucketid(p));
        skipWhite(p);
        if (*p) {
            if (*p != ',' && *p != ';') {
                return false;
            } else {
                ++p;
                skipWhite(p);
                if (*p == 0) {
                    return false;
                }
            }
        }
    } while (*p);
    return true;
}

void parseBuckets(vector<uint16_t> &buckets, const char *str) throw (std::string) {
    bool success = false;
    skipWhite(str);

    if (*str == '[') {
        success = doParseBucketsRange(buckets, str);
    } else if (*str) {
        // may be comma separated
        success = doParseBucketList(buckets, str);
    }

    if (!success) {
        std::stringstream err;
        err << "Incorrect syntax for -b: " << str << endl;
        throw err.str();
    }
}
