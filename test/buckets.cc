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
#include "buckets.h"
#include <assert.h>
#include <iostream>
#include <cassert>
#include <cstdlib>

using namespace std;

static void testSingleBucket() {
    vector<uint16_t> buckets;

    try {
        parseBuckets(buckets, "102");
    } catch (string& e) {
        cerr << e.c_str() << std::endl;
        abort();
    }
    assert(buckets.size() == 1);
    assert(buckets[0] == 102);
}

static void testMultipleBuckets() {
    vector<uint16_t> buckets;

    try {
        parseBuckets(buckets, "102,   \t\n\r 103; 104  ");
    } catch (string& e) {
        cerr << e.c_str() << std::endl;
        abort();
    }
    assert(buckets.size() == 3);
    assert(buckets[0] == 102);
    assert(buckets[1] == 103);
    assert(buckets[2] == 104);
}

static void testBucketRange() {
    vector<uint16_t> buckets;

    try {
        parseBuckets(buckets, "[102, \t\n\r 105]");
    } catch (string& e) {
        cerr << e.c_str() << std::endl;
        abort();
    }
    assert(buckets.size() == 4);
    assert(buckets[0] == 102);
    assert(buckets[1] == 103);
    assert(buckets[2] == 104);
    assert(buckets[3] == 105);
}

static void testIllegalSyntax() {
    vector<uint16_t> buckets;

    try {
        parseBuckets(buckets, "[102, 10 2]");
        abort();
    } catch (string& e) {
        /* Success! */
    }

    try {
        parseBuckets(buckets, "[102,,]");
        abort();
    } catch (string& e) {
        /* Success! */
    }

    try {
        parseBuckets(buckets, "[a,12]");
        abort();
    } catch (string& e) {
        /* Success! */
    }

    try {
        parseBuckets(buckets, "[1,a]");
        abort();
    } catch (string& e) {
        /* Success! */
    }

    try {
        parseBuckets(buckets, "1,a,2,3");
        abort();
    } catch (string& e) {
        /* Success! */
    }

    try {
        parseBuckets(buckets, "1,,");
        abort();
    } catch (string& e) {
        /* Success! */
    }

    try {
        parseBuckets(buckets, "1,");
        abort();
    } catch (string& e) {
        /* Success! */
    }

    try {
        parseBuckets(buckets, "1 2 3");
        abort();
    } catch (string& e) {
        /* Success! */
    }

    try {
        parseBuckets(buckets, "");
        abort();
    } catch (string& e) {
        /* Success! */
    }
}


int main(int argc, char **argv) {
    (void)argc;
    (void)argv;

    testSingleBucket();
    testMultipleBuckets();
    testBucketRange();
    testIllegalSyntax();

    return 0;
}
