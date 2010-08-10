/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
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
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

static void usage(void) {
    fprintf(stderr, "Usage: moveit -a servera -b serverb [-n num] [-B bid]"
            " [-v vbucketmigrator] [-o options]\n");
    exit(EXIT_FAILURE);
}

int main(int argc, char **argv)
{
    int c;
    int iteration = 5000;
    const char *server_a = NULL;
    const char *server_b = NULL;
    int bid = 0;
    const char *vbucketmigrator = "./vbucketmigrator";
    const char *options = "";

    while ((c = getopt(argc, argv, "a:b:B:n:v:o:?")) != EOF) {
        switch (c) {
        case 'a': server_a = optarg; break;
        case 'b': server_b = optarg; break;
        case 'n': iteration = atoi(optarg); break;
        case 'B': bid = atoi(optarg); break;
        case 'v': vbucketmigrator = optarg; break;
        case 'o': options = optarg; break;
        default:
            usage();
        }
    }

    if (server_a == NULL || server_b == NULL) {
        usage();
    }

    for (int ii = 0; ii < iteration; ++ii) {
        char cmd[1024];

        const char *from = (ii % 2) ? server_b : server_a;
        const char *to = (ii % 2) ? server_a : server_b;

        sprintf(cmd, "%s -h %s -d %s -b %u -t %s",
                vbucketmigrator, from, to, bid, options);
        fprintf(stderr, "\rMove data from %s to %s. (%u)", from, to, ii);
        fflush(stderr);
        int ret = system(cmd);
        if (ret != 0) {
            fprintf(stderr, " ERROR: vbucketmigrator failed with: %d\n", ret);
            return ret;
        }
    }
    fprintf(stderr, "\n");

    return 0;
}
