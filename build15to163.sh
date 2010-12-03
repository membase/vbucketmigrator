#!/bin/sh

git clean -dfx && \
config/autorun.sh && \
./configure --program-transform-name='s/^vbucketmigrator$/vbm15to163/' && \
make rpm
