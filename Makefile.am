# vbucketmigrator
# Copyright (C) 2010 NorthScale, Inc
# All rights reserved.
#
# Use and distribution licensed under the BSD license.  See
# the COPYING file in this directory for full text.
ACLOCAL_AMFLAGS = -I m4 --force

bin_PROGRAMS = vbucketmigrator

vbucketmigrator_SOURCES = src/binarymessage.h \
                          src/binarymessagepipe.cc src/binarymessagepipe.h \
                          src/config_helper.h \
                          src/messagequeue.h \
                          src/mutex.h \
                          src/sockstream.cc src/sockstream.h \
                          src/vbucketmigrator.cc
vbucketmigrator_LDADD = ${LTLIBEVENT} ${LTLIBVBUCKET}

if BUILD_WINDOWS_FILES
vbucketmigrator_SOURCES += src/mutex_win32.cc src/winsock.cc
vbucketmigrator_LDADD += -lWs2_32 -lmswsock
else
if HAVE_PTHREAD
vbucketmigrator_SOURCES += src/mutex_pthread.cc
endif
endif

check_PROGRAMS=
TESTS=${check_PROGRAMS}

test: check-TESTS
