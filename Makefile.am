# vbucketmigrator
#
#     Copyright 2010 NorthScale, Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

ACLOCAL_AMFLAGS = -I m4 --force

bin_PROGRAMS = vbucketmigrator

if BUILD_PORT_ADAPTOR
bin_PROGRAMS += port_adaptor
endif

port_adaptor_SOURCES = src/port_adaptor.c

vbucketmigrator_SOURCES = src/binarymessage.h \
                          src/binarymessagepipe.cc src/binarymessagepipe.h \
                          src/config_helper.h \
                          src/mutex.h \
                          src/sockstream.cc src/sockstream.h \
                          src/vbucketmigrator.cc
vbucketmigrator_LDADD = ${LTLIBEVENT} ${LTLIBVBUCKET} ${LTLIBSASL} ${LTLIBSASL2}


man_MANS =
if BUILD_DOCS
man_MANS += \
            vbucketmigrator.1m
endif

CLEANFILES= ${man_MANS}

vbucketmigrator.1m: docs/vbucketmigrator.pod
	${POD2MAN} -c "$*" -r "" -s 1m $< $@

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
