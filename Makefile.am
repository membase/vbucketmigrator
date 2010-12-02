# vbucketmigrator
#
#     Copyright 2010 Membase, Inc.
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

SPEC=vbm15to163.spec
RELEASE=1

ACLOCAL_AMFLAGS = -I m4 --force

bin_PROGRAMS = vbucketmigrator
noinst_PROGRAMS = moveit
EXTRA_DIST = docs LICENSE

vbucketmigrator_SOURCES = src/backoff.cc src/backoff.h \
                          src/binarymessage.h \
                          src/binarymessagepipe.cc src/binarymessagepipe.h \
                          src/buckets.cc src/buckets.h \
                          src/config_helper.h \
                          src/mutex.h \
                          src/sockstream.cc src/sockstream.h \
                          src/vbucketmigrator.cc
vbucketmigrator_LDADD = ${LTLIBEVENT}

if HAVE_SASL
vbucketmigrator_LDADD +=  ${LTLIBSASL} ${LTLIBSASL2}
endif

if BUILD_ISASL
vbucketmigrator_SOURCES += src/isasl.h src/isasl.c
endif

man_MANS =
if BUILD_DOCS
man_MANS += \
            vbucketmigrator.1m
endif

moveit_SOURCES = src/moveit.c

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

vbucketmigrator_LDADD += -lpthread

buckets_test_SOURCES = src/buckets.h src/buckets.cc test/buckets.cc

check_PROGRAMS=buckets_test
TESTS=${check_PROGRAMS}

test: check-TESTS

.builddir:; -@mkdir $@
.builddir/rpmtop/BUILD:; -@mkdir -p $@
.builddir/rpmtop/RPMS/i386:; -@mkdir -p $@
.builddir/rpmtop/RPMS/x86_64:; -@mkdir -p $@
.builddir/rpmtop/SOURCES:; -@mkdir -p $@
.builddir/rpmtop/SRPMS:; -@mkdir -p $@

dirs: .builddir/rpmtop/BUILD \
      .builddir/rpmtop/RPMS/i386 \
      .builddir/rpmtop/RPMS/x86_64 \
      .builddir/rpmtop/SOURCES \
      .builddir/rpmtop/SRPMS

vbm15to163.spec:
	cat ${SPEC}.in | sed "s/^Version:.*/Version: ${VERSION}/" | \
	sed "s/^Release:.*/Release: ${RELEASE}/" > ${SPEC}

rpm: all dist dirs vbm15to163.spec
	cp vbucketmigrator-$(VERSION).tar.gz ${PWD}/.builddir/SOURCES/
	rpmbuild --define="_topdir ${PWD}/.builddir/rpmtop" -ba vbm15to163.spec
	cp .builddir/rpmtop/RPMS/*/*.rpm .
