# vbucketmigrator
# Copyright (C) 2010 NorthScale, Inc
# All rights reserved.
#
# Use and distribution licensed under the BSD license.  See
# the COPYING file in this directory for full text.
AC_PREREQ(2.59)
m4_include([m4/version.m4])
AC_INIT(vbucketmigrator, VERSION_NUMBER, trond.norbye@gmail.com)
AC_CONFIG_SRCDIR([src/vbucketmigrator.cc])
AC_CONFIG_AUX_DIR(config)
AM_CONFIG_HEADER([config.h])
AC_CONFIG_MACRO_DIR([m4])
AC_DISABLE_STATIC

PANDORA_CANONICAL_TARGET(less-warnings, warnings-always-on, require-cxx,
                         force-gcc42)
PANDORA_REQUIRE_LIBEVENT
PANDORA_REQUIRE_LIBVBUCKET
PANDORA_REQUIRE_SASL
REQUIRE_POD2MAN

AH_TOP([
#ifndef CONFIG_H
#define CONFIG_H
/*
 * Copyright (C) 2010 NorthScale, Inc
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in this directory for full text.
 */
#ifdef _SYS_FEATURE_TESTS_H
#error "You should include config.h as your first include file"
#endif
])

AC_DEFUN([AC_C_HTONLL],
[
    AC_CACHE_CHECK([for htonll],[ac_cv_have_htonll],[
    AC_RUN_IFELSE([
       AC_LANG_PROGRAM([[
#include <sys/types.h>
#include <netinet/in.h>
#include <inttypes.h>
       ]],[[
          return htonll(0);
       ]])
    ], [
      ac_cv_have_htonll=yes
    ],[
      ac_cv_have_htonll=no
    ])])

    AS_IF([test "x$ac_cv_have_htonll" = "xyes"],[
      AC_DEFINE([HAVE_HTONLL], [1], [Have ntohll])])
])

AC_C_HTONLL

AC_CHECK_HEADERS([arpa/inet.h pthread.h windows.h winsock2.h ws2tcpip.h socket.h netinet/in.h netdb.h sysexits.h sasl/sasl.h])

AS_IF([test "x${ac_cv_header_windows_h}" = "xno"],
      [AC_SEARCH_LIBS(pthread_create, pthread)])

AM_CONDITIONAL(BUILD_WINDOWS_FILES,
               [test "x${ac_cv_header_windows_h}" = "xyes"])
AM_CONDITIONAL(HAVE_PTHREAD,
               [test "x${ac_cv_header_pthread_h}" = "xyes"])

AC_SEARCH_LIBS(gethostbyname, nsl socket)
AC_SEARCH_LIBS(getaddrinfo, nsl socket)
AC_SEARCH_LIBS(socket, nsl socket)

AC_ARG_WITH([memcached],
    [AS_HELP_STRING([--with-memcached=PATH],
       [Specify path to memcached source tree @<:@default=../memcached@:>@])],
    [ac_with_memcached="$withval"],
    [ac_with_memcached="../memcached"])

AC_MSG_CHECKING([for memcached location])
AS_IF([ test -z "$ac_with_memcached" ],
      [ AC_MSG_ERROR([You can't specify an empty location])],
      [ AS_IF([ test -d $ac_with_memcached ],
              [ MEMCACHED_DIR=$ac_with_memcached ],
              [ AC_MSG_ERROR([
"$ac_with_memcached" is not a directory.
Specify its path using --with-memcached. ex: --with-memcached=../memcached
])
              ])
      ])
AC_MSG_RESULT([$ac_with_memcached])
MEMCACHED_DIR=$ac_with_memcached
AC_SUBST(MEMCACHED_DIR)
AM_CPPFLAGS="$AM_CPPFLAGS -I$MEMCACHED_DIR/include"

AS_IF([test "x$SUNCC" = "xyes"],
      [
        CFLAGS="-D_XOPEN_SOURCE=600 $CFLAGS"
        CPPFLAGS="-xldscope=hidden -mt $CPPFLAGS"
      ])

AH_BOTTOM([
#include "src/config_helper.h"
#endif
])

dnl ----------------------------------------------------------------------------

AC_CONFIG_FILES(Makefile)
AC_OUTPUT
echo "---"
echo "Configuration summary for $PACKAGE_NAME version $VERSION"
echo ""
echo "   * Installation prefix:       $prefix"
echo "   * System type:               $host_vendor-$host_os"
echo "   * Host CPU:                  $host_cpu"
echo "   * C Compiler:                $CC_VERSION"
echo "   * C++ Compiler:              $CXX_VERSION"
echo "   * Assertions enabled:        $ac_cv_assert"
echo "   * Debug enabled:             $with_debug"
echo "   * Warnings as failure:       $ac_cv_warnings_as_errors"
echo "   * Memcached location:        $ac_with_memcached"
echo ""
echo "---"
