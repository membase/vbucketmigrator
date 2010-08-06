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

AC_ARG_WITH([docs],
  [AS_HELP_STRING([--with-docs],
    [Generate documentation (yes|no) @<:@default=yes@:>@])],
  [with_docs=$withval],
  [with_docs=yes])

AS_IF([test "$with_docs" = "yes"],
  [
    REQUIRE_POD2MAN
  ])
AM_CONDITIONAL(BUILD_DOCS, test "$with_docs" = "yes")

AC_ARG_WITH([sasl],
  [AS_HELP_STRING([--with-sasl],
    [with support for SASL (yes|no) @<:@default=yes@:>@])],
  [with_sasl=$withval],
  [with_sasl=yes])

sasl_support=none
AS_IF([test "$with_sasl" = "yes"],
  [
    PANDORA_REQUIRE_SASL
    AC_DEFINE(ENABLE_SASL, 1, [Enable sasl support])
    sasl_support=sasl
  ])

AC_ARG_WITH([isasl],
  [AS_HELP_STRING([--with-isasl],
    [with support for iSASL (yes|no) @<:@default=no@:>@])],
  [with_isasl=$withval],
  [with_isasl=no])

AS_IF([test "$with_isasl" = "yes"],
      [AS_IF([test "$with_sasl" = "yes"],
             [AC_MSG_ERROR([Can't use both sasl and isasl])])
       AC_DEFINE(HAVE_ISASL, 1, [Define if you want to use iSASL])
       AC_DEFINE(ENABLE_SASL, 1, [Enable sasl support])
       sasl_support=isasl
      ])

AM_CONDITIONAL(BUILD_ISASL, test "$with_isasl" = "yes")

AC_CHECK_FUNCS_ONCE(fork getpass)
AM_CONDITIONAL(BUILD_PORT_ADAPTOR, test "x$ac_cv_func_fork" = "xyes")

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
echo "   * SASL support:              $sasl_support"
echo "   * port_adaptor support:      $ac_cv_func_fork"
echo ""
echo "---"

AS_IF(test "$with_docs" = "no",
  [
    echo "*****"
    echo "*"
    echo "*  WARNING: You are not generating any documentation."
    echo "*           Please don't ship vbucketmigrator to an end user"
    echo "*           without documentation..."
    echo "*"
    echo "*****"
  ])
