dnl Copyright (C) 2009 Facebook
dnl Copying and distribution of this file, with or without modification,
dnl are permitted in any medium without royalty provided the copyright
dnl notice and this notice are preserved.

AC_DEFUN([FB_INITIALIZE],
[
AM_INIT_AUTOMAKE([ foreign 1.9.5 no-define ])
if test "x$1" = "xlocalinstall"; then
wdir=`pwd`
# To use $wdir undef quote.
#
##########
AC_PREFIX_DEFAULT([`pwd`/install])
echo
fi
AC_PROG_CC
AC_PROG_CXX
AC_PROG_RANLIB(RANLIB, ranlib)
AC_PATH_PROGS(BASH, bash)
AC_PATH_PROGS(PERL, perl)
AC_PATH_PROGS(PYTHON, python)
AC_PATH_PROGS(AR, ar)
AC_PATH_PROGS(ANT, ant)
PRODUCT_MK=""
])

AC_DEFUN([FB_WITH_EXTERNAL_PATH],
[
cdir=`pwd`
AC_MSG_CHECKING([Checking EXTERNAL_PATH set to])
AC_ARG_WITH([externalpath],
  [ --with-externalpath=DIR User specified path to external facebook components.],
  [
    if test "x${EXTERNAL_PATH}" != "x"; then
       echo ""
       echo "ERROR: You have already set EXTERNAL_PATH in your environment"
       echo "Cannot override it using --with-externalpath. Unset EXTERNAL_PATH to use this option"
       exit 1
    fi
    EXTERNAL_PATH=$withval
  ],
  [
    if test "x${EXTERNAL_PATH}" = "x"; then
       EXTERNAL_PATH=$1
    fi
  ]
)
if test "x${EXTERNAL_PATH}" = "x"; then
   export EXTERNAL_PATH="$cdir/external"
   GLOBAL_HEADER_MK="include ${EXTERNAL_PATH}/global_header.mk"
   GLOBAL_FOOTER_MK="include ${EXTERNAL_PATH}/global_footer.mk"
else
   export EXTERNAL_PATH
   GLOBAL_HEADER_MK="include ${EXTERNAL_PATH}/global_header.mk"
   GLOBAL_FOOTER_MK="include ${EXTERNAL_PATH}/global_footer.mk"
fi
AC_MSG_RESULT($EXTERNAL_PATH)
if test ! -d ${EXTERNAL_PATH}; then
       echo ""
       echo "ERROR: EXTERNAL_PATH set to an nonexistent directory ${EXTERNAL_PATH}"
       exit 1
fi
AC_SUBST(EXTERNAL_PATH)
AC_SUBST(GLOBAL_HEADER_MK)
AC_SUBST(GLOBAL_FOOTER_MK)
])

# Set option to enable shared mode. Set DEBUG and OPT for use in Makefile.am.
AC_DEFUN([FB_ENABLE_DEFAULT_OPT_BUILD],
[
AC_MSG_CHECKING([whether to enable optimized build])
AC_ARG_ENABLE([opt],
  [  --disable-opt     Set up debug mode.],
  [
     ENABLED_OPT=$enableval
  ],
  [
     ENABLED_OPT="yes"
  ]
)
if test "$ENABLED_OPT" = "yes"
then
     CFLAGS="-Wall -O3"
     CXXFLAGS="-Wall -O3"
else
     CFLAGS="-Wall -g"
     CXXFLAGS="-Wall -g"
fi
AC_MSG_RESULT($ENABLED_OPT)
AM_CONDITIONAL([OPT], [test "$ENABLED_OPT" = yes])
AM_CONDITIONAL([DEBUG], [test "$ENABLED_OPT" = no])
])

# Set option to enable debug mode. Set DEBUG and OPT for use in Makefile.am.
AC_DEFUN([FB_ENABLE_DEFAULT_DEBUG_BUILD],
[
AC_MSG_CHECKING([whether to enable debug build])
AC_ARG_ENABLE([debug],
  [  --disable-debug     Set up opt mode.],
  [
     ENABLED_DEBUG=$enableval
  ],
  [
     ENABLED_DEBUG="yes"
  ]
)
if test "$ENABLED_DEBUG" = "yes"
then
     CFLAGS="-Wall -g"
     CXXFLAGS="-Wall -g"
else
     CFLAGS="-Wall -O3"
     CXXFLAGS="-Wall -O3"
fi
AC_MSG_RESULT($ENABLED_DEBUG)
AM_CONDITIONAL([DEBUG], [test "$ENABLED_DEBUG" = yes])
AM_CONDITIONAL([OPT], [test "$ENABLED_DEBUG" = no])
])

# Set option to enable static libs.
AC_DEFUN([FB_ENABLE_DEFAULT_STATIC],
[
SHARED=""
STATIC=""
AC_MSG_CHECKING([whether to enable static mode])
AC_ARG_ENABLE([static],
  [  --disable-static     Set up shared mode.],
  [
     ENABLED_STATIC=$enableval
  ],
  [
     ENABLED_STATIC="yes"
  ]
)
if test "$ENABLED_STATIC" = "yes"
then
     LTYPE=".a"
else
     LTYPE=".so"
     SHARED_CXXFLAGS="-fPIC"
     SHARED_CFLAGS="-fPIC"
     SHARED_LDFLAGS="-shared -fPIC"
     AC_SUBST(SHARED_CXXFLAGS)
     AC_SUBST(SHARED_CFLAGS)
     AC_SUBST(SHARED_LDFLAGS)
fi
AC_MSG_RESULT($ENABLED_STATIC)
AC_SUBST(LTYPE)
AM_CONDITIONAL([STATIC], [test "$ENABLED_STATIC" = yes])
AM_CONDITIONAL([SHARED], [test "$ENABLED_STATIC" = no])
])

# Set option to enable shared libs.
AC_DEFUN([FB_ENABLE_DEFAULT_SHARED],
[
SHARED=""
STATIC=""
AC_MSG_CHECKING([whether to enable shared mode])
AC_ARG_ENABLE([shared],
  [  --disable-shared     Set up static mode.],
  [
    ENABLED_SHARED=$enableval
  ],
  [
     ENABLED_SHARED="yes"
  ]
)
if test "$ENABLED_SHARED" = "yes"
then
     LTYPE=".so"
     SHARED_CXXFLAGS="-fPIC"
     SHARED_CFLAGS="-fPIC"
     SHARED_LDFLAGS="-shared -fPIC"
     AC_SUBST(SHARED_CXXFLAGS)
     AC_SUBST(SHARED_CFLAGS)
     AC_SUBST(SHARED_LDFLAGS)
else
     LTYPE=".a"
fi
AC_MSG_RESULT($ENABLED_SHARED)
AC_SUBST(LTYPE)
AM_CONDITIONAL([SHARED], [test "$ENABLED_SHARED" = yes])
AM_CONDITIONAL([STATIC], [test "$ENABLED_SHARED" = no])
])

# Generates define flags and conditionals as specified by user.
# This gets enabled *only* if user selects --enable-<FEATURE> otion.
AC_DEFUN([FB_ENABLE_FEATURE],
[
ENABLE=""
flag="$1"
value="$3"
AC_MSG_CHECKING([whether to enable $1])
AC_ARG_ENABLE([$2],
  [  --enable-$2     Enable $2.],
  [
     ENABLE=$enableval
  ],
  [
     ENABLE="no"
  ]
)
AM_CONDITIONAL([$1], [test "$ENABLE" = yes])
if test "$ENABLE" = "yes"
then
   if test "x${value}" = "x"
   then
       AC_DEFINE([$1])
   else
       AC_DEFINE_UNQUOTED([$1], [$value])
   fi
fi
AC_MSG_RESULT($ENABLE)
])


# can also use eval $2=$withval;AC_SUBST($2)
AC_DEFUN([FB_WITH_PATH],
[
USRFLAG=""
USRFLAG=$1
AC_MSG_CHECKING([Checking $1 set to])
AC_ARG_WITH([$2],
  [ --with-$2=DIR User specified path.],
  [
    LOC=$withval
    eval $USRFLAG=$withval
  ],
  [
    LOC=$3
    eval $USRFLAG=$3
  ]
)
AC_SUBST([$1])
AC_MSG_RESULT($LOC)
])

AC_DEFUN([FB_SET_FLAG_VALUE],
[
SETFLAG=""
AC_MSG_CHECKING([Checking $1 set to])
SETFLAG=$1
eval $SETFLAG=\"$2\"
AC_SUBST([$SETFLAG])
AC_MSG_RESULT($2)
])

# NOTES
# if using if else bourne stmt you must have more than a macro in it.
# EX1 is not correct. EX2 is correct
# EX1: if test "$XX" = "yes"; then
#        AC_SUBST(xx)
#      fi
# EX2: if test "$XX" = "yes"; then
#        xx="foo"
#        AC_SUBST(xx)
#      fi
