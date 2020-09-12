# -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT([faiss], [1.0])
AC_COPYRIGHT([Copyright (c) Facebook, Inc. and its affiliates.

This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.])
AC_CONFIG_SRCDIR([Index.h])
AC_CONFIG_AUX_DIR([build-aux])
AC_CONFIG_MACRO_DIR([acinclude])

: ${CXXFLAGS="-g -O3 -Wall -Wextra"}

# Checks for programs.
AC_LANG(C++)
AC_PROG_CXX
AX_CXX_COMPILE_STDCXX([11], [noext], [mandatory])
AC_PROG_CPP
AC_PROG_MAKE_SET
AC_PROG_MKDIR_P

FA_PYTHON

if test x$PYTHON != x; then
   FA_NUMPY
fi

FA_PROG_SWIG

FA_CHECK_CUDA


# Checks for header files.
AC_CHECK_HEADERS([float.h limits.h stddef.h stdint.h stdlib.h string.h sys/time.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_CHECK_HEADER_STDBOOL
AC_C_INLINE
AC_TYPE_INT32_T
AC_TYPE_INT64_T
AC_C_RESTRICT
AC_TYPE_SIZE_T
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T

# Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_MMAP
AC_CHECK_FUNCS([clock_gettime floor gettimeofday memmove memset munmap pow sqrt strerror strstr])

AC_OPENMP

AX_BLAS
if test "x$ax_blas_ok" == "xno"; then
  AC_MSG_ERROR([An implementation of BLAS is required but none was found.])
fi

AX_LAPACK
if test "x$ax_lapack_ok" == "xno"; then
  AC_MSG_ERROR([An implementation of LAPACK is required but none was found.])
fi

AX_CPU_ARCH

AC_CONFIG_FILES([makefile.inc])
AC_OUTPUT
