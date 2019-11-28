AC_DEFUN([FA_CHECK_CUDA], [

AC_ARG_WITH(cuda,
  [AS_HELP_STRING([--with-cuda=<prefix>], [prefix of the CUDA installation])])
AC_ARG_WITH(cuda-arch,
  [AS_HELP_STRING([--with-cuda-arch=<gencodes>], [device specific -gencode flags])],
  [],
  [with_cuda_arch=default])

if test x$with_cuda != xno; then
  if test x$with_cuda != x; then
    cuda_prefix=$with_cuda
    AC_CHECK_PROG(NVCC, [nvcc], [$cuda_prefix/bin/nvcc], [], [$cuda_prefix/bin])
    NVCC_CPPFLAGS="-I$cuda_prefix/include"
    NVCC_LDFLAGS="-L$cuda_prefix/lib64"
  else
    AC_CHECK_PROGS(NVCC, [nvcc /usr/local/cuda/bin/nvcc], [])
    if test "x$NVCC" == "x/usr/local/cuda/bin/nvcc"; then
      cuda_prefix="/usr/local/cuda"
      NVCC_CPPFLAGS="-I$cuda_prefix/include"
      NVCC_LDFLAGS="-L$cuda_prefix/lib64"
    else
      cuda_prefix=""
      NVCC_CPPFLAGS=""
      NVCC_LDFLAGS=""
    fi
  fi

  if test "x$NVCC" == x; then
    AC_MSG_ERROR([Couldn't find nvcc])
  fi

  if test "x$with_cuda_arch" == xdefault; then
    with_cuda_arch="-gencode=arch=compute_35,code=compute_35 \\
-gencode=arch=compute_52,code=compute_52 \\
-gencode=arch=compute_60,code=compute_60 \\
-gencode=arch=compute_61,code=compute_61 \\
-gencode=arch=compute_70,code=compute_70 \\
-gencode=arch=compute_75,code=compute_75"
  fi

  fa_save_CPPFLAGS="$CPPFLAGS"
  fa_save_LDFLAGS="$LDFLAGS"
  fa_save_LIBS="$LIBS"

  CPPFLAGS="$NVCC_CPPFLAGS $CPPFLAGS"
  LDFLAGS="$NVCC_LDFLAGS $LDFLAGS"

  AC_CHECK_HEADER([cuda.h], [], AC_MSG_FAILURE([Couldn't find cuda.h]))
  AC_CHECK_LIB([cublas], [cublasAlloc], [], AC_MSG_FAILURE([Couldn't find libcublas]))
  AC_CHECK_LIB([cudart], [cudaSetDevice], [], AC_MSG_FAILURE([Couldn't find libcudart]))

  NVCC_LIBS="$LIBS"
  NVCC_CPPFLAGS="$CPPFLAGS"
  NVCC_LDFLAGS="$LDFLAGS"
  CPPFLAGS="$fa_save_CPPFLAGS"
  LDFLAGS="$fa_save_LDFLAGS"
  LIBS="$fa_save_LIBS"
fi

AC_SUBST(NVCC)
AC_SUBST(NVCC_CPPFLAGS)
AC_SUBST(NVCC_LDFLAGS)
AC_SUBST(NVCC_LIBS)
AC_SUBST(CUDA_PREFIX, $cuda_prefix)
AC_SUBST(CUDA_ARCH, $with_cuda_arch)
])
