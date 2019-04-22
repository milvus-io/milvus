#!/bin/sh
#
# Set environment variables so that we can compile rocksdb using
# fbcode settings.  It uses the latest g++ and clang compilers and also
# uses jemalloc
# Environment variables that change the behavior of this script:
# PIC_BUILD -- if true, it will only take pic versions of libraries from fbcode. libraries that don't have pic variant will not be included


BASEDIR=`dirname $BASH_SOURCE`
source "$BASEDIR/dependencies.sh"

CFLAGS=""

# libgcc
LIBGCC_INCLUDE="$LIBGCC_BASE/include"
LIBGCC_LIBS=" -L $LIBGCC_BASE/lib"

# glibc
GLIBC_INCLUDE="$GLIBC_BASE/include"
GLIBC_LIBS=" -L $GLIBC_BASE/lib"

# snappy
SNAPPY_INCLUDE=" -I $SNAPPY_BASE/include/"
if test -z $PIC_BUILD; then
  SNAPPY_LIBS=" $SNAPPY_BASE/lib/libsnappy.a"
else
  SNAPPY_LIBS=" $SNAPPY_BASE/lib/libsnappy_pic.a"
fi
CFLAGS+=" -DSNAPPY"

if test -z $PIC_BUILD; then
  # location of zlib headers and libraries
  ZLIB_INCLUDE=" -I $ZLIB_BASE/include/"
  ZLIB_LIBS=" $ZLIB_BASE/lib/libz.a"
  CFLAGS+=" -DZLIB"

  # location of bzip headers and libraries
  BZIP_INCLUDE=" -I $BZIP2_BASE/include/"
  BZIP_LIBS=" $BZIP2_BASE/lib/libbz2.a"
  CFLAGS+=" -DBZIP2"

  LZ4_INCLUDE=" -I $LZ4_BASE/include/"
  LZ4_LIBS=" $LZ4_BASE/lib/liblz4.a"
  CFLAGS+=" -DLZ4"
fi

ZSTD_INCLUDE=" -I $ZSTD_BASE/include/"
if test -z $PIC_BUILD; then
  ZSTD_LIBS=" $ZSTD_BASE/lib/libzstd.a"
else
  ZSTD_LIBS=" $ZSTD_BASE/lib/libzstd_pic.a"
fi
CFLAGS+=" -DZSTD -DZSTD_STATIC_LINKING_ONLY"

# location of gflags headers and libraries
GFLAGS_INCLUDE=" -I $GFLAGS_BASE/include/"
if test -z $PIC_BUILD; then
  GFLAGS_LIBS=" $GFLAGS_BASE/lib/libgflags.a"
else
  GFLAGS_LIBS=" $GFLAGS_BASE/lib/libgflags_pic.a"
fi
CFLAGS+=" -DGFLAGS=gflags"

# location of jemalloc
JEMALLOC_INCLUDE=" -I $JEMALLOC_BASE/include/"
JEMALLOC_LIB=" $JEMALLOC_BASE/lib/libjemalloc.a"

if test -z $PIC_BUILD; then
  # location of numa
  NUMA_INCLUDE=" -I $NUMA_BASE/include/"
  NUMA_LIB=" $NUMA_BASE/lib/libnuma.a"
  CFLAGS+=" -DNUMA"

  # location of libunwind
  LIBUNWIND="$LIBUNWIND_BASE/lib/libunwind.a"
fi

# location of TBB
TBB_INCLUDE=" -isystem $TBB_BASE/include/"
if test -z $PIC_BUILD; then
  TBB_LIBS="$TBB_BASE/lib/libtbb.a"
else
  TBB_LIBS="$TBB_BASE/lib/libtbb_pic.a"
fi
CFLAGS+=" -DTBB"

# use Intel SSE support for checksum calculations
export USE_SSE=1
export PORTABLE=1

BINUTILS="$BINUTILS_BASE/bin"
AR="$BINUTILS/ar"

DEPS_INCLUDE="$SNAPPY_INCLUDE $ZLIB_INCLUDE $BZIP_INCLUDE $LZ4_INCLUDE $ZSTD_INCLUDE $GFLAGS_INCLUDE $NUMA_INCLUDE $TBB_INCLUDE"

STDLIBS="-L $GCC_BASE/lib64"

CLANG_BIN="$CLANG_BASE/bin"
CLANG_LIB="$CLANG_BASE/lib"
CLANG_SRC="$CLANG_BASE/../../src"

CLANG_ANALYZER="$CLANG_BIN/clang++"
CLANG_SCAN_BUILD="$CLANG_SRC/llvm/tools/clang/tools/scan-build/bin/scan-build"

if [ -z "$USE_CLANG" ]; then
  # gcc
  CC="$GCC_BASE/bin/gcc"
  CXX="$GCC_BASE/bin/g++"

  CFLAGS+=" -B$BINUTILS/gold"
  CFLAGS+=" -isystem $GLIBC_INCLUDE"
  CFLAGS+=" -isystem $LIBGCC_INCLUDE"
  JEMALLOC=1
else
  # clang
  CLANG_INCLUDE="$CLANG_LIB/clang/stable/include"
  CC="$CLANG_BIN/clang"
  CXX="$CLANG_BIN/clang++"

  KERNEL_HEADERS_INCLUDE="$KERNEL_HEADERS_BASE/include"

  CFLAGS+=" -B$BINUTILS/gold -nostdinc -nostdlib"
  CFLAGS+=" -isystem $LIBGCC_BASE/include/c++/5.x "
  CFLAGS+=" -isystem $LIBGCC_BASE/include/c++/5.x/x86_64-facebook-linux "
  CFLAGS+=" -isystem $GLIBC_INCLUDE"
  CFLAGS+=" -isystem $LIBGCC_INCLUDE"
  CFLAGS+=" -isystem $CLANG_INCLUDE"
  CFLAGS+=" -isystem $KERNEL_HEADERS_INCLUDE/linux "
  CFLAGS+=" -isystem $KERNEL_HEADERS_INCLUDE "
  CFLAGS+=" -Wno-expansion-to-defined "
  CXXFLAGS="-nostdinc++"
fi

CFLAGS+=" $DEPS_INCLUDE"
CFLAGS+=" -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DROCKSDB_FALLOCATE_PRESENT -DROCKSDB_MALLOC_USABLE_SIZE -DROCKSDB_RANGESYNC_PRESENT -DROCKSDB_SCHED_GETCPU_PRESENT -DROCKSDB_SUPPORT_THREAD_LOCAL -DHAVE_SSE42"
CXXFLAGS+=" $CFLAGS"

EXEC_LDFLAGS=" $SNAPPY_LIBS $ZLIB_LIBS $BZIP_LIBS $LZ4_LIBS $ZSTD_LIBS $GFLAGS_LIBS $NUMA_LIB $TBB_LIBS"
EXEC_LDFLAGS+=" -B$BINUTILS/gold"
EXEC_LDFLAGS+=" -Wl,--dynamic-linker,/usr/local/fbcode/gcc-5-glibc-2.23/lib/ld.so"
EXEC_LDFLAGS+=" $LIBUNWIND"
EXEC_LDFLAGS+=" -Wl,-rpath=/usr/local/fbcode/gcc-5-glibc-2.23/lib"
# required by libtbb
EXEC_LDFLAGS+=" -ldl"

PLATFORM_LDFLAGS="$LIBGCC_LIBS $GLIBC_LIBS $STDLIBS -lgcc -lstdc++"

EXEC_LDFLAGS_SHARED="$SNAPPY_LIBS $ZLIB_LIBS $BZIP_LIBS $LZ4_LIBS $ZSTD_LIBS $GFLAGS_LIBS $TBB_LIBS"

VALGRIND_VER="$VALGRIND_BASE/bin/"

LUA_PATH="$LUA_BASE"

if test -z $PIC_BUILD; then
  LUA_LIB=" $LUA_PATH/lib/liblua.a"
else
  LUA_LIB=" $LUA_PATH/lib/liblua_pic.a"
fi

export CC CXX AR CFLAGS CXXFLAGS EXEC_LDFLAGS EXEC_LDFLAGS_SHARED VALGRIND_VER JEMALLOC_LIB JEMALLOC_INCLUDE CLANG_ANALYZER CLANG_SCAN_BUILD LUA_PATH LUA_LIB
