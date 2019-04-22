#!/bin/sh
#
# Set environment variables so that we can compile rocksdb using
# fbcode settings.  It uses the latest g++ compiler and also
# uses jemalloc

BASEDIR=`dirname $BASH_SOURCE`
source "$BASEDIR/dependencies_4.8.1.sh"

# location of libgcc
LIBGCC_INCLUDE="$LIBGCC_BASE/include"
LIBGCC_LIBS=" -L $LIBGCC_BASE/lib"

# location of glibc
GLIBC_INCLUDE="$GLIBC_BASE/include"
GLIBC_LIBS=" -L $GLIBC_BASE/lib"

# location of snappy headers and libraries
SNAPPY_INCLUDE=" -I $SNAPPY_BASE/include"
SNAPPY_LIBS=" $SNAPPY_BASE/lib/libsnappy.a"

# location of zlib headers and libraries
ZLIB_INCLUDE=" -I $ZLIB_BASE/include"
ZLIB_LIBS=" $ZLIB_BASE/lib/libz.a"

# location of bzip headers and libraries
BZIP2_INCLUDE=" -I $BZIP2_BASE/include/"
BZIP2_LIBS=" $BZIP2_BASE/lib/libbz2.a"

LZ4_INCLUDE=" -I $LZ4_BASE/include"
LZ4_LIBS=" $LZ4_BASE/lib/liblz4.a"

ZSTD_INCLUDE=" -I $ZSTD_BASE/include"
ZSTD_LIBS=" $ZSTD_BASE/lib/libzstd.a"

# location of gflags headers and libraries
GFLAGS_INCLUDE=" -I $GFLAGS_BASE/include/"
GFLAGS_LIBS=" $GFLAGS_BASE/lib/libgflags.a"

# location of jemalloc
JEMALLOC_INCLUDE=" -I $JEMALLOC_BASE/include"
JEMALLOC_LIB="$JEMALLOC_BASE/lib/libjemalloc.a"

# location of numa
NUMA_INCLUDE=" -I $NUMA_BASE/include/"
NUMA_LIB=" $NUMA_BASE/lib/libnuma.a"

# location of libunwind
LIBUNWIND="$LIBUNWIND_BASE/lib/libunwind.a"

# location of tbb
TBB_INCLUDE=" -isystem $TBB_BASE/include/"
TBB_LIBS="$TBB_BASE/lib/libtbb.a"

# use Intel SSE support for checksum calculations
export USE_SSE=1
export PORTABLE=1

BINUTILS="$BINUTILS_BASE/bin"
AR="$BINUTILS/ar"

DEPS_INCLUDE="$SNAPPY_INCLUDE $ZLIB_INCLUDE $BZIP2_INCLUDE $LZ4_INCLUDE $ZSTD_INCLUDE $GFLAGS_INCLUDE $NUMA_INCLUDE $TBB_INCLUDE"

STDLIBS="-L $GCC_BASE/lib64"

if [ -z "$USE_CLANG" ]; then
  # gcc
  CC="$GCC_BASE/bin/gcc"
  CXX="$GCC_BASE/bin/g++"

  CFLAGS="-B$BINUTILS/gold -m64 -mtune=generic"
  CFLAGS+=" -isystem $GLIBC_INCLUDE"
  CFLAGS+=" -isystem $LIBGCC_INCLUDE"
  JEMALLOC=1
else
  # clang
  CLANG_BIN="$CLANG_BASE/bin"
  CLANG_LIB="$CLANG_BASE/lib"
  CLANG_INCLUDE="$CLANG_LIB/clang/*/include"
  CC="$CLANG_BIN/clang"
  CXX="$CLANG_BIN/clang++"

  KERNEL_HEADERS_INCLUDE="$KERNEL_HEADERS_BASE/include/"

  CFLAGS="-B$BINUTILS/gold -nostdinc -nostdlib"
  CFLAGS+=" -isystem $LIBGCC_BASE/include/c++/4.8.1 "
  CFLAGS+=" -isystem $LIBGCC_BASE/include/c++/4.8.1/x86_64-facebook-linux "
  CFLAGS+=" -isystem $GLIBC_INCLUDE"
  CFLAGS+=" -isystem $LIBGCC_INCLUDE"
  CFLAGS+=" -isystem $CLANG_INCLUDE"
  CFLAGS+=" -isystem $KERNEL_HEADERS_INCLUDE/linux "
  CFLAGS+=" -isystem $KERNEL_HEADERS_INCLUDE "
  CXXFLAGS="-nostdinc++"
fi

CFLAGS+=" $DEPS_INCLUDE"
CFLAGS+=" -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DROCKSDB_FALLOCATE_PRESENT -DROCKSDB_MALLOC_USABLE_SIZE -DROCKSDB_RANGESYNC_PRESENT -DROCKSDB_SCHED_GETCPU_PRESENT -DROCKSDB_SUPPORT_THREAD_LOCAL -DHAVE_SSE42"
CFLAGS+=" -DSNAPPY -DGFLAGS=google -DZLIB -DBZIP2 -DLZ4 -DZSTD -DNUMA -DTBB"
CXXFLAGS+=" $CFLAGS"

EXEC_LDFLAGS=" $SNAPPY_LIBS $ZLIB_LIBS $BZIP2_LIBS $LZ4_LIBS $ZSTD_LIBS $GFLAGS_LIBS $NUMA_LIB $TBB_LIBS"
EXEC_LDFLAGS+=" -Wl,--dynamic-linker,/usr/local/fbcode/gcc-4.8.1-glibc-2.17/lib/ld.so"
EXEC_LDFLAGS+=" $LIBUNWIND"
EXEC_LDFLAGS+=" -Wl,-rpath=/usr/local/fbcode/gcc-4.8.1-glibc-2.17/lib"
# required by libtbb
EXEC_LDFLAGS+=" -ldl"

PLATFORM_LDFLAGS="$LIBGCC_LIBS $GLIBC_LIBS $STDLIBS -lgcc -lstdc++"

EXEC_LDFLAGS_SHARED="$SNAPPY_LIBS $ZLIB_LIBS $BZIP2_LIBS $LZ4_LIBS $ZSTD_LIBS $GFLAGS_LIBS"

VALGRIND_VER="$VALGRIND_BASE/bin/"

LUA_PATH="$LUA_BASE"

export CC CXX AR CFLAGS CXXFLAGS EXEC_LDFLAGS EXEC_LDFLAGS_SHARED VALGRIND_VER JEMALLOC_LIB JEMALLOC_INCLUDE LUA_PATH
