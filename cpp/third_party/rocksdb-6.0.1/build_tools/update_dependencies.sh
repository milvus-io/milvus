#!/bin/sh
#
# Update dependencies.sh file with the latest avaliable versions

BASEDIR=$(dirname $0)
OUTPUT=""

function log_variable()
{
  echo "$1=${!1}" >> "$OUTPUT"
}


TP2_LATEST="/mnt/vol/engshare/fbcode/third-party2"
## $1 => lib name
## $2 => lib version (if not provided, will try to pick latest)
## $3 => platform (if not provided, will try to pick latest gcc)
##
## get_lib_base will set a variable named ${LIB_NAME}_BASE to the lib location
function get_lib_base()
{
  local lib_name=$1
  local lib_version=$2
  local lib_platform=$3

  local result="$TP2_LATEST/$lib_name/"
  
  # Lib Version
  if [ -z "$lib_version" ] || [ "$lib_version" = "LATEST" ]; then
    # version is not provided, use latest
    result=`ls -dr1v $result/*/ | head -n1`
  else
    result="$result/$lib_version/"
  fi
  
  # Lib Platform
  if [ -z "$lib_platform" ]; then
    # platform is not provided, use latest gcc
    result=`ls -dr1v $result/gcc-*[^fb]/ | head -n1`
  else
    echo $lib_platform
    result="$result/$lib_platform/"
  fi
  
  result=`ls -1d $result/*/ | head -n1`
  
  # lib_name => LIB_NAME_BASE
  local __res_var=${lib_name^^}"_BASE"
  __res_var=`echo $__res_var | tr - _`
  # LIB_NAME_BASE=$result
  eval $__res_var=`readlink -f $result`
  
  log_variable $__res_var
}

###########################################################
#                platform007 dependencies                 #
###########################################################

OUTPUT="$BASEDIR/dependencies_platform007.sh"

rm -f "$OUTPUT"
touch "$OUTPUT"

echo "Writing dependencies to $OUTPUT"

# Compilers locations
GCC_BASE=`readlink -f $TP2_LATEST/gcc/7.x/centos7-native/*/`
CLANG_BASE=`readlink -f $TP2_LATEST/llvm-fb/stable/centos7-native/*/`

log_variable GCC_BASE
log_variable CLANG_BASE

# Libraries locations
get_lib_base libgcc     7.x     platform007
get_lib_base glibc      2.26    platform007
get_lib_base snappy     LATEST  platform007
get_lib_base zlib       LATEST  platform007
get_lib_base bzip2      LATEST  platform007
get_lib_base lz4        LATEST  platform007
get_lib_base zstd       LATEST  platform007
get_lib_base gflags     LATEST  platform007
get_lib_base jemalloc   LATEST  platform007
get_lib_base numa       LATEST  platform007
get_lib_base libunwind  LATEST  platform007
get_lib_base tbb        LATEST  platform007

get_lib_base kernel-headers fb platform007
get_lib_base binutils   LATEST centos7-native
get_lib_base valgrind   LATEST platform007
get_lib_base lua        5.3.4  platform007

git diff $OUTPUT

###########################################################
#                   5.x dependencies                      #
###########################################################

OUTPUT="$BASEDIR/dependencies.sh"

rm -f "$OUTPUT"
touch "$OUTPUT"

echo "Writing dependencies to $OUTPUT"

# Compilers locations
GCC_BASE=`readlink -f $TP2_LATEST/gcc/5.x/centos7-native/*/`
CLANG_BASE=`readlink -f $TP2_LATEST/llvm-fb/stable/centos7-native/*/`

log_variable GCC_BASE
log_variable CLANG_BASE

# Libraries locations
get_lib_base libgcc     5.x     gcc-5-glibc-2.23
get_lib_base glibc      2.23    gcc-5-glibc-2.23
get_lib_base snappy     LATEST  gcc-5-glibc-2.23
get_lib_base zlib       LATEST  gcc-5-glibc-2.23
get_lib_base bzip2      LATEST  gcc-5-glibc-2.23
get_lib_base lz4        LATEST  gcc-5-glibc-2.23
get_lib_base zstd       LATEST  gcc-5-glibc-2.23
get_lib_base gflags     LATEST  gcc-5-glibc-2.23
get_lib_base jemalloc   LATEST  gcc-5-glibc-2.23
get_lib_base numa       LATEST  gcc-5-glibc-2.23
get_lib_base libunwind  LATEST  gcc-5-glibc-2.23
get_lib_base tbb        LATEST  gcc-5-glibc-2.23

get_lib_base kernel-headers 4.0.9-36_fbk5_2933_gd092e3f gcc-5-glibc-2.23
get_lib_base binutils   LATEST centos7-native
get_lib_base valgrind   LATEST gcc-5-glibc-2.23
get_lib_base lua        5.2.3 gcc-5-glibc-2.23

git diff $OUTPUT

###########################################################
#                   4.8.1 dependencies                    #
###########################################################

OUTPUT="$BASEDIR/dependencies_4.8.1.sh"

rm -f "$OUTPUT"
touch "$OUTPUT"

echo "Writing 4.8.1 dependencies to $OUTPUT"

# Compilers locations
GCC_BASE=`readlink -f $TP2_LATEST/gcc/4.8.1/centos6-native/*/`
CLANG_BASE=`readlink -f $TP2_LATEST/llvm-fb/stable/centos6-native/*/`

log_variable GCC_BASE
log_variable CLANG_BASE

# Libraries locations
get_lib_base libgcc     4.8.1  gcc-4.8.1-glibc-2.17
get_lib_base glibc      2.17   gcc-4.8.1-glibc-2.17  
get_lib_base snappy     LATEST gcc-4.8.1-glibc-2.17
get_lib_base zlib       LATEST gcc-4.8.1-glibc-2.17
get_lib_base bzip2      LATEST gcc-4.8.1-glibc-2.17
get_lib_base lz4        LATEST gcc-4.8.1-glibc-2.17
get_lib_base zstd       LATEST gcc-4.8.1-glibc-2.17
get_lib_base gflags     LATEST gcc-4.8.1-glibc-2.17
get_lib_base jemalloc   LATEST gcc-4.8.1-glibc-2.17
get_lib_base numa       LATEST gcc-4.8.1-glibc-2.17
get_lib_base libunwind  LATEST gcc-4.8.1-glibc-2.17
get_lib_base tbb        4.0_update2 gcc-4.8.1-glibc-2.17

get_lib_base kernel-headers LATEST gcc-4.8.1-glibc-2.17 
get_lib_base binutils   LATEST centos6-native 
get_lib_base valgrind   3.8.1  gcc-4.8.1-glibc-2.17
get_lib_base lua        5.2.3 centos6-native

git diff $OUTPUT
