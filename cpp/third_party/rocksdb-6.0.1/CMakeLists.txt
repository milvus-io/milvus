# Prerequisites for Windows:
#     This cmake build is for Windows 64-bit only.
#
# Prerequisites:
#     You must have at least Visual Studio 2015 Update 3. Start the Developer Command Prompt window that is a part of Visual Studio installation.
#     Run the build commands from within the Developer Command Prompt window to have paths to the compiler and runtime libraries set.
#     You must have git.exe in your %PATH% environment variable.
#
# To build Rocksdb for Windows is as easy as 1-2-3-4-5:
#
# 1. Update paths to third-party libraries in thirdparty.inc file
# 2. Create a new directory for build artifacts
#        mkdir build
#        cd build
# 3. Run cmake to generate project files for Windows, add more options to enable required third-party libraries.
#    See thirdparty.inc for more information.
#        sample command: cmake -G "Visual Studio 15 Win64" -DWITH_GFLAGS=1 -DWITH_SNAPPY=1 -DWITH_JEMALLOC=1 -DWITH_JNI=1 ..
# 4. Then build the project in debug mode (you may want to add /m[:<N>] flag to run msbuild in <N> parallel threads
#                                          or simply /m to use all avail cores)
#        msbuild rocksdb.sln
#
#        rocksdb.sln build features exclusions of test only code in Release. If you build ALL_BUILD then everything
#        will be attempted but test only code does not build in Release mode.
#
# 5. And release mode (/m[:<N>] is also supported)
#        msbuild rocksdb.sln /p:Configuration=Release
#
# Linux:
#
# 1. Install a recent toolchain such as devtoolset-3 if you're on a older distro. C++11 required.
# 2. mkdir build; cd build
# 3. cmake ..
# 4. make -j

cmake_minimum_required(VERSION 2.8.12)
project(rocksdb)
enable_language(CXX)
enable_language(C)
enable_language(ASM)

if(POLICY CMP0042)
  cmake_policy(SET CMP0042 NEW)
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake/modules/")

option(WITH_JEMALLOC "build with JeMalloc" OFF)
option(WITH_SNAPPY "build with SNAPPY" OFF)
option(WITH_LZ4 "build with lz4" OFF)
option(WITH_ZLIB "build with zlib" OFF)
option(WITH_ZSTD "build with zstd" OFF)
option(WITH_WINDOWS_UTF8_FILENAMES "use UTF8 as characterset for opening files, regardles of the system code page" OFF)
if (WITH_WINDOWS_UTF8_FILENAMES)
  add_definitions(-DROCKSDB_WINDOWS_UTF8_FILENAMES)
endif()
if(MSVC)
  # Defaults currently different for GFLAGS.
  #  We will address find_package work a little later
  option(WITH_GFLAGS "build with GFlags" OFF)
  option(WITH_XPRESS "build with windows built in compression" OFF)
  include(${CMAKE_CURRENT_SOURCE_DIR}/thirdparty.inc)
else()
  if(CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
    # FreeBSD has jemalloc as default malloc
    # but it does not have all the jemalloc files in include/...
    set(WITH_JEMALLOC ON)
  else()
    if(WITH_JEMALLOC)
      find_package(JeMalloc REQUIRED)
      add_definitions(-DROCKSDB_JEMALLOC -DJEMALLOC_NO_DEMANGLE)
      include_directories(${JEMALLOC_INCLUDE_DIR})
      list(APPEND THIRDPARTY_LIBS ${JEMALLOC_LIBRARIES})
    endif()
  endif()

  # No config file for this
  option(WITH_GFLAGS "build with GFlags" ON)
  if(WITH_GFLAGS)
    find_package(gflags)
    if(gflags_FOUND)
      add_definitions(-DGFLAGS=1)
      include_directories(${gflags_INCLUDE_DIR})
      list(APPEND THIRDPARTY_LIBS ${gflags_LIBRARIES})
    endif()
  endif()

  if(WITH_SNAPPY)
    find_package(snappy REQUIRED)
    add_definitions(-DSNAPPY)
    include_directories(${SNAPPY_INCLUDE_DIR})
    list(APPEND THIRDPARTY_LIBS ${SNAPPY_LIBRARIES})
  endif()

  if(WITH_ZLIB)
    find_package(zlib REQUIRED)
    add_definitions(-DZLIB)
    if(ZLIB_INCLUDE_DIRS)
      # CMake 3
      include_directories(${ZLIB_INCLUDE_DIRS})
    else()
      # CMake 2
      include_directories(${ZLIB_INCLUDE_DIR})
    endif()
    list(APPEND THIRDPARTY_LIBS ${ZLIB_LIBRARIES})
  endif()

  option(WITH_BZ2 "build with bzip2" OFF)
  if(WITH_BZ2)
    find_package(bzip2 REQUIRED)
    add_definitions(-DBZIP2)
    include_directories(${BZIP2_INCLUDE_DIR})
    list(APPEND THIRDPARTY_LIBS ${BZIP2_LIBRARIES})
  endif()

  if(WITH_LZ4)
    find_package(lz4 REQUIRED)
    add_definitions(-DLZ4)
    include_directories(${LZ4_INCLUDE_DIR})
    list(APPEND THIRDPARTY_LIBS ${LZ4_LIBRARIES})
  endif()

  if(WITH_ZSTD)
    find_package(zstd REQUIRED)
    add_definitions(-DZSTD)
    include_directories(${ZSTD_INCLUDE_DIR})
    list(APPEND THIRDPARTY_LIBS ${ZSTD_LIBRARIES})
  endif()
endif()

string(TIMESTAMP GIT_DATE_TIME "%Y/%m/%d %H:%M:%S" UTC)

find_package(Git)

if(GIT_FOUND AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/.git")
  if(WIN32)
    execute_process(COMMAND $ENV{COMSPEC} /C ${GIT_EXECUTABLE} -C ${CMAKE_CURRENT_SOURCE_DIR} rev-parse HEAD OUTPUT_VARIABLE GIT_SHA)
  else()
    execute_process(COMMAND ${GIT_EXECUTABLE} -C ${CMAKE_CURRENT_SOURCE_DIR} rev-parse HEAD OUTPUT_VARIABLE GIT_SHA)
  endif()
else()
  set(GIT_SHA 0)
endif()

string(REGEX REPLACE "[^0-9a-f]+" "" GIT_SHA "${GIT_SHA}")


# Read rocksdb version from version.h header file.
file(READ include/rocksdb/version.h version_header_file)
string(REGEX MATCH "#define ROCKSDB_MAJOR ([0-9]+)" _ ${version_header_file})
set(ROCKSDB_VERSION_MAJOR ${CMAKE_MATCH_1})
string(REGEX MATCH "#define ROCKSDB_MINOR ([0-9]+)" _ ${version_header_file})
set(ROCKSDB_VERSION_MINOR ${CMAKE_MATCH_1})
string(REGEX MATCH "#define ROCKSDB_PATCH ([0-9]+)" _ ${version_header_file})
set(ROCKSDB_VERSION_PATCH ${CMAKE_MATCH_1})
set(ROCKSDB_VERSION ${ROCKSDB_VERSION_MAJOR}.${ROCKSDB_VERSION_MINOR}.${ROCKSDB_VERSION_PATCH})


option(WITH_MD_LIBRARY "build with MD" ON)
if(WIN32 AND MSVC)
  if(WITH_MD_LIBRARY)
    set(RUNTIME_LIBRARY "MD")
  else()
    set(RUNTIME_LIBRARY "MT")
  endif()
endif()

set(BUILD_VERSION_CC ${CMAKE_BINARY_DIR}/build_version.cc)
configure_file(util/build_version.cc.in ${BUILD_VERSION_CC} @ONLY)
add_library(build_version OBJECT ${BUILD_VERSION_CC})
target_include_directories(build_version PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/util)
if(MSVC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Zi /nologo /EHsc /GS /Gd /GR /GF /fp:precise /Zc:wchar_t /Zc:forScope /errorReport:queue")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /FC /d2Zi+ /W4 /wd4127 /wd4800 /wd4996 /wd4351 /wd4100 /wd4204 /wd4324")
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -W -Wextra -Wall")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wsign-compare -Wshadow -Wno-unused-parameter -Wno-unused-variable -Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers -Wno-strict-aliasing")
  if(MINGW)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-format")
  endif()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
  if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
    include(CheckCXXCompilerFlag)
    CHECK_CXX_COMPILER_FLAG("-momit-leaf-frame-pointer" HAVE_OMIT_LEAF_FRAME_POINTER)
    if(HAVE_OMIT_LEAF_FRAME_POINTER)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -momit-leaf-frame-pointer")
    endif()
  endif()
endif()

include(CheckCCompilerFlag)
if(CMAKE_SYSTEM_PROCESSOR MATCHES "ppc64le")
  CHECK_C_COMPILER_FLAG("-maltivec" HAS_ALTIVEC)
  if(HAS_ALTIVEC)
    message(STATUS " HAS_ALTIVEC yes")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -maltivec")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -maltivec")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mcpu=power8")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mcpu=power8")
  endif(HAS_ALTIVEC)
endif(CMAKE_SYSTEM_PROCESSOR MATCHES "ppc64le")

option(PORTABLE "build a portable binary" OFF)
option(FORCE_SSE42 "force building with SSE4.2, even when PORTABLE=ON" OFF)
if(PORTABLE)
  # MSVC does not need a separate compiler flag to enable SSE4.2; if nmmintrin.h
  # is available, it is available by default.
  if(FORCE_SSE42 AND NOT MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2 -mpclmul")
  endif()
else()
  if(MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /arch:AVX2")
  else()
    if(NOT HAVE_POWER8)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
    endif()
  endif()
endif()

include(CheckCXXSourceCompiles)
if(NOT MSVC)
  set(CMAKE_REQUIRED_FLAGS "-msse4.2 -mpclmul")
endif()
CHECK_CXX_SOURCE_COMPILES("
#include <cstdint>
#include <nmmintrin.h>
#include <wmmintrin.h>
int main() {
  volatile uint32_t x = _mm_crc32_u32(0, 0);
  const auto a = _mm_set_epi64x(0, 0);
  const auto b = _mm_set_epi64x(0, 0);
  const auto c = _mm_clmulepi64_si128(a, b, 0x00);
  auto d = _mm_cvtsi128_si64(c);
}
" HAVE_SSE42)
unset(CMAKE_REQUIRED_FLAGS)
if(HAVE_SSE42)
  add_definitions(-DHAVE_SSE42)
  add_definitions(-DHAVE_PCLMUL)
elseif(FORCE_SSE42)
  message(FATAL_ERROR "FORCE_SSE42=ON but unable to compile with SSE4.2 enabled")
endif()

CHECK_CXX_SOURCE_COMPILES("
#if defined(_MSC_VER) && !defined(__thread)
#define __thread __declspec(thread)
#endif
int main() {
  static __thread int tls;
}
" HAVE_THREAD_LOCAL)
if(HAVE_THREAD_LOCAL)
  add_definitions(-DROCKSDB_SUPPORT_THREAD_LOCAL)
endif()

option(FAIL_ON_WARNINGS "Treat compile warnings as errors" ON)
if(FAIL_ON_WARNINGS)
  if(MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /WX")
  else() # assume GCC
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
  endif()
endif()

option(WITH_ASAN "build with ASAN" OFF)
if(WITH_ASAN)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address")
  if(WITH_JEMALLOC)
    message(FATAL "ASAN does not work well with JeMalloc")
  endif()
endif()

option(WITH_TSAN "build with TSAN" OFF)
if(WITH_TSAN)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread -pie")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -fPIC")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=thread -fPIC")
  if(WITH_JEMALLOC)
    message(FATAL "TSAN does not work well with JeMalloc")
  endif()
endif()

option(WITH_UBSAN "build with UBSAN" OFF)
if(WITH_UBSAN)
  add_definitions(-DROCKSDB_UBSAN_RUN)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=undefined")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=undefined")
  if(WITH_JEMALLOC)
    message(FATAL "UBSAN does not work well with JeMalloc")
  endif()
endif()

option(WITH_NUMA "build with NUMA policy support" OFF)
if(WITH_NUMA)
  find_package(NUMA REQUIRED)
  add_definitions(-DNUMA)
  include_directories(${NUMA_INCLUDE_DIR})
  list(APPEND THIRDPARTY_LIBS ${NUMA_LIBRARIES})
endif()

option(WITH_TBB "build with Threading Building Blocks (TBB)" OFF)
if(WITH_TBB)
  find_package(TBB REQUIRED)
  add_definitions(-DTBB)
  include_directories(${TBB_INCLUDE_DIR})
  list(APPEND THIRDPARTY_LIBS ${TBB_LIBRARIES})
endif()

# Stall notifications eat some performance from inserts
option(DISABLE_STALL_NOTIF "Build with stall notifications" OFF)
if(DISABLE_STALL_NOTIF)
  add_definitions(-DROCKSDB_DISABLE_STALL_NOTIFICATION)
endif()


if(DEFINED USE_RTTI)
  if(USE_RTTI)
    message(STATUS "Enabling RTTI")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DROCKSDB_USE_RTTI")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -DROCKSDB_USE_RTTI")
  else()
    if(MSVC)
      message(STATUS "Disabling RTTI in Release builds. Always on in Debug.")
      set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DROCKSDB_USE_RTTI")
      set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /GR-")
    else()
      message(STATUS "Disabling RTTI in Release builds")
      set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-rtti")
      set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fno-rtti")
    endif()
  endif()
else()
  message(STATUS "Enabling RTTI in Debug builds only (default)")
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DROCKSDB_USE_RTTI")
  if(MSVC)
     set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /GR-")
  else()
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fno-rtti")
  endif()
endif()

# Used to run CI build and tests so we can run faster
option(OPTDBG "Build optimized debug build with MSVC" OFF)
option(WITH_RUNTIME_DEBUG "build with debug version of runtime library" ON)
if(MSVC)
  if(OPTDBG)
    message(STATUS "Debug optimization is enabled")
    set(CMAKE_CXX_FLAGS_DEBUG "/Oxt")
  else()
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /Od /RTC1 /Gm")
  endif()
  if(WITH_RUNTIME_DEBUG)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /${RUNTIME_LIBRARY}d")
  else()
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /${RUNTIME_LIBRARY}")
  endif()
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /Oxt /Zp8 /Gm- /Gy /${RUNTIME_LIBRARY}")

  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} /DEBUG")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /DEBUG")
endif()

if(CMAKE_COMPILER_IS_GNUCXX)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-memcmp")
endif()

option(ROCKSDB_LITE "Build RocksDBLite version" OFF)
if(ROCKSDB_LITE)
  add_definitions(-DROCKSDB_LITE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-exceptions -Os")
endif()

if(CMAKE_SYSTEM_NAME MATCHES "Cygwin")
  add_definitions(-fno-builtin-memcmp -DCYGWIN)
elseif(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  add_definitions(-DOS_MACOSX)
  if(CMAKE_SYSTEM_PROCESSOR MATCHES arm)
    add_definitions(-DIOS_CROSS_COMPILE -DROCKSDB_LITE)
    # no debug info for IOS, that will make our library big
    add_definitions(-DNDEBUG)
  endif()
elseif(CMAKE_SYSTEM_NAME MATCHES "Linux")
  add_definitions(-DOS_LINUX)
elseif(CMAKE_SYSTEM_NAME MATCHES "SunOS")
  add_definitions(-DOS_SOLARIS)
elseif(CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
  add_definitions(-DOS_FREEBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "NetBSD")
  add_definitions(-DOS_NETBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "OpenBSD")
  add_definitions(-DOS_OPENBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "DragonFly")
  add_definitions(-DOS_DRAGONFLYBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "Android")
  add_definitions(-DOS_ANDROID)
elseif(CMAKE_SYSTEM_NAME MATCHES "Windows")
  add_definitions(-DWIN32 -DOS_WIN -D_MBCS -DWIN64 -DNOMINMAX)
  if(MINGW)
    add_definitions(-D_WIN32_WINNT=_WIN32_WINNT_VISTA)
  endif()
endif()

if(NOT WIN32)
  add_definitions(-DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX)
endif()

option(WITH_FALLOCATE "build with fallocate" ON)
if(WITH_FALLOCATE)
  CHECK_CXX_SOURCE_COMPILES("
#include <fcntl.h>
#include <linux/falloc.h>
int main() {
 int fd = open(\"/dev/null\", 0);
 fallocate(fd, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE, 0, 1024);
}
" HAVE_FALLOCATE)
  if(HAVE_FALLOCATE)
    add_definitions(-DROCKSDB_FALLOCATE_PRESENT)
  endif()
endif()

CHECK_CXX_SOURCE_COMPILES("
#include <fcntl.h>
int main() {
  int fd = open(\"/dev/null\", 0);
  sync_file_range(fd, 0, 1024, SYNC_FILE_RANGE_WRITE);
}
" HAVE_SYNC_FILE_RANGE_WRITE)
if(HAVE_SYNC_FILE_RANGE_WRITE)
  add_definitions(-DROCKSDB_RANGESYNC_PRESENT)
endif()

CHECK_CXX_SOURCE_COMPILES("
#include <pthread.h>
int main() {
  (void) PTHREAD_MUTEX_ADAPTIVE_NP;
}
" HAVE_PTHREAD_MUTEX_ADAPTIVE_NP)
if(HAVE_PTHREAD_MUTEX_ADAPTIVE_NP)
  add_definitions(-DROCKSDB_PTHREAD_ADAPTIVE_MUTEX)
endif()

include(CheckCXXSymbolExists)
check_cxx_symbol_exists(malloc_usable_size malloc.h HAVE_MALLOC_USABLE_SIZE)
if(HAVE_MALLOC_USABLE_SIZE)
  add_definitions(-DROCKSDB_MALLOC_USABLE_SIZE)
endif()

check_cxx_symbol_exists(sched_getcpu sched.h HAVE_SCHED_GETCPU)
if(HAVE_SCHED_GETCPU)
  add_definitions(-DROCKSDB_SCHED_GETCPU_PRESENT)
endif()

include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(SYSTEM ${PROJECT_SOURCE_DIR}/third-party/gtest-1.7.0/fused-src)
find_package(Threads REQUIRED)

add_subdirectory(third-party/gtest-1.7.0/fused-src/gtest)

# Main library source code

set(SOURCES
        cache/clock_cache.cc
        cache/lru_cache.cc
        cache/sharded_cache.cc
        db/builder.cc
        db/c.cc
        db/column_family.cc
        db/compacted_db_impl.cc
        db/compaction.cc
        db/compaction_iterator.cc
        db/compaction_job.cc
        db/compaction_picker.cc
        db/compaction_picker_fifo.cc
        db/compaction_picker_universal.cc
        db/convenience.cc
        db/db_filesnapshot.cc
        db/db_impl.cc
        db/db_impl_write.cc
        db/db_impl_compaction_flush.cc
        db/db_impl_files.cc
        db/db_impl_open.cc
        db/db_impl_debug.cc
        db/db_impl_experimental.cc
        db/db_impl_readonly.cc
        db/db_info_dumper.cc
        db/db_iter.cc
        db/dbformat.cc
        db/error_handler.cc
        db/event_helpers.cc
        db/experimental.cc
        db/external_sst_file_ingestion_job.cc
        db/file_indexer.cc
        db/flush_job.cc
        db/flush_scheduler.cc
        db/forward_iterator.cc
        db/internal_stats.cc
        db/in_memory_stats_history.cc
        db/logs_with_prep_tracker.cc
        db/log_reader.cc
        db/log_writer.cc
        db/malloc_stats.cc
        db/memtable.cc
        db/memtable_list.cc
        db/merge_helper.cc
        db/merge_operator.cc
        db/range_del_aggregator.cc
        db/range_tombstone_fragmenter.cc
        db/repair.cc
        db/snapshot_impl.cc
        db/table_cache.cc
        db/table_properties_collector.cc
        db/transaction_log_impl.cc
        db/version_builder.cc
        db/version_edit.cc
        db/version_set.cc
        db/wal_manager.cc
        db/write_batch.cc
        db/write_batch_base.cc
        db/write_controller.cc
        db/write_thread.cc
        env/env.cc
        env/env_chroot.cc
        env/env_encryption.cc
        env/env_hdfs.cc
        env/mock_env.cc
        memtable/alloc_tracker.cc
        memtable/hash_linklist_rep.cc
        memtable/hash_skiplist_rep.cc
        memtable/skiplistrep.cc
        memtable/vectorrep.cc
        memtable/write_buffer_manager.cc
        monitoring/histogram.cc
        monitoring/histogram_windowing.cc
        monitoring/instrumented_mutex.cc
        monitoring/iostats_context.cc
        monitoring/perf_context.cc
        monitoring/perf_level.cc
        monitoring/statistics.cc
        monitoring/thread_status_impl.cc
        monitoring/thread_status_updater.cc
        monitoring/thread_status_util.cc
        monitoring/thread_status_util_debug.cc
        options/cf_options.cc
        options/db_options.cc
        options/options.cc
        options/options_helper.cc
        options/options_parser.cc
        options/options_sanity_check.cc
        port/stack_trace.cc
        table/adaptive_table_factory.cc
        table/block.cc
        table/block_based_filter_block.cc
        table/block_based_table_builder.cc
        table/block_based_table_factory.cc
        table/block_based_table_reader.cc
        table/block_builder.cc
        table/block_fetcher.cc
        table/block_prefix_index.cc
        table/bloom_block.cc
        table/cuckoo_table_builder.cc
        table/cuckoo_table_factory.cc
        table/cuckoo_table_reader.cc
        table/data_block_hash_index.cc
        table/data_block_footer.cc
        table/flush_block_policy.cc
        table/format.cc
        table/full_filter_block.cc
        table/get_context.cc
        table/index_builder.cc
        table/iterator.cc
        table/merging_iterator.cc
        table/meta_blocks.cc
        table/partitioned_filter_block.cc
        table/persistent_cache_helper.cc
        table/plain_table_builder.cc
        table/plain_table_factory.cc
        table/plain_table_index.cc
        table/plain_table_key_coding.cc
        table/plain_table_reader.cc
        table/sst_file_reader.cc
        table/sst_file_writer.cc
        table/table_properties.cc
        table/two_level_iterator.cc
        tools/db_bench_tool.cc
        tools/dump/db_dump_tool.cc
        tools/ldb_cmd.cc
        tools/ldb_tool.cc
        tools/sst_dump_tool.cc
        tools/trace_analyzer_tool.cc
        util/arena.cc
        util/auto_roll_logger.cc
        util/bloom.cc
        util/coding.cc
        util/compaction_job_stats_impl.cc
        util/comparator.cc
        util/compression_context_cache.cc
        util/concurrent_arena.cc
        util/concurrent_task_limiter_impl.cc
        util/crc32c.cc
        util/delete_scheduler.cc
        util/dynamic_bloom.cc
        util/event_logger.cc
        util/file_reader_writer.cc
        util/file_util.cc
        util/filename.cc
        util/filter_policy.cc
        util/hash.cc
        util/jemalloc_nodump_allocator.cc
        util/log_buffer.cc
        util/murmurhash.cc
        util/random.cc
        util/rate_limiter.cc
        util/slice.cc
        util/sst_file_manager_impl.cc
        util/status.cc
        util/string_util.cc
        util/sync_point.cc
        util/sync_point_impl.cc
        util/testutil.cc
        util/thread_local.cc
        util/threadpool_imp.cc
        util/trace_replay.cc
        util/transaction_test_util.cc
        util/xxhash.cc
        utilities/backupable/backupable_db.cc
        utilities/blob_db/blob_compaction_filter.cc
        utilities/blob_db/blob_db.cc
        utilities/blob_db/blob_db_impl.cc
        utilities/blob_db/blob_db_impl_filesnapshot.cc
        utilities/blob_db/blob_dump_tool.cc
        utilities/blob_db/blob_file.cc
        utilities/blob_db/blob_log_reader.cc
        utilities/blob_db/blob_log_writer.cc
        utilities/blob_db/blob_log_format.cc
        utilities/cassandra/cassandra_compaction_filter.cc
        utilities/cassandra/format.cc
        utilities/cassandra/merge_operator.cc
        utilities/checkpoint/checkpoint_impl.cc
        utilities/compaction_filters/remove_emptyvalue_compactionfilter.cc
        utilities/debug.cc
        utilities/env_mirror.cc
        utilities/env_timed.cc
        utilities/leveldb_options/leveldb_options.cc
        utilities/memory/memory_util.cc
        utilities/merge_operators/bytesxor.cc
        utilities/merge_operators/max.cc
        utilities/merge_operators/put.cc
        utilities/merge_operators/string_append/stringappend.cc
        utilities/merge_operators/string_append/stringappend2.cc
        utilities/merge_operators/uint64add.cc
        utilities/option_change_migration/option_change_migration.cc
        utilities/options/options_util.cc
        utilities/persistent_cache/block_cache_tier.cc
        utilities/persistent_cache/block_cache_tier_file.cc
        utilities/persistent_cache/block_cache_tier_metadata.cc
        utilities/persistent_cache/persistent_cache_tier.cc
        utilities/persistent_cache/volatile_tier_impl.cc
        utilities/simulator_cache/sim_cache.cc
        utilities/table_properties_collectors/compact_on_deletion_collector.cc
        utilities/trace/file_trace_reader_writer.cc
        utilities/transactions/optimistic_transaction_db_impl.cc
        utilities/transactions/optimistic_transaction.cc
        utilities/transactions/pessimistic_transaction.cc
        utilities/transactions/pessimistic_transaction_db.cc
        utilities/transactions/snapshot_checker.cc
        utilities/transactions/transaction_base.cc
        utilities/transactions/transaction_db_mutex_impl.cc
        utilities/transactions/transaction_lock_mgr.cc
        utilities/transactions/transaction_util.cc
        utilities/transactions/write_prepared_txn.cc
        utilities/transactions/write_prepared_txn_db.cc
        utilities/transactions/write_unprepared_txn.cc
        utilities/transactions/write_unprepared_txn_db.cc
        utilities/ttl/db_ttl_impl.cc
        utilities/write_batch_with_index/write_batch_with_index.cc
        utilities/write_batch_with_index/write_batch_with_index_internal.cc
        $<TARGET_OBJECTS:build_version>)

if(HAVE_SSE42 AND NOT MSVC)
  set_source_files_properties(
    util/crc32c.cc
    PROPERTIES COMPILE_FLAGS "-msse4.2 -mpclmul")
endif()

if(HAVE_POWER8)
  list(APPEND SOURCES
    util/crc32c_ppc.c
    util/crc32c_ppc_asm.S)
endif(HAVE_POWER8)

if(WIN32)
  list(APPEND SOURCES
    port/win/io_win.cc
    port/win/env_win.cc
    port/win/env_default.cc
    port/win/port_win.cc
    port/win/win_logger.cc
    port/win/win_thread.cc)

if(WITH_XPRESS)
  list(APPEND SOURCES
    port/win/xpress_win.cc)
endif()

if(WITH_JEMALLOC)
  list(APPEND SOURCES
    port/win/win_jemalloc.cc)
endif()

else()
  list(APPEND SOURCES
    port/port_posix.cc
    env/env_posix.cc
    env/io_posix.cc)
endif()

set(ROCKSDB_STATIC_LIB rocksdb${ARTIFACT_SUFFIX})
set(ROCKSDB_SHARED_LIB rocksdb-shared${ARTIFACT_SUFFIX})
set(ROCKSDB_IMPORT_LIB ${ROCKSDB_SHARED_LIB})

option(WITH_LIBRADOS "Build with librados" OFF)
if(WITH_LIBRADOS)
  list(APPEND SOURCES
    utilities/env_librados.cc)
  list(APPEND THIRDPARTY_LIBS rados)
endif()

if(WIN32)
  set(SYSTEM_LIBS ${SYSTEM_LIBS} Shlwapi.lib Rpcrt4.lib)
  set(LIBS ${ROCKSDB_STATIC_LIB} ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})
else()
  set(SYSTEM_LIBS ${CMAKE_THREAD_LIBS_INIT})
  set(LIBS ${ROCKSDB_SHARED_LIB} ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})

  add_library(${ROCKSDB_SHARED_LIB} SHARED ${SOURCES})
  target_link_libraries(${ROCKSDB_SHARED_LIB}
    ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})
  set_target_properties(${ROCKSDB_SHARED_LIB} PROPERTIES
                        LINKER_LANGUAGE CXX
                        VERSION ${ROCKSDB_VERSION}
                        SOVERSION ${ROCKSDB_VERSION_MAJOR}
                        CXX_STANDARD 11
                        OUTPUT_NAME "rocksdb")
endif()

add_library(${ROCKSDB_STATIC_LIB} STATIC ${SOURCES})
target_link_libraries(${ROCKSDB_STATIC_LIB}
  ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})

if(WIN32)
  add_library(${ROCKSDB_IMPORT_LIB} SHARED ${SOURCES})
  target_link_libraries(${ROCKSDB_IMPORT_LIB}
    ${THIRDPARTY_LIBS} ${SYSTEM_LIBS})
  set_target_properties(${ROCKSDB_IMPORT_LIB} PROPERTIES
    COMPILE_DEFINITIONS "ROCKSDB_DLL;ROCKSDB_LIBRARY_EXPORTS")
  if(MSVC)
    set_target_properties(${ROCKSDB_STATIC_LIB} PROPERTIES
      COMPILE_FLAGS "/Fd${CMAKE_CFG_INTDIR}/${ROCKSDB_STATIC_LIB}.pdb")
    set_target_properties(${ROCKSDB_IMPORT_LIB} PROPERTIES
      COMPILE_FLAGS "/Fd${CMAKE_CFG_INTDIR}/${ROCKSDB_IMPORT_LIB}.pdb")
  endif()
endif()

option(WITH_JNI "build with JNI" OFF)
if(WITH_JNI OR JNI)
  message(STATUS "JNI library is enabled")
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/java)
else()
  message(STATUS "JNI library is disabled")
endif()

# Installation and packaging
if(WIN32)
  option(ROCKSDB_INSTALL_ON_WINDOWS "Enable install target on Windows" OFF)
endif()
if(NOT WIN32 OR ROCKSDB_INSTALL_ON_WINDOWS)
  if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
    if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
      # Change default installation prefix on Linux to /usr
      set(CMAKE_INSTALL_PREFIX /usr CACHE PATH "Install path prefix, prepended onto install directories." FORCE)
    endif()
  endif()

  include(GNUInstallDirs)
  include(CMakePackageConfigHelpers)

  set(package_config_destination ${CMAKE_INSTALL_LIBDIR}/cmake/rocksdb)

  configure_package_config_file(
    ${CMAKE_CURRENT_LIST_DIR}/cmake/RocksDBConfig.cmake.in RocksDBConfig.cmake
    INSTALL_DESTINATION ${package_config_destination}
  )

  write_basic_package_version_file(
    RocksDBConfigVersion.cmake
    VERSION ${ROCKSDB_VERSION}
    COMPATIBILITY SameMajorVersion
  )

  install(DIRECTORY include/rocksdb COMPONENT devel DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")

  install(
    TARGETS ${ROCKSDB_STATIC_LIB}
    EXPORT RocksDBTargets
    COMPONENT devel
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
  )

  install(
    TARGETS ${ROCKSDB_SHARED_LIB}
    EXPORT RocksDBTargets
    COMPONENT runtime
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
  )

  install(
    EXPORT RocksDBTargets
    COMPONENT devel
    DESTINATION ${package_config_destination}
    NAMESPACE RocksDB::
  )

  install(
    FILES
    ${CMAKE_CURRENT_BINARY_DIR}/RocksDBConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/RocksDBConfigVersion.cmake
    COMPONENT devel
    DESTINATION ${package_config_destination}
  )
endif()

option(WITH_TESTS "build with tests" ON)
if(WITH_TESTS)
  set(TESTS
        cache/cache_test.cc
        cache/lru_cache_test.cc
        db/column_family_test.cc
        db/compact_files_test.cc
        db/compaction_iterator_test.cc
        db/compaction_job_stats_test.cc
        db/compaction_job_test.cc
        db/compaction_picker_test.cc
        db/comparator_db_test.cc
        db/corruption_test.cc
        db/cuckoo_table_db_test.cc
        db/db_basic_test.cc
        db/db_blob_index_test.cc
        db/db_block_cache_test.cc
        db/db_bloom_filter_test.cc
        db/db_compaction_filter_test.cc
        db/db_compaction_test.cc
        db/db_dynamic_level_test.cc
        db/db_flush_test.cc
        db/db_inplace_update_test.cc
        db/db_io_failure_test.cc
        db/db_iter_test.cc
        db/db_iter_stress_test.cc
        db/db_iterator_test.cc
        db/db_log_iter_test.cc
        db/db_memtable_test.cc
        db/db_merge_operator_test.cc
        db/db_options_test.cc
        db/db_properties_test.cc
        db/db_range_del_test.cc
        db/db_sst_test.cc
        db/db_statistics_test.cc
        db/db_table_properties_test.cc
        db/db_tailing_iter_test.cc
        db/db_test.cc
        db/db_test2.cc
        db/db_universal_compaction_test.cc
        db/db_wal_test.cc
        db/db_write_test.cc
        db/dbformat_test.cc
        db/deletefile_test.cc
        db/error_handler_test.cc
        db/obsolete_files_test.cc
        db/external_sst_file_basic_test.cc
        db/external_sst_file_test.cc
        db/fault_injection_test.cc
        db/file_indexer_test.cc
        db/filename_test.cc
        db/flush_job_test.cc
        db/listener_test.cc
        db/log_test.cc
        db/manual_compaction_test.cc
        db/memtable_list_test.cc
        db/merge_helper_test.cc
        db/merge_test.cc
        db/options_file_test.cc
        db/perf_context_test.cc
        db/plain_table_db_test.cc
        db/prefix_test.cc
        db/range_del_aggregator_test.cc
        db/range_tombstone_fragmenter_test.cc
        db/repair_test.cc
        db/table_properties_collector_test.cc
        db/version_builder_test.cc
        db/version_edit_test.cc
        db/version_set_test.cc
        db/wal_manager_test.cc
        db/write_batch_test.cc
        db/write_callback_test.cc
        db/write_controller_test.cc
        env/env_basic_test.cc
        env/env_test.cc
        env/mock_env_test.cc
        memtable/inlineskiplist_test.cc
        memtable/skiplist_test.cc
        memtable/write_buffer_manager_test.cc
        monitoring/histogram_test.cc
        monitoring/iostats_context_test.cc
        monitoring/statistics_test.cc
        options/options_settable_test.cc
        options/options_test.cc
        table/block_based_filter_block_test.cc
        table/block_test.cc
        table/cleanable_test.cc
        table/cuckoo_table_builder_test.cc
        table/cuckoo_table_reader_test.cc
        table/data_block_hash_index_test.cc
        table/full_filter_block_test.cc
        table/merger_test.cc
        table/sst_file_reader_test.cc
        table/table_test.cc
        tools/ldb_cmd_test.cc
        tools/reduce_levels_test.cc
        tools/sst_dump_test.cc
        tools/trace_analyzer_test.cc
        util/arena_test.cc
        util/auto_roll_logger_test.cc
        util/autovector_test.cc
        util/bloom_test.cc
        util/coding_test.cc
        util/crc32c_test.cc
        util/delete_scheduler_test.cc
        util/dynamic_bloom_test.cc
        util/event_logger_test.cc
        util/file_reader_writer_test.cc
        util/filelock_test.cc
        util/hash_test.cc
        util/heap_test.cc
        util/rate_limiter_test.cc
        util/repeatable_thread_test.cc
        util/slice_transform_test.cc
        util/timer_queue_test.cc
        util/thread_list_test.cc
        util/thread_local_test.cc
        utilities/backupable/backupable_db_test.cc
        utilities/blob_db/blob_db_test.cc
        utilities/cassandra/cassandra_functional_test.cc
        utilities/cassandra/cassandra_format_test.cc
        utilities/cassandra/cassandra_row_merge_test.cc
        utilities/cassandra/cassandra_serialize_test.cc
        utilities/checkpoint/checkpoint_test.cc
        utilities/memory/memory_test.cc
        utilities/merge_operators/string_append/stringappend_test.cc
        utilities/object_registry_test.cc
        utilities/option_change_migration/option_change_migration_test.cc
        utilities/options/options_util_test.cc
        utilities/persistent_cache/hash_table_test.cc
        utilities/persistent_cache/persistent_cache_test.cc
        utilities/simulator_cache/sim_cache_test.cc
        utilities/table_properties_collectors/compact_on_deletion_collector_test.cc
        utilities/transactions/optimistic_transaction_test.cc
        utilities/transactions/transaction_test.cc
        utilities/transactions/write_prepared_transaction_test.cc
        utilities/transactions/write_unprepared_transaction_test.cc
        utilities/ttl/ttl_test.cc
        utilities/write_batch_with_index/write_batch_with_index_test.cc
  )
  if(WITH_LIBRADOS)
    list(APPEND TESTS utilities/env_librados_test.cc)
  endif()

  set(BENCHMARKS
    cache/cache_bench.cc
    memtable/memtablerep_bench.cc
    db/range_del_aggregator_bench.cc
    tools/db_bench.cc
    table/table_reader_bench.cc
    utilities/persistent_cache/hash_table_bench.cc)
  add_library(testharness OBJECT util/testharness.cc)
  foreach(sourcefile ${BENCHMARKS})
    get_filename_component(exename ${sourcefile} NAME_WE)
    add_executable(${exename}${ARTIFACT_SUFFIX} ${sourcefile}
      $<TARGET_OBJECTS:testharness>)
    target_link_libraries(${exename}${ARTIFACT_SUFFIX} gtest ${LIBS})
  endforeach(sourcefile ${BENCHMARKS})

  # For test util library that is build only in DEBUG mode
  # and linked to tests. Add test only code that is not #ifdefed for Release here.
  set(TESTUTIL_SOURCE
      db/db_test_util.cc
      monitoring/thread_status_updater_debug.cc
      table/mock_table.cc
      util/fault_injection_test_env.cc
      utilities/cassandra/test_utils.cc
  )
  # test utilities are only build in debug
  enable_testing()
  add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND})
  set(TESTUTILLIB testutillib${ARTIFACT_SUFFIX})
  add_library(${TESTUTILLIB} STATIC ${TESTUTIL_SOURCE})
  if(MSVC)
    set_target_properties(${TESTUTILLIB} PROPERTIES COMPILE_FLAGS "/Fd${CMAKE_CFG_INTDIR}/testutillib${ARTIFACT_SUFFIX}.pdb")
  endif()
  set_target_properties(${TESTUTILLIB}
        PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
        EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
        EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
        )

  # Tests are excluded from Release builds
  set(TEST_EXES ${TESTS})

  foreach(sourcefile ${TEST_EXES})
      get_filename_component(exename ${sourcefile} NAME_WE)
      add_executable(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} ${sourcefile}
        $<TARGET_OBJECTS:testharness>)
      set_target_properties(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX}
        PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
        EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
        EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
        OUTPUT_NAME ${exename}${ARTIFACT_SUFFIX}
        )
      target_link_libraries(${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX} testutillib${ARTIFACT_SUFFIX} gtest ${LIBS})
      if(NOT "${exename}" MATCHES "db_sanity_test")
        add_test(NAME ${exename} COMMAND ${exename}${ARTIFACT_SUFFIX})
        add_dependencies(check ${CMAKE_PROJECT_NAME}_${exename}${ARTIFACT_SUFFIX})
      endif()
  endforeach(sourcefile ${TEST_EXES})

  # C executables must link to a shared object
  set(C_TESTS db/c_test.c)
  set(C_TEST_EXES ${C_TESTS})

  foreach(sourcefile ${C_TEST_EXES})
      string(REPLACE ".c" "" exename ${sourcefile})
      string(REGEX REPLACE "^((.+)/)+" "" exename ${exename})
      add_executable(${exename}${ARTIFACT_SUFFIX} ${sourcefile})
      set_target_properties(${exename}${ARTIFACT_SUFFIX}
        PROPERTIES EXCLUDE_FROM_DEFAULT_BUILD_RELEASE 1
        EXCLUDE_FROM_DEFAULT_BUILD_MINRELEASE 1
        EXCLUDE_FROM_DEFAULT_BUILD_RELWITHDEBINFO 1
        )
      target_link_libraries(${exename}${ARTIFACT_SUFFIX} ${ROCKSDB_IMPORT_LIB} testutillib${ARTIFACT_SUFFIX})
      add_test(NAME ${exename} COMMAND ${exename}${ARTIFACT_SUFFIX})
      add_dependencies(check ${exename}${ARTIFACT_SUFFIX})
  endforeach(sourcefile ${C_TEST_EXES})
endif()

option(WITH_TOOLS "build with tools" ON)
if(WITH_TOOLS)
  add_subdirectory(tools)
endif()
