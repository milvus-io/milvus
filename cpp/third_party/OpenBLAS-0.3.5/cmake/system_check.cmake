##
## Author: Hank Anderson <hank@statease.com>
## Description: Ported from the OpenBLAS/c_check perl script.
##              This is triggered by prebuild.cmake and runs before any of the code is built.
##              Creates config.h and Makefile.conf.

# Convert CMake vars into the format that OpenBLAS expects
string(TOUPPER ${CMAKE_SYSTEM_NAME} HOST_OS)
if (${HOST_OS} STREQUAL "WINDOWS")
  set(HOST_OS WINNT)
endif ()

if (${HOST_OS} STREQUAL "LINUX")
# check if we're building natively on Android (TERMUX)
    EXECUTE_PROCESS( COMMAND uname -o COMMAND tr -d '\n' OUTPUT_VARIABLE OPERATING_SYSTEM)
      if(${OPERATING_SYSTEM} MATCHES "Android")
        set(HOST_OS ANDROID)
      endif(${OPERATING_SYSTEM} MATCHES "Android")
endif()



if(CMAKE_COMPILER_IS_GNUCC AND WIN32)
    execute_process(COMMAND ${CMAKE_C_COMPILER} -dumpmachine
              OUTPUT_VARIABLE OPENBLAS_GCC_TARGET_MACHINE
              OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(OPENBLAS_GCC_TARGET_MACHINE MATCHES "amd64|x86_64|AMD64")
      set(MINGW64 1)
    endif()
endif()

# Pretty thorough determination of arch. Add more if needed
if(CMAKE_CL_64 OR MINGW64)
  set(X86_64 1)
elseif(MINGW OR (MSVC AND NOT CMAKE_CROSSCOMPILING))
  set(X86 1)
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "ppc.*|power.*|Power.*")
  set(PPC 1)
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "mips64.*")
  set(MIPS64 1)
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "amd64.*|x86_64.*|AMD64.*")
  set(X86_64 1)
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "i686.*|i386.*|x86.*|amd64.*|AMD64.*")
  set(X86 1)
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(arm.*|ARM.*)")
  set(ARM 1)
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64.*|AARCH64.*)")
  set(ARM64 1)
endif()

if (X86_64)
  set(ARCH "x86_64")
elseif(X86)
  set(ARCH "x86")
elseif(PPC)
  set(ARCH "power")
elseif(ARM)
  set(ARCH "arm")
elseif(ARM64)
  set(ARCH "arm64")
else()
  set(ARCH ${CMAKE_SYSTEM_PROCESSOR} CACHE STRING "Target Architecture")
endif ()

if (NOT BINARY)
  if (X86_64 OR ARM64 OR PPC OR MIPS64)
    set(BINARY 64)
  else ()
    set(BINARY 32)
  endif ()
endif()

if(BINARY EQUAL 64)
  set(BINARY64 1)
else()
  set(BINARY32 1)
endif()

if (X86_64 OR X86)
  file(WRITE ${PROJECT_BINARY_DIR}/avx512.tmp "#include <immintrin.h>\n\nint main(void){ __asm__ volatile(\"vbroadcastss -4 * 4(%rsi), %zmm2\"); }")
execute_process(COMMAND ${CMAKE_C_COMPILER} -march=skylake-avx512 -v -o ${PROJECT_BINARY_DIR}/avx512.o -x c ${PROJECT_BINARY_DIR}/avx512.tmp OUTPUT_QUIET ERROR_QUIET RESULT_VARIABLE NO_AVX512)
if (NO_AVX512 EQUAL 1)
set (CCOMMON_OPT "${CCOMMON_OPT} -DNO_AVX512")
endif()
  file(REMOVE "avx512.tmp" "avx512.o")
endif()

