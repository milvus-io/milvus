##
## Author: Hank Anderson <hank@statease.com>
## Description: Ported from OpenBLAS/Makefile.prebuild
##              This is triggered by system.cmake and runs before any of the code is built.
##              Creates config.h and Makefile.conf by first running the c_check perl script (which creates those files).
##              Next it runs f_check and appends some fortran information to the files.
##              Then it runs getarch and getarch_2nd for even more environment information.
##              Finally it builds gen_config_h for use at build time to generate config.h.

# CMake vars set by this file:
# CORE
# LIBCORE
# NUM_CORES
# HAVE_MMX
# HAVE_SSE
# HAVE_SSE2
# HAVE_SSE3
# MAKE
# SGEMM_UNROLL_M
# SGEMM_UNROLL_N
# DGEMM_UNROLL_M
# DGEMM_UNROLL_M
# QGEMM_UNROLL_N
# QGEMM_UNROLL_N
# CGEMM_UNROLL_M
# CGEMM_UNROLL_M
# ZGEMM_UNROLL_N
# ZGEMM_UNROLL_N
# XGEMM_UNROLL_M
# XGEMM_UNROLL_N
# CGEMM3M_UNROLL_M
# CGEMM3M_UNROLL_N
# ZGEMM3M_UNROLL_M
# ZGEMM3M_UNROLL_M
# XGEMM3M_UNROLL_N
# XGEMM3M_UNROLL_N

# CPUIDEMU = ../../cpuid/table.o


if (DEFINED CPUIDEMU)
  set(EXFLAGS "-DCPUIDEMU -DVENDOR=99")
endif ()

if (BUILD_KERNEL)
  # set the C flags for just this file
  set(GETARCH2_FLAGS "-DBUILD_KERNEL")
  set(TARGET_CONF "config_kernel.h")
  set(TARGET_CONF_DIR ${PROJECT_BINARY_DIR}/kernel_config/${TARGET_CORE})
else()
  set(TARGET_CONF "config.h")
  set(TARGET_CONF_DIR ${PROJECT_BINARY_DIR})
endif ()

set(TARGET_CONF_TEMP "${PROJECT_BINARY_DIR}/${TARGET_CONF}.tmp")

# c_check
set(FU "")
if (APPLE OR (MSVC AND NOT ${CMAKE_C_COMPILER_ID} MATCHES "Clang"))
  set(FU "_")
endif()

set(COMPILER_ID ${CMAKE_C_COMPILER_ID})
if (${COMPILER_ID} STREQUAL "GNU")
  set(COMPILER_ID "GCC")
endif ()

string(TOUPPER ${ARCH} UC_ARCH)

file(WRITE ${TARGET_CONF_TEMP}
  "#define OS_${HOST_OS}\t1\n"
  "#define ARCH_${UC_ARCH}\t1\n"
  "#define C_${COMPILER_ID}\t1\n"
  "#define __${BINARY}BIT__\t1\n"
  "#define FUNDERSCORE\t${FU}\n")

if (${HOST_OS} STREQUAL "WINDOWSSTORE")
  file(APPEND ${TARGET_CONF_TEMP}
    "#define OS_WINNT\t1\n")
endif ()

# f_check
if (NOT NOFORTRAN)
  include("${PROJECT_SOURCE_DIR}/cmake/f_check.cmake")
endif ()

# Cannot run getarch on target if we are cross-compiling
if (DEFINED CORE AND CMAKE_CROSSCOMPILING AND NOT (${HOST_OS} STREQUAL "WINDOWSSTORE"))
  # Write to config as getarch would

  # TODO: Set up defines that getarch sets up based on every other target
  # Perhaps this should be inside a different file as it grows larger
  file(APPEND ${TARGET_CONF_TEMP}
    "#define ${CORE}\n"
    "#define CHAR_CORENAME \"${CORE}\"\n")
  if ("${CORE}" STREQUAL "ARMV7")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_DATA_SIZE\t65536\n"
      "#define L1_DATA_LINESIZE\t32\n"
      "#define L2_SIZE\t512488\n"
      "#define L2_LINESIZE\t32\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define L2_ASSOCIATIVE\t4\n"
      "#define HAVE_VFPV3\n"
      "#define HAVE_VFP\n")
    set(SGEMM_UNROLL_M 4)
    set(SGEMM_UNROLL_N 4)
    set(DGEMM_UNROLL_M 4)
    set(DGEMM_UNROLL_N 4)
  elseif ("${CORE}" STREQUAL "ARMV8")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_DATA_SIZE\t32768\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L2_SIZE\t262144\n"
      "#define L2_LINESIZE\t64\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define L2_ASSOCIATIVE\t32\n"
      "#define ARMV8\n")
    set(SGEMM_UNROLL_M 4)
    set(SGEMM_UNROLL_N 4)
  elseif ("${CORE}" STREQUAL "CORTEXA57" OR "${CORE}" STREQUAL "CORTEXA53")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_CODE_SIZE\t32768\n"
      "#define L1_CODE_LINESIZE\t64\n"
      "#define L1_CODE_ASSOCIATIVE\t3\n"
      "#define L1_DATA_SIZE\t32768\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L1_DATA_ASSOCIATIVE\t2\n"
      "#define L2_SIZE\t262144\n"
      "#define L2_LINESIZE\t64\n"
      "#define L2_ASSOCIATIVE\t16\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define HAVE_VFPV4\n"
      "#define HAVE_VFPV3\n"
      "#define HAVE_VFP\n"
      "#define HAVE_NEON\n"
      "#define ARMV8\n")
    set(SGEMM_UNROLL_M 16)
    set(SGEMM_UNROLL_N 4)
    set(DGEMM_UNROLL_M 8)
    set(DGEMM_UNROLL_N 4)
    set(CGEMM_UNROLL_M 8)
    set(CGEMM_UNROLL_N 4)
    set(ZGEMM_UNROLL_M 8)
    set(ZGEMM_UNROLL_N 4)
  elseif ("${CORE}" STREQUAL "CORTEXA72" OR "${CORE}" STREQUAL "CORTEXA73")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_CODE_SIZE\t49152\n"
      "#define L1_CODE_LINESIZE\t64\n"
      "#define L1_CODE_ASSOCIATIVE\t3\n"
      "#define L1_DATA_SIZE\t32768\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L1_DATA_ASSOCIATIVE\t2\n"
      "#define L2_SIZE\t524288\n"
      "#define L2_LINESIZE\t64\n"
      "#define L2_ASSOCIATIVE\t16\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define HAVE_VFPV4\n"
      "#define HAVE_VFPV3\n"
      "#define HAVE_VFP\n"
      "#define HAVE_NEON\n"
      "#define ARMV8\n")
    set(SGEMM_UNROLL_M 16)
    set(SGEMM_UNROLL_N 4)
    set(DGEMM_UNROLL_M 8)
    set(DGEMM_UNROLL_N 4)
    set(CGEMM_UNROLL_M 8)
    set(CGEMM_UNROLL_N 4)
    set(ZGEMM_UNROLL_M 8)
    set(ZGEMM_UNROLL_N 4)
  elseif ("${CORE}" STREQUAL "FALKOR")
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_CODE_SIZE\t65536\n"
      "#define L1_CODE_LINESIZE\t64\n"
      "#define L1_CODE_ASSOCIATIVE\t3\n"
      "#define L1_DATA_SIZE\t32768\n"
      "#define L1_DATA_LINESIZE\t128\n"
      "#define L1_DATA_ASSOCIATIVE\t2\n"
      "#define L2_SIZE\t524288\n"
      "#define L2_LINESIZE\t64\n"
      "#define L2_ASSOCIATIVE\t16\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define HAVE_VFPV4\n"
      "#define HAVE_VFPV3\n"
      "#define HAVE_VFP\n"
      "#define HAVE_NEON\n"
      "#define ARMV8\n")
    set(SGEMM_UNROLL_M 16)
    set(SGEMM_UNROLL_N 4)
    set(DGEMM_UNROLL_M 8)
    set(DGEMM_UNROLL_N 4)
    set(CGEMM_UNROLL_M 8)
    set(CGEMM_UNROLL_N 4)
    set(ZGEMM_UNROLL_M 8)
    set(ZGEMM_UNROLL_N 4)
  elseif ("${CORE}" STREQUAL "THUNDERX)
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_CODE_SIZE\t32768\n"
      "#define L1_CODE_LINESIZE\t64\n"
      "#define L1_CODE_ASSOCIATIVE\t3\n"
      "#define L1_DATA_SIZE\t32768\n"
      "#define L1_DATA_LINESIZE\t128\n"
      "#define L1_DATA_ASSOCIATIVE\t2\n"
      "#define L2_SIZE\t167772164\n"
      "#define L2_LINESIZE\t128\n"
      "#define L2_ASSOCIATIVE\t16\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define HAVE_VFPV4\n"
      "#define HAVE_VFPV3\n"
      "#define HAVE_VFP\n"
      "#define HAVE_NEON\n"
      "#define ARMV8\n")
    set(SGEMM_UNROLL_M 4)
    set(SGEMM_UNROLL_N 4)
    set(DGEMM_UNROLL_M 2)
    set(DGEMM_UNROLL_N 2)
    set(CGEMM_UNROLL_M 2)
    set(CGEMM_UNROLL_N 2)
    set(ZGEMM_UNROLL_M 2)
    set(ZGEMM_UNROLL_N 2)
  elseif ("${CORE}" STREQUAL "THUNDERX2T99)
    file(APPEND ${TARGET_CONF_TEMP}
      "#define L1_CODE_SIZE\t32768\n"
      "#define L1_CODE_LINESIZE\t64\n"
      "#define L1_CODE_ASSOCIATIVE\t8\n"
      "#define L1_DATA_SIZE\t32768\n"
      "#define L1_DATA_LINESIZE\t64\n"
      "#define L1_DATA_ASSOCIATIVE\t8\n"
      "#define L2_SIZE\t262144\n"
      "#define L2_LINESIZE\t64\n"
      "#define L2_ASSOCIATIVE\t8\n"
      "#define L3_SIZE\t33554432\n"
      "#define L3_LINESIZE\t64\n"
      "#define L3_ASSOCIATIVE\t32\n"
      "#define DTB_DEFAULT_ENTRIES\t64\n"
      "#define DTB_SIZE\t4096\n"
      "#define VULCAN\n")
    set(SGEMM_UNROLL_M 16)
    set(SGEMM_UNROLL_N 4)
    set(DGEMM_UNROLL_M 8)
    set(DGEMM_UNROLL_N 4)
    set(CGEMM_UNROLL_M 8)
    set(CGEMM_UNROLL_N 4)
    set(ZGEMM_UNROLL_M 4)
    set(ZGEMM_UNROLL_N 4)
  endif()

  # Or should this actually be NUM_CORES?
  if (${NUM_THREADS} GREATER 0)
    file(APPEND ${TARGET_CONF_TEMP} "#define NUM_CORES\t${NUM_THREADS}\n")
  endif()

  # GetArch_2nd
  foreach(float_char S;D;Q;C;Z;X)
    if (NOT DEFINED ${float_char}GEMM_UNROLL_M)
      set(${float_char}GEMM_UNROLL_M 2)
    endif()
    if (NOT DEFINED ${float_char}GEMM_UNROLL_N)
      set(${float_char}GEMM_UNROLL_N 2)
    endif()
  endforeach()
  file(APPEND ${TARGET_CONF_TEMP}
    "#define GEMM_MULTITHREAD_THRESHOLD\t${GEMM_MULTITHREAD_THRESHOLD}\n")
  # Move to where gen_config_h would place it
  file(MAKE_DIRECTORY ${TARGET_CONF_DIR})
  file(RENAME ${TARGET_CONF_TEMP} "${TARGET_CONF_DIR}/${TARGET_CONF}")  

else(NOT CMAKE_CROSSCOMPILING)
  # compile getarch
  set(GETARCH_SRC
    ${PROJECT_SOURCE_DIR}/getarch.c
    ${CPUIDEMU}
  )

  if ("${CMAKE_C_COMPILER_ID}" STREQUAL "MSVC")
    #Use generic for MSVC now
    message("MSVC")
    set(GETARCH_FLAGS ${GETARCH_FLAGS} -DFORCE_GENERIC)
  else()
    list(APPEND GETARCH_SRC ${PROJECT_SOURCE_DIR}/cpuid.S)
  endif ()

  if ("${CMAKE_SYSTEM_NAME}" STREQUAL "WindowsStore")
    # disable WindowsStore strict CRT checks
    set(GETARCH_FLAGS ${GETARCH_FLAGS} -D_CRT_SECURE_NO_WARNINGS)
  endif ()

  set(GETARCH_DIR "${PROJECT_BINARY_DIR}/getarch_build")
  set(GETARCH_BIN "getarch${CMAKE_EXECUTABLE_SUFFIX}")
  file(MAKE_DIRECTORY ${GETARCH_DIR})
  configure_file(${TARGET_CONF_TEMP} ${GETARCH_DIR}/${TARGET_CONF} COPYONLY)
  if (NOT "${CMAKE_SYSTEM_NAME}" STREQUAL "WindowsStore")
    try_compile(GETARCH_RESULT ${GETARCH_DIR}
      SOURCES ${GETARCH_SRC}
    COMPILE_DEFINITIONS ${EXFLAGS} ${GETARCH_FLAGS} -I${GETARCH_DIR} -I"${PROJECT_SOURCE_DIR}" -I"${PROJECT_BINARY_DIR}"
      OUTPUT_VARIABLE GETARCH_LOG
      COPY_FILE ${PROJECT_BINARY_DIR}/${GETARCH_BIN}
    )
  
    if (NOT ${GETARCH_RESULT})
      MESSAGE(FATAL_ERROR "Compiling getarch failed ${GETARCH_LOG}")
    endif ()
  endif ()
  message(STATUS "Running getarch")

  # use the cmake binary w/ the -E param to run a shell command in a cross-platform way
execute_process(COMMAND "${PROJECT_BINARY_DIR}/${GETARCH_BIN}" 0 OUTPUT_VARIABLE GETARCH_MAKE_OUT)
execute_process(COMMAND "${PROJECT_BINARY_DIR}/${GETARCH_BIN}" 1 OUTPUT_VARIABLE GETARCH_CONF_OUT)

  message(STATUS "GETARCH results:\n${GETARCH_MAKE_OUT}")

  # append config data from getarch to the TARGET file and read in CMake vars
  file(APPEND ${TARGET_CONF_TEMP} ${GETARCH_CONF_OUT})
  ParseGetArchVars(${GETARCH_MAKE_OUT})

  set(GETARCH2_DIR "${PROJECT_BINARY_DIR}/getarch2_build")
  set(GETARCH2_BIN "getarch_2nd${CMAKE_EXECUTABLE_SUFFIX}")
  file(MAKE_DIRECTORY ${GETARCH2_DIR})
  configure_file(${TARGET_CONF_TEMP} ${GETARCH2_DIR}/${TARGET_CONF} COPYONLY)
  if (NOT "${CMAKE_SYSTEM_NAME}" STREQUAL "WindowsStore")
    try_compile(GETARCH2_RESULT ${GETARCH2_DIR}
      SOURCES ${PROJECT_SOURCE_DIR}/getarch_2nd.c
    COMPILE_DEFINITIONS ${EXFLAGS} ${GETARCH_FLAGS} ${GETARCH2_FLAGS} -I${GETARCH2_DIR} -I"${PROJECT_SOURCE_DIR}" -I"${PROJECT_BINARY_DIR}"
      OUTPUT_VARIABLE GETARCH2_LOG
      COPY_FILE ${PROJECT_BINARY_DIR}/${GETARCH2_BIN}
    )

    if (NOT ${GETARCH2_RESULT})
      MESSAGE(FATAL_ERROR "Compiling getarch_2nd failed ${GETARCH2_LOG}")
    endif ()
  endif ()

  # use the cmake binary w/ the -E param to run a shell command in a cross-platform way
execute_process(COMMAND "${PROJECT_BINARY_DIR}/${GETARCH2_BIN}" 0 OUTPUT_VARIABLE GETARCH2_MAKE_OUT)
execute_process(COMMAND "${PROJECT_BINARY_DIR}/${GETARCH2_BIN}" 1 OUTPUT_VARIABLE GETARCH2_CONF_OUT)

  # append config data from getarch_2nd to the TARGET file and read in CMake vars
  file(APPEND ${TARGET_CONF_TEMP} ${GETARCH2_CONF_OUT})

  configure_file(${TARGET_CONF_TEMP} ${TARGET_CONF_DIR}/${TARGET_CONF} COPYONLY)

  ParseGetArchVars(${GETARCH2_MAKE_OUT})

endif()
