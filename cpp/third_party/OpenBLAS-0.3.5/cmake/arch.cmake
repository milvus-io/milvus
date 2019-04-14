##
## Author: Hank Anderson <hank@statease.com>
## Description: Ported from portion of OpenBLAS/Makefile.system
##              Sets various variables based on architecture.

if (X86 OR X86_64)

  if (X86)
    if (NOT BINARY)
      set(NO_BINARY_MODE 1)
    endif ()
  endif ()

  if (NOT NO_EXPRECISION)
    if (${F_COMPILER} MATCHES "GFORTRAN")
      # N.B. I'm not sure if CMake differentiates between GCC and LSB -hpa
      if (${CMAKE_C_COMPILER_ID} STREQUAL "GNU" OR ${CMAKE_C_COMPILER_ID} STREQUAL "LSB")
        set(EXPRECISION	1)
        set(CCOMMON_OPT "${CCOMMON_OPT} -DEXPRECISION -m128bit-long-double")
        set(FCOMMON_OPT	"${FCOMMON_OPT} -m128bit-long-double")
      endif ()
      if (${CMAKE_C_COMPILER_ID} STREQUAL "Clang")
        set(EXPRECISION	1)
        set(CCOMMON_OPT "${CCOMMON_OPT} -DEXPRECISION")
        set(FCOMMON_OPT	"${FCOMMON_OPT} -m128bit-long-double")
      endif ()
    endif ()
  endif ()
endif ()

if (${CMAKE_C_COMPILER_ID} STREQUAL "Intel")
  set(CCOMMON_OPT "${CCOMMON_OPT} -wd981")
endif ()

if (USE_OPENMP)
  # USE_SIMPLE_THREADED_LEVEL3 = 1
  # NO_AFFINITY = 1
  find_package(OpenMP REQUIRED)
  if (OpenMP_FOUND)
    set(CCOMMON_OPT "${CCOMMON_OPT} ${OpenMP_C_FLAGS} -DUSE_OPENMP")
    set(FCOMMON_OPT "${FCOMMON_OPT} ${OpenMP_Fortran_FLAGS}")
  endif()
endif ()


if (DYNAMIC_ARCH)
  if (ARM64)
    set(DYNAMIC_CORE ARMV8 CORTEXA53 CORTEXA57 CORTEXA72 CORTEXA73 FALKOR THUNDERX THUNDERX2T99)
  endif ()
  
  if (X86)
    set(DYNAMIC_CORE KATMAI COPPERMINE NORTHWOOD PRESCOTT BANIAS CORE2 PENRYN DUNNINGTON NEHALEM ATHLON OPTERON OPTERON_SSE3 BARCELONA BOBCAT ATOM NANO)
  endif ()

  if (X86_64)
    set(DYNAMIC_CORE PRESCOTT CORE2)
    if (DYNAMIC_OLDER)
	set (DYNAMIC_CORE ${DYNAMIC_CORE} PENRYN DUNNINGTON)
    endif ()
    set (DYNAMIC_CORE ${DYNAMIC_CORE} NEHALEM)
    if (DYNAMIC_OLDER)
	set (DYNAMIC_CORE ${DYNAMIC_CORE} OPTERON OPTERON_SSE3)
    endif ()
    set (DYNAMIC_CORE ${DYNAMIC_CORE} BARCELONA) 
    if (DYNAMIC_OLDER)
	set (DYNAMIC_CORE ${DYNAMIC_CORE} BOBCAT ATOM NANO)
    endif ()
    if (NOT NO_AVX)
      set(DYNAMIC_CORE ${DYNAMIC_CORE} SANDYBRIDGE BULLDOZER PILEDRIVER STEAMROLLER EXCAVATOR)
    endif ()
    if (NOT NO_AVX2)
      set(DYNAMIC_CORE ${DYNAMIC_CORE} HASWELL ZEN)
    endif ()
    if (NOT NO_AVX512)
      set(DYNAMIC_CORE ${DYNAMIC_CORE} SKYLAKEX)
    endif ()
  endif ()

  if (NOT DYNAMIC_CORE)
    unset(DYNAMIC_ARCH)
  endif ()
endif ()

if (${ARCH} STREQUAL "ia64")
  set(NO_BINARY_MODE 1)
  set(BINARY_DEFINED 1)

  if (${F_COMPILER} MATCHES "GFORTRAN")
    if (${CMAKE_C_COMPILER_ID} STREQUAL "GNU")
      # EXPRECISION	= 1
      # CCOMMON_OPT	+= -DEXPRECISION
    endif ()
  endif ()
endif ()

if (MIPS64)
  set(NO_BINARY_MODE 1)
endif ()

if (${ARCH} STREQUAL "alpha")
  set(NO_BINARY_MODE 1)
  set(BINARY_DEFINED 1)
endif ()

if (ARM)
  set(NO_BINARY_MODE 1)
  set(BINARY_DEFINED 1)
endif ()

if (ARM64)
  set(NO_BINARY_MODE 1)
  set(BINARY_DEFINED 1)
endif ()

