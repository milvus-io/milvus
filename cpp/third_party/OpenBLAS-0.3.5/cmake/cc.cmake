##
## Author: Hank Anderson <hank@statease.com>
## Description: Ported from portion of OpenBLAS/Makefile.system
##              Sets C related variables.

if (${CMAKE_C_COMPILER} STREQUAL "GNU" OR ${CMAKE_C_COMPILER} STREQUAL "LSB" OR ${CMAKE_C_COMPILER} STREQUAL "Clang")

  set(CCOMMON_OPT "${CCOMMON_OPT} -Wall")
  set(COMMON_PROF "${COMMON_PROF} -fno-inline")
  set(NO_UNINITIALIZED_WARN "-Wno-uninitialized")

  if (QUIET_MAKE)
    set(CCOMMON_OPT "${CCOMMON_OPT} ${NO_UNINITIALIZED_WARN} -Wno-unused")
  endif ()

  if (NO_BINARY_MODE)

    if (MIPS64)
      if (BINARY64)
        set(CCOMMON_OPT "${CCOMMON_OPT} -mabi=64")
      else ()
        set(CCOMMON_OPT "${CCOMMON_OPT} -mabi=n32")
      endif ()
      set(BINARY_DEFINED 1)
    endif ()

    if (${CORE} STREQUAL "LOONGSON3A" OR ${CORE} STREQUAL "LOONGSON3B")
      set(CCOMMON_OPT "${CCOMMON_OPT} -march=mips64")
      set(FCOMMON_OPT "${FCOMMON_OPT} -march=mips64")
    endif ()

    if (CMAKE_SYSTEM_NAME STREQUAL "AIX")
      set(BINARY_DEFINED 1)
    endif ()
  endif ()

  if (NOT BINARY_DEFINED)
    if (BINARY64)
      set(CCOMMON_OPT "${CCOMMON_OPT} -m64")
    else ()
      set(CCOMMON_OPT "${CCOMMON_OPT} -m32")
    endif ()
  endif ()
endif ()

if (${CMAKE_C_COMPILER} STREQUAL "PGI")
  if (BINARY64)
    set(CCOMMON_OPT "${CCOMMON_OPT} -tp p7-64")
  else ()
    set(CCOMMON_OPT "${CCOMMON_OPT} -tp p7")
  endif ()
endif ()

if (${CMAKE_C_COMPILER} STREQUAL "PATHSCALE")
  if (BINARY64)
    set(CCOMMON_OPT "${CCOMMON_OPT} -m64")
  else ()
    set(CCOMMON_OPT "${CCOMMON_OPT} -m32")
  endif ()
endif ()

if (${CMAKE_C_COMPILER} STREQUAL "OPEN64")

  if (MIPS64)

    if (NOT BINARY64)
      set(CCOMMON_OPT "${CCOMMON_OPT} -n32")
    else ()
      set(CCOMMON_OPT "${CCOMMON_OPT} -n64")
    endif ()

    if (${CORE} STREQUAL "LOONGSON3A")
      set(CCOMMON_OPT "${CCOMMON_OPT} -loongson3 -static")
    endif ()

    if (${CORE} STREQUAL "LOONGSON3B")
      set(CCOMMON_OPT "${CCOMMON_OPT} -loongson3 -static")
    endif ()

  else ()

    if (BINARY64)
      set(CCOMMON_OPT "${CCOMMON_OPT} -m32")
    else ()
      set(CCOMMON_OPT "${CCOMMON_OPT} -m64")
    endif ()
  endif ()
endif ()

if (${CMAKE_C_COMPILER} STREQUAL "SUN")
  set(CCOMMON_OPT "${CCOMMON_OPT} -w")
  if (X86)
    set(CCOMMON_OPT "${CCOMMON_OPT} -m32")
  else ()
    set(CCOMMON_OPT "${CCOMMON_OPT} -m64")
  endif ()
endif ()

