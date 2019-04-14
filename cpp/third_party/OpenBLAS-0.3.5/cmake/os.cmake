##
## Author: Hank Anderson <hank@statease.com>
## Description: Ported from portion of OpenBLAS/Makefile.system
##              Detects the OS and sets appropriate variables.

if (${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
  set(EXTRALIB "${EXTRALIB} -lm")
  set(NO_EXPRECISION 1)
endif ()

if (${CMAKE_SYSTEM_NAME} STREQUAL "AIX")
  set(EXTRALIB "${EXTRALIB} -lm")
endif ()

# TODO: this is probably meant for mingw, not other windows compilers
if (${CMAKE_SYSTEM_NAME} STREQUAL "Windows")

  set(NEED_PIC 0)
  set(NO_EXPRECISION 1)

  set(EXTRALIB "${EXTRALIB} -defaultlib:advapi32")

  # probably not going to use these
  set(SUFFIX "obj")
  set(PSUFFIX "pobj")
  set(LIBSUFFIX "a")

  if (${CMAKE_C_COMPILER_ID} STREQUAL "Clang")
    set(CCOMMON_OPT	"${CCOMMON_OPT} -DMS_ABI")
  endif ()

  if (${CMAKE_C_COMPILER_ID} STREQUAL "GNU")

    # Test for supporting MS_ABI
    # removed string parsing in favor of CMake's version comparison -hpa
    execute_process(COMMAND ${CMAKE_C_COMPILER} -dumpversion OUTPUT_VARIABLE GCC_VERSION)
    if (${GCC_VERSION} VERSION_GREATER 4.7 OR ${GCC_VERSION} VERSION_EQUAL 4.7)
      # GCC Version >=4.7
      # It is compatible with MSVC ABI.
      set(CCOMMON_OPT "${CCOMMON_OPT} -DMS_ABI")
    endif ()
  endif ()

  # Ensure the correct stack alignment on Win32
  # http://permalink.gmane.org/gmane.comp.lib.openblas.general/97
  if (X86)
    if (NOT MSVC AND NOT ${CMAKE_C_COMPILER_ID} STREQUAL "Clang")
      set(CCOMMON_OPT "${CCOMMON_OPT} -mincoming-stack-boundary=2")
    endif ()
    set(FCOMMON_OPT "${FCOMMON_OPT} -mincoming-stack-boundary=2")
  endif ()
  
endif ()

if (${CMAKE_SYSTEM_NAME} STREQUAL "Interix")
  set(NEED_PIC 0)
  set(NO_EXPRECISION 1)
  
  set(INTERIX_TOOL_DIR STREQUAL "/opt/gcc.3.3/i586-pc-interix3/bin")
endif ()

if (CYGWIN)
  set(NEED_PIC 0)
  set(NO_EXPRECISION 1)
endif ()

if (NOT ${CMAKE_SYSTEM_NAME} STREQUAL "Windows" AND NOT ${CMAKE_SYSTEM_NAME} STREQUAL "Interix" AND NOT ${CMAKE_SYSTEM_NAME} STREQUAL "Android")
  if (USE_THREAD)
    set(EXTRALIB "${EXTRALIB} -lpthread")
  endif ()
endif ()

if (QUAD_PRECISION)
  set(CCOMMON_OPT "${CCOMMON_OPT} -DQUAD_PRECISION")
  set(NO_EXPRECISION 1)
endif ()

if (X86)
  set(NO_EXPRECISION 1)
endif ()

if (UTEST_CHECK)
  set(CCOMMON_OPT "${CCOMMON_OPT} -DUTEST_CHECK")
  set(SANITY_CHECK 1)
endif ()

if (SANITY_CHECK)
  # TODO: need some way to get $(*F) (target filename)
  set(CCOMMON_OPT "${CCOMMON_OPT} -DSANITY_CHECK -DREFNAME=$(*F)f${BU}")
endif ()

