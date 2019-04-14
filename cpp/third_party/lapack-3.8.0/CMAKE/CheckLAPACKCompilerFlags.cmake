# This module checks against various known compilers and thier respective
# flags to determine any specific flags needing to be set.
#
#  1.  If FPE traps are enabled either abort or disable them
#  2.  Specify fixed form if needed
#  3.  Ensure that Release builds use O2 instead of O3
#
#=============================================================================
# Author: Chuck Atkins
# Copyright 2011
#=============================================================================

macro( CheckLAPACKCompilerFlags )

set( FPE_EXIT FALSE )

# GNU Fortran
if( CMAKE_Fortran_COMPILER_ID STREQUAL "GNU" )
  if( "${CMAKE_Fortran_FLAGS}" MATCHES "-ffpe-trap=[izoupd]")
    set( FPE_EXIT TRUE )
  endif()

# Intel Fortran
elseif( CMAKE_Fortran_COMPILER_ID STREQUAL "Intel" )
  if( "${CMAKE_Fortran_FLAGS}" MATCHES "[-/]fpe(-all=|)0" )
    set( FPE_EXIT TRUE )
  endif()

# SunPro F95
elseif( CMAKE_Fortran_COMPILER_ID STREQUAL "SunPro" )
  if( ("${CMAKE_Fortran_FLAGS}" MATCHES "-ftrap=") AND
      NOT ("${CMAKE_Fortran_FLAGS}" MATCHES "-ftrap=(%|)none") )
    set( FPE_EXIT TRUE )
  elseif( NOT (CMAKE_Fortran_FLAGS MATCHES "-ftrap=") )
    message( STATUS "Disabling FPE trap handlers with -ftrap=%none" )
    set( CMAKE_Fortran_FLAGS "${CMAKE_Fortran_FLAGS} -ftrap=%none"
         CACHE STRING "Flags for Fortran compiler." FORCE )
  endif()

# IBM XL Fortran
elseif( (CMAKE_Fortran_COMPILER_ID STREQUAL "VisualAge" ) OR  # CMake 2.6
        (CMAKE_Fortran_COMPILER_ID STREQUAL "XL" ) )          # CMake 2.8
  if( "${CMAKE_Fortran_FLAGS}" MATCHES "-qflttrap=[a-zA-Z:]:enable" )
    set( FPE_EXIT TRUE )
  endif()

  if( NOT ("${CMAKE_Fortran_FLAGS}" MATCHES "-qfixed") )
    message( STATUS "Enabling fixed format F90/F95 with -qfixed" )
    set( CMAKE_Fortran_FLAGS "${CMAKE_Fortran_FLAGS} -qfixed"
         CACHE STRING "Flags for Fortran compiler." FORCE )
  endif()

# HP Fortran
elseif( CMAKE_Fortran_COMPILER_ID STREQUAL "HP" )
  if( "${CMAKE_Fortran_FLAGS}" MATCHES "\\+fp_exception" )
    set( FPE_EXIT TRUE )
  endif()

  if( NOT ("${CMAKE_Fortran_FLAGS}" MATCHES "\\+fltconst_strict") )
    message( STATUS "Enabling strict float conversion with +fltconst_strict" )
    set( CMAKE_Fortran_FLAGS "${CMAKE_Fortran_FLAGS} +fltconst_strict"
         CACHE STRING "Flags for Fortran compiler." FORCE )
  endif()

  # Most versions of cmake don't have good default options for the HP compiler
  set( CMAKE_Fortran_FLAGS_DEBUG "${CMAKE_Fortran_FLAGS_DEBUG} -g"
       CACHE STRING "Flags used by the compiler during debug builds" FORCE )
  set( CMAKE_Fortran_FLAGS_DEBUG "${CMAKE_Fortran_FLAGS_MINSIZEREL} +Osize"
       CACHE STRING "Flags used by the compiler during release minsize builds" FORCE )
  set( CMAKE_Fortran_FLAGS_DEBUG "${CMAKE_Fortran_FLAGS_RELEASE} +O2"
       CACHE STRING "Flags used by the compiler during release builds" FORCE )
  set( CMAKE_Fortran_FLAGS_DEBUG "${CMAKE_Fortran_FLAGS_RELWITHDEBINFO} +O2 -g"
       CACHE STRING "Flags used by the compiler during release with debug info builds" FORCE )
else()
endif()

if( "${CMAKE_Fortran_FLAGS_RELEASE}" MATCHES "O[3-9]" )
  message( STATUS "Reducing RELEASE optimization level to O2" )
  string( REGEX REPLACE "O[3-9]" "O2" CMAKE_Fortran_FLAGS_RELEASE
          "${CMAKE_Fortran_FLAGS_RELEASE}" )
  set( CMAKE_Fortran_FLAGS_RELEASE "${CMAKE_Fortran_FLAGS_RELEASE}"
       CACHE STRING "Flags used by the compiler during release builds" FORCE )
endif()


if( FPE_EXIT )
  message( FATAL_ERROR "Floating Point Exception (FPE) trap handlers are currently explicitly enabled in the compiler flags.  LAPACK is designed to check for and handle these cases internally and enabling these traps will likely cause LAPACK to crash.  Please re-configure with floating point exception trapping disabled." )
endif()

endmacro()
