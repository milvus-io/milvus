##
## Author: Hank Anderson <hank@statease.com>
## Copyright: (c) Stat-Ease, Inc.
## Created: 12/29/14
## Last Modified: 12/29/14
## Description: Ported from the OpenBLAS/f_check perl script.
##              This is triggered by prebuild.cmake and runs before any of the code is built.
##              Appends Fortran information to config.h and Makefile.conf.

# CMake vars set by this file:
# F_COMPILER
# FC
# BU
# NOFORTRAN
# NEED2UNDERSCORES
# FEXTRALIB

# Defines set by this file:
# BUNDERSCORE
# NEEDBUNDERSCORE
# NEED2UNDERSCORES

if (NOT NO_LAPACK)
  enable_language(Fortran)
else()
  include(CMakeForceCompiler)
  CMAKE_FORCE_Fortran_COMPILER(gfortran GNU)
endif()

if (NOT ONLY_CBLAS)
  # run f_check (appends to TARGET files)

  # TODO: detect whether underscore needed, set #defines and BU appropriately - use try_compile
  # TODO: set FEXTRALIB flags a la f_check?

  set(BU "_")
  file(APPEND ${TARGET_CONF_TEMP}
    "#define BUNDERSCORE _\n"
    "#define NEEDBUNDERSCORE 1\n"
    "#define NEED2UNDERSCORES 0\n")

else ()

  #When we only build CBLAS, we set NOFORTRAN=2
  set(NOFORTRAN 2)
  set(NO_FBLAS 1)
  #set(F_COMPILER GFORTRAN) # CMake handles the fortran compiler
  set(BU "_")
  file(APPEND ${TARGET_CONF_TEMP}
    "#define BUNDERSCORE _\n"
    "#define NEEDBUNDERSCORE 1\n")
endif()

get_filename_component(F_COMPILER ${CMAKE_Fortran_COMPILER} NAME_WE)
string(TOUPPER ${F_COMPILER} F_COMPILER)

