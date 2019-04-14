cmake_minimum_required(VERSION 2.8.12)

project(LAPACK Fortran C)

set(LAPACK_MAJOR_VERSION 3)
set(LAPACK_MINOR_VERSION 8)
set(LAPACK_PATCH_VERSION 0)
set(
  LAPACK_VERSION
  ${LAPACK_MAJOR_VERSION}.${LAPACK_MINOR_VERSION}.${LAPACK_PATCH_VERSION}
  )

# Add the CMake directory for custon CMake modules
set(CMAKE_MODULE_PATH "${LAPACK_SOURCE_DIR}/CMAKE" ${CMAKE_MODULE_PATH})

# Set a default build type if none was specified
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to 'Release' as none was specified.")
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "MinSizeRel" "RelWithDebInfo" "Coverage")
endif()

string(TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE_UPPER)
if(${CMAKE_BUILD_TYPE_UPPER} STREQUAL "COVERAGE")
  message(STATUS "Adding coverage")
  find_package(codecov)
endif()

# By default static library
option(BUILD_SHARED_LIBS "Build shared libraries" OFF)

include(GNUInstallDirs)

# Updated OSX RPATH settings
# In response to CMake 3.0 generating warnings regarding policy CMP0042,
# the OSX RPATH settings have been updated per recommendations found
# in the CMake Wiki:
#  http://www.cmake.org/Wiki/CMake_RPATH_handling#Mac_OS_X_and_the_RPATH
set(CMAKE_MACOSX_RPATH ON)
set(CMAKE_SKIP_BUILD_RPATH FALSE)
set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
list(FIND CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES ${CMAKE_INSTALL_FULL_LIBDIR} isSystemDir)
if("${isSystemDir}" STREQUAL "-1")
  set(CMAKE_INSTALL_RPATH ${CMAKE_INSTALL_FULL_LIBDIR})
  set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
endif()


# Configure the warning and code coverage suppression file
configure_file(
  "${LAPACK_SOURCE_DIR}/CTestCustom.cmake.in"
  "${LAPACK_BINARY_DIR}/CTestCustom.cmake"
  @ONLY
)

include(PreventInSourceBuilds)
include(PreventInBuildInstalls)

if(UNIX)
  if("${CMAKE_Fortran_COMPILER}" MATCHES "ifort")
    set(CMAKE_Fortran_FLAGS "${CMAKE_Fortran_FLAGS} -fp-model strict")
  endif()
  if("${CMAKE_Fortran_COMPILER}" MATCHES "xlf")
    set(CMAKE_Fortran_FLAGS "${CMAKE_Fortran_FLAGS} -qnosave -qstrict=none")
  endif()
# Delete libmtsk in linking sequence for Sun/Oracle Fortran Compiler.
# This library is not present in the Sun package SolarisStudio12.3-linux-x86-bin
  string(REPLACE \;mtsk\; \; CMAKE_Fortran_IMPLICIT_LINK_LIBRARIES "${CMAKE_Fortran_IMPLICIT_LINK_LIBRARIES}")
endif()

if(CMAKE_Fortran_COMPILER_ID STREQUAL "Compaq")
  if(WIN32)
    if(CMAKE_GENERATOR STREQUAL "NMake Makefiles")
      get_filename_component(CMAKE_Fortran_COMPILER_CMDNAM ${CMAKE_Fortran_COMPILER} NAME_WE)
      message(STATUS "Using Compaq Fortran compiler with command name ${CMAKE_Fortran_COMPILER_CMDNAM}")
      set(cmd ${CMAKE_Fortran_COMPILER_CMDNAM})
      string(TOLOWER "${cmd}" cmdlc)
      if(cmdlc STREQUAL "df")
        message(STATUS "Assume the Compaq Visual Fortran Compiler is being used")
        set(CMAKE_Fortran_USE_RESPONSE_FILE_FOR_OBJECTS 1)
        set(CMAKE_Fortran_USE_RESPONSE_FILE_FOR_INCLUDES 1)
        #This is a workaround that is needed to avoid forward-slashes in the
        #filenames listed in response files from incorrectly being interpreted as
        #introducing compiler command options
        if(${BUILD_SHARED_LIBS})
          message(FATAL_ERROR "Making of shared libraries with CVF has not been tested.")
        endif()
        set(str "NMake version 9 or later should be used. NMake version 6.0 which is\n")
        set(str "${str}   included with the CVF distribution fails to build Lapack because\n")
        set(str "${str}   the number of source files exceeds the limit for NMake v6.0\n")
        message(STATUS ${str})
        set(CMAKE_Fortran_LINK_EXECUTABLE "LINK /out:<TARGET> <LINK_FLAGS> <LINK_LIBRARIES> <OBJECTS>")
      endif()
    endif()
  endif()
endif()

# Get Python
message(STATUS "Looking for Python greater than 2.6 - ${PYTHONINTERP_FOUND}")
find_package(PythonInterp 2.7) # lapack_testing.py uses features from python 2.7 and greater
if(PYTHONINTERP_FOUND)
  message(STATUS "Using Python version ${PYTHON_VERSION_STRING}")
else()
  message(STATUS "No suitable Python version found, so skipping summary tests.")
endif()
# --------------------------------------------------

set(LAPACK_INSTALL_EXPORT_NAME lapack-targets)

macro(lapack_install_library lib)
  install(TARGETS ${lib}
    EXPORT ${LAPACK_INSTALL_EXPORT_NAME}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  )
endmacro()

set(PKG_CONFIG_DIR ${CMAKE_INSTALL_LIBDIR}/pkgconfig)

# --------------------------------------------------
# Testing
option(BUILD_TESTING "Build tests" OFF)
enable_testing()
include(CTest)
enable_testing()
message(STATUS "Build tests: ${BUILD_TESTING}")

# --------------------------------------------------
# Organize output files.  On Windows this also keeps .dll files next
# to the .exe files that need them, making tests easy to run.
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${LAPACK_BINARY_DIR}/bin)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${LAPACK_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${LAPACK_BINARY_DIR}/lib)

# --------------------------------------------------
# Check for any necessary platform specific compiler flags
include(CheckLAPACKCompilerFlags)
CheckLAPACKCompilerFlags()

# --------------------------------------------------
# Check second function

include(CheckTimeFunction)
set(TIME_FUNC NONE ${TIME_FUNC})
CHECK_TIME_FUNCTION(NONE TIME_FUNC)
CHECK_TIME_FUNCTION(INT_CPU_TIME TIME_FUNC)
CHECK_TIME_FUNCTION(EXT_ETIME TIME_FUNC)
CHECK_TIME_FUNCTION(EXT_ETIME_ TIME_FUNC)
CHECK_TIME_FUNCTION(INT_ETIME TIME_FUNC)
message(STATUS "--> Will use second_${TIME_FUNC}.f and dsecnd_${TIME_FUNC}.f as timing function.")

set(SECOND_SRC ${LAPACK_SOURCE_DIR}/INSTALL/second_${TIME_FUNC}.f)
set(DSECOND_SRC ${LAPACK_SOURCE_DIR}/INSTALL/dsecnd_${TIME_FUNC}.f)

# deprecated LAPACK and LAPACKE routines
option(BUILD_DEPRECATED "Build deprecated routines" OFF)
message(STATUS "Build deprecated routines: ${BUILD_DEPRECATED}")

# --------------------------------------------------
# Precision to build
# By default all precisions are generated
option(BUILD_SINGLE "Build single precision real" ON)
option(BUILD_DOUBLE "Build double precision real" ON)
option(BUILD_COMPLEX "Build single precision complex" ON)
option(BUILD_COMPLEX16 "Build double precision complex" ON)
message(STATUS "Build single precision real: ${BUILD_SINGLE}")
message(STATUS "Build double precision real: ${BUILD_DOUBLE}")
message(STATUS "Build single precision complex: ${BUILD_COMPLEX}")
message(STATUS "Build double precision complex: ${BUILD_COMPLEX16}")

if(NOT (BUILD_SINGLE OR BUILD_DOUBLE OR BUILD_COMPLEX OR BUILD_COMPLEX16))
  message(FATAL_ERROR "Nothing to build, no precision selected.
  Please enable at least one of these:
  BUILD_SINGLE, BUILD_DOUBLE, BUILD_COMPLEX, BUILD_COMPLEX16.")
endif()

# --------------------------------------------------
# Subdirectories that need to be processed
option(USE_OPTIMIZED_BLAS "Whether or not to use an optimized BLAS library instead of included netlib BLAS" OFF)

# Check the usage of the user provided BLAS libraries
if(BLAS_LIBRARIES)
  include(CheckFortranFunctionExists)
  set(CMAKE_REQUIRED_LIBRARIES ${BLAS_LIBRARIES})
  CHECK_FORTRAN_FUNCTION_EXISTS("dgemm" BLAS_FOUND)
  unset(CMAKE_REQUIRED_LIBRARIES)
  if(BLAS_FOUND)
    message(STATUS "--> BLAS supplied by user is WORKING, will use ${BLAS_LIBRARIES}.")
  else()
    message(ERROR "--> BLAS supplied by user is not WORKING, CANNOT USE ${BLAS_LIBRARIES}.")
    message(ERROR "-->     Will use REFERENCE BLAS (by default)")
    message(ERROR "-->     Or Correct your BLAS_LIBRARIES entry ")
    message(ERROR "-->     Or Consider checking USE_OPTIMIZED_BLAS")
  endif()

# User did not provide a BLAS Library but specified to search for one
elseif(USE_OPTIMIZED_BLAS)
  find_package(BLAS)
endif()

# Neither user specified or optimized BLAS libraries can be used
if(NOT BLAS_FOUND)
  message(STATUS "Using supplied NETLIB BLAS implementation")
  add_subdirectory(BLAS)
  set(BLAS_LIBRARIES blas)
else()
  set(CMAKE_EXE_LINKER_FLAGS
    "${CMAKE_EXE_LINKER_FLAGS} ${BLAS_LINKER_FLAGS}"
    CACHE STRING "Linker flags for executables" FORCE)
  set(CMAKE_MODULE_LINKER_FLAGS
    "${CMAKE_MODULE_LINKER_FLAGS} ${BLAS_LINKER_FLAGS}"
    CACHE STRING "Linker flags for modules" FORCE)
  set(CMAKE_SHARED_LINKER_FLAGS
    "${CMAKE_SHARED_LINKER_FLAGS} ${BLAS_LINKER_FLAGS}"
    CACHE STRING "Linker flags for shared libs" FORCE)
endif()


# --------------------------------------------------
# CBLAS
option(CBLAS "Build CBLAS" OFF)

if(CBLAS)
  add_subdirectory(CBLAS)
endif()

# --------------------------------------------------
# XBLAS

option(USE_XBLAS "Build extended precision (needs XBLAS)" OFF)
if(USE_XBLAS)
  find_library(XBLAS_LIBRARY NAMES xblas)
endif()

option(USE_OPTIMIZED_LAPACK "Whether or not to use an optimized LAPACK library instead of included netlib LAPACK" OFF)

# --------------------------------------------------
# LAPACK
# User did not provide a LAPACK Library but specified to search for one
if(USE_OPTIMIZED_LAPACK)
  find_package(LAPACK)
endif()

# Check the usage of the user provided or automatically found LAPACK libraries
if(LAPACK_LIBRARIES)
  include(CheckFortranFunctionExists)
  set(CMAKE_REQUIRED_LIBRARIES ${LAPACK_LIBRARIES})
  # Check if new routine of 3.4.0 is in LAPACK_LIBRARIES
  CHECK_FORTRAN_FUNCTION_EXISTS("dgeqrt" LATESTLAPACK_FOUND)
  unset(CMAKE_REQUIRED_LIBRARIES)
  if(LATESTLAPACK_FOUND)
    message(STATUS "--> LAPACK supplied by user is WORKING, will use ${LAPACK_LIBRARIES}.")
  else()
    message(ERROR "--> LAPACK supplied by user is not WORKING or is older than LAPACK 3.4.0, CANNOT USE ${LAPACK_LIBRARIES}.")
    message(ERROR "-->     Will use REFERENCE LAPACK (by default)")
    message(ERROR "-->     Or Correct your LAPACK_LIBRARIES entry ")
    message(ERROR "-->     Or Consider checking USE_OPTIMIZED_LAPACK")
  endif()
endif()

# Neither user specified or optimized LAPACK libraries can be used
if(NOT LATESTLAPACK_FOUND)
  message(STATUS "Using supplied NETLIB LAPACK implementation")
  set(LAPACK_LIBRARIES lapack)
  add_subdirectory(SRC)
else()
  set(CMAKE_EXE_LINKER_FLAGS
    "${CMAKE_EXE_LINKER_FLAGS} ${LAPACK_LINKER_FLAGS}"
    CACHE STRING "Linker flags for executables" FORCE)
  set(CMAKE_MODULE_LINKER_FLAGS
    "${CMAKE_MODULE_LINKER_FLAGS} ${LAPACK_LINKER_FLAGS}"
    CACHE STRING "Linker flags for modules" FORCE)
  set(CMAKE_SHARED_LINKER_FLAGS
    "${CMAKE_SHARED_LINKER_FLAGS} ${LAPACK_LINKER_FLAGS}"
    CACHE STRING "Linker flags for shared libs" FORCE)
endif()

if(BUILD_TESTING)
  add_subdirectory(TESTING)
endif()

# --------------------------------------------------
# LAPACKE
option(LAPACKE "Build LAPACKE" OFF)

# LAPACKE has also the interface to some routines from tmglib,
# if LAPACKE_WITH_TMG is selected, we need to add those routines to LAPACKE
option(LAPACKE_WITH_TMG "Build LAPACKE with tmglib routines" OFF)
if(LAPACKE_WITH_TMG)
  set(LAPACKE ON)
endif()
if(BUILD_TESTING OR LAPACKE_WITH_TMG) #already included, avoid double inclusion
  add_subdirectory(TESTING/MATGEN)
endif()

if(LAPACKE)
  add_subdirectory(LAPACKE)
endif()

# --------------------------------------------------
# CPACK Packaging

set(CPACK_PACKAGE_NAME "LAPACK")
set(CPACK_PACKAGE_VENDOR "University of Tennessee, Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "LAPACK- Linear Algebra Package")
set(CPACK_PACKAGE_VERSION_MAJOR 3)
set(CPACK_PACKAGE_VERSION_MINOR 5)
set(CPACK_PACKAGE_VERSION_PATCH 0)
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE")
set(CPACK_PACKAGE_INSTALL_DIRECTORY "LAPACK")
if(WIN32 AND NOT UNIX)
  # There is a bug in NSI that does not handle full unix paths properly. Make
  # sure there is at least one set of four (4) backlasshes.
  set(CPACK_NSIS_HELP_LINK "http:\\\\\\\\http://icl.cs.utk.edu/lapack-forum")
  set(CPACK_NSIS_URL_INFO_ABOUT "http:\\\\\\\\www.netlib.org/lapack")
  set(CPACK_NSIS_CONTACT "lapack@eecs.utk.edu")
  set(CPACK_NSIS_MODIFY_PATH ON)
  set(CPACK_NSIS_DISPLAY_NAME "LAPACK-${LAPACK_VERSION}")
  set(CPACK_PACKAGE_RELOCATABLE "true")
else()
  set(CPACK_GENERATOR "TGZ")
  set(CPACK_SOURCE_GENERATOR TGZ)
  set(CPACK_SOURCE_PACKAGE_FILE_NAME "lapack-${LAPACK_VERSION}")
  set(CPACK_SOURCE_IGNORE_FILES ~$ .svn ${CPACK_SOURCE_IGNORE_FILES})
endif()
include(CPack)


# --------------------------------------------------

if(NOT BLAS_FOUND)
  set(ALL_TARGETS ${ALL_TARGETS} blas)
endif()

if(NOT LATESTLAPACK_FOUND)
  set(ALL_TARGETS ${ALL_TARGETS} lapack)
endif()

if(BUILD_TESTING OR LAPACKE_WITH_TMG)
  set(ALL_TARGETS ${ALL_TARGETS} tmglib)
endif()

# Export lapack targets, not including lapacke, from the
# install tree, if any.
set(_lapack_config_install_guard_target "")
if(ALL_TARGETS)
  install(EXPORT lapack-targets
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/lapack-${LAPACK_VERSION})

  # Choose one of the lapack targets to use as a guard for
  # lapack-config.cmake to load targets from the install tree.
  list(GET ALL_TARGETS 0 _lapack_config_install_guard_target)
endif()

# Include cblas in targets exported from the build tree.
if(CBLAS)
  set(ALL_TARGETS ${ALL_TARGETS} cblas)
endif()

# Include lapacke in targets exported from the build tree.
if(LAPACKE)
  set(ALL_TARGETS ${ALL_TARGETS} lapacke)
endif()

# Export lapack and lapacke targets from the build tree, if any.
set(_lapack_config_build_guard_target "")
if(ALL_TARGETS)
  export(TARGETS ${ALL_TARGETS} FILE lapack-targets.cmake)

  # Choose one of the lapack or lapacke targets to use as a guard
  # for lapack-config.cmake to load targets from the build tree.
  list(GET ALL_TARGETS 0 _lapack_config_build_guard_target)
endif()

configure_file(${LAPACK_SOURCE_DIR}/CMAKE/lapack-config-build.cmake.in
  ${LAPACK_BINARY_DIR}/lapack-config.cmake @ONLY)


configure_file(${CMAKE_CURRENT_SOURCE_DIR}/lapack.pc.in ${CMAKE_CURRENT_BINARY_DIR}/lapack.pc @ONLY)
  install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/lapack.pc
  DESTINATION ${PKG_CONFIG_DIR}
  )

configure_file(${LAPACK_SOURCE_DIR}/CMAKE/lapack-config-install.cmake.in
  ${LAPACK_BINARY_DIR}/CMakeFiles/lapack-config.cmake @ONLY)

include(CMakePackageConfigHelpers)
write_basic_package_version_file(
  ${LAPACK_BINARY_DIR}/lapack-config-version.cmake
  VERSION ${LAPACK_VERSION}
  COMPATIBILITY SameMajorVersion
  )

install(FILES
  ${LAPACK_BINARY_DIR}/CMakeFiles/lapack-config.cmake
  ${LAPACK_BINARY_DIR}/lapack-config-version.cmake
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/lapack-${LAPACK_VERSION}
  )
