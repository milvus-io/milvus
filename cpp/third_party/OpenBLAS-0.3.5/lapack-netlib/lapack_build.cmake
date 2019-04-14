##
## HINTS: ctest -Ddashboard_model=Continuous   -S $(pwd)/lapack/lapack_build.cmake
## HINTS: ctest -Ddashboard_model=Experimental -S $(pwd)/lapack/lapack_build.cmake
## HINTS: ctest -Ddashboard_model=Nightly      -S $(pwd)/lapack/lapack_build.cmake
##

cmake_minimum_required(VERSION 2.8.10)
###################################################################
# The values in this section must always be provided
###################################################################
if(UNIX)
  if(NOT compiler)
    set(compiler gcc)
  endif()
  if(NOT c_compiler)
    set(c_compiler gcc)
  endif()
  if(NOT full_compiler)
    set(full_compiler g++)
  endif()
endif()

if(EXISTS "/proc/cpuinfo")
  set(parallel 1)
  file(STRINGS "/proc/cpuinfo" CPUINFO)
  foreach(line ${CPUINFO})
    if("${line}" MATCHES processor)
      math(EXPR parallel "${parallel} + 1")
    endif()
  endforeach()
endif()

if(WIN32)
  set(VSLOCATIONS
    "[HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\VisualStudio\\6.0\\Setup;VsCommonDir]/MSDev98/Bin"
    "[HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\VisualStudio\\7.0\\Setup\\VS;EnvironmentDirectory]"
    "[HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\VisualStudio\\7.1\\Setup\\VS;EnvironmentDirectory]"
    "[HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\VisualStudio\\8.0;InstallDir]"
    "[HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\VisualStudio\\8.0\\Setup;Dbghelp_path]"
    "[HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\VisualStudio\\9.0\\Setup\\VS;EnvironmentDirectory]"
    )
  set(GENERATORS
    "Visual Studio 6"
    "Visual Studio 7"
    "Visual Studio 7 .NET 2003"
    "Visual Studio 8 2005"
    "Visual Studio 8 2005"
    "Visual Studio 9 2008"
    )
  set(vstype 0)
  foreach(p ${VSLOCATIONS})
    get_filename_component(VSPATH ${p} PATH)
    if(NOT "${VSPATH}" STREQUAL "/" AND EXISTS "${VSPATH}")
      message(" found VS install = ${VSPATH}")
      set(genIndex ${vstype})
    endif()
    math(EXPR vstype "${vstype} +1")
  endforeach()
  if(NOT DEFINED genIndex)
    message(FATAL_ERROR "Could not find installed visual stuido")
  endif()
  list(GET GENERATORS ${genIndex} GENERATOR)
  set(CTEST_CMAKE_GENERATOR      "${GENERATOR}")
  message("${CTEST_CMAKE_GENERATOR} - found")
  set(compiler cl)
endif()

find_program(HOSTNAME NAMES hostname)
find_program(UNAME NAMES uname)

# Get the build name and hostname
exec_program(${HOSTNAME} ARGS OUTPUT_VARIABLE hostname)
string(REGEX REPLACE "[/\\\\+<> #]" "-" hostname "${hostname}")

message("HOSTNAME: ${hostname}")
# default to parallel 1
if(NOT DEFINED parallel)
  set(parallel 1)
endif()

find_package(Git REQUIRED)

set(CTEST_GIT_COMMAND     ${GIT_EXECUTABLE})
set(CTEST_UPDATE_COMMAND  ${GIT_EXECUTABLE})
macro(getuname name flag)
  exec_program("${UNAME}" ARGS "${flag}" OUTPUT_VARIABLE "${name}")
  string(REGEX REPLACE "[/\\\\+<> #]" "-" "${name}" "${${name}}")
  string(REGEX REPLACE "^(......|.....|....|...|..|.).*" "\\1" "${name}" "${${name}}")
endmacro()

getuname(osname -s)
getuname(osver  -v)
getuname(osrel  -r)
getuname(cpu    -m)
if("${osname}" MATCHES Darwin)
  find_program(SW_VER sw_vers)
  execute_process(COMMAND "${SW_VER}" -productVersion OUTPUT_VARIABLE osver)
  string(REPLACE "\n" "" osver "${osver}")
  set(osname "MacOSX")
  set(osrel "")
  if("${cpu}" MATCHES "Power")
    set(cpu "ppc")
  endif()
endif()

if(NOT compiler)
  message(FATAL_ERROR "compiler must be set")
endif()


set(BUILDNAME "${osname}${osver}${osrel}${cpu}-${compiler}")
message("BUILDNAME: ${BUILDNAME}")

# this is the module name that should be checked out
set (CTEST_DIR_NAME "lapackGIT")

# Settings:
message("NOSPACES = ${NOSPACES}")
if(NOSPACES)
  set(CTEST_DASHBOARD_ROOT    "$ENV{HOME}/Dashboards/MyTests-${BUILDNAME}")
else()
  set(CTEST_DASHBOARD_ROOT    "$ENV{HOME}/Dashboards/My Tests-${BUILDNAME}")
endif()
set(CTEST_SITE              "${hostname}")
set(CTEST_BUILD_NAME        "${BUILDNAME}")
set(CTEST_TEST_TIMEOUT           "36000")

# GIT command and the checkout command
# Select Git source to use.
if(NOT DEFINED dashboard_git_url)
  set(dashboard_git_url "https://github.com/Reference-LAPACK/lapack.git")
endif()
if(NOT DEFINED dashboard_git_branch)
  set(dashboard_git_branch master)
endif()

if(NOT EXISTS "${CTEST_DASHBOARD_ROOT}/${CTEST_DIR_NAME}")
  set(CTEST_CHECKOUT_COMMAND
    "\"${CTEST_UPDATE_COMMAND}\" clone ${dashboard_git_url} ${CTEST_DIR_NAME}")
endif()

# Explicitly specify the remote as "origin". This ensure we are pulling from
# the correct remote and prevents command failures when the git tracking
# branch has not been configured.
set(CTEST_GIT_UPDATE_CUSTOM "${CTEST_GIT_COMMAND}" pull origin ${dashboard_git_branch})

# Set the generator and build configuration
if(NOT DEFINED CTEST_CMAKE_GENERATOR)
  set(CTEST_CMAKE_GENERATOR      "Unix Makefiles")
endif()
set(CTEST_PROJECT_NAME         "LAPACK")
set(CTEST_BUILD_CONFIGURATION  "Release")

# Extra special variables
set(ENV{DISPLAY}             "")
if(CTEST_CMAKE_GENERATOR MATCHES Makefiles)
  set(ENV{CC}                  "${c_compiler}")
  set(ENV{FC}                  "${f_compiler}")
  set(ENV{CXX}                 "${full_compiler}")
endif()

#----------------------------------------------------------------------------------
# Should not need to edit under this line
#----------------------------------------------------------------------------------

# if you do not want to use the default location for a
# dashboard then set this variable to the directory
# the dashboard should be in
make_directory("${CTEST_DASHBOARD_ROOT}")
# these are the the name of the source and binary directory on disk.
# They will be appended to DASHBOARD_ROOT
set(CTEST_SOURCE_DIRECTORY  "${CTEST_DASHBOARD_ROOT}/${CTEST_DIR_NAME}")
set(CTEST_BINARY_DIRECTORY  "${CTEST_SOURCE_DIRECTORY}-${CTEST_BUILD_NAME}")
set(CTEST_NOTES_FILES  "${CTEST_NOTES_FILES}"
  "${CMAKE_CURRENT_LIST_FILE}"
  )

# check for parallel
if(parallel GREATER 1 )
  if(NOT CTEST_BUILD_COMMAND)
    set(CTEST_BUILD_COMMAND "make -j${parallel} -i")
  endif()

  message("Use parallel build")
  message("CTEST_BUILD_COMMAND: ${CTEST_BUILD_COMMAND}")
  message("CTEST_CONFIGURE_COMMAND: ${CTEST_CONFIGURE_COMMAND}")
endif()

###################################################################
# Values for the cmake build
###################################################################

set( CACHE_CONTENTS "
SITE:STRING=${hostname}
BUILDNAME:STRING=${BUILDNAME}
DART_ROOT:PATH=
GITCOMMAND:FILEPATH=${CTEST_UPDATE_COMMAND}
DROP_METHOD:STRING=https
DART_TESTING_TIMEOUT:STRING=${CTEST_TEST_TIMEOUT}
#Set build type to use optimized build
CMAKE_BUILD_TYPE:STRING=Release
# Enable LAPACKE
LAPACKE:OPTION=ON
CBLAS:OPTION=ON
# Use Reference BLAS by default
USE_OPTIMIZED_BLAS:OPTION=OFF
USE_OPTIMIZED_LAPACK:OPTION=OFF
" )


##########################################################################
# wipe the binary dir
message("Remove binary directory...")
ctest_empty_binary_directory("${CTEST_BINARY_DIRECTORY}")

message("CTest Directory: ${CTEST_DASHBOARD_ROOT}")
message("Initial checkout: ${CTEST_CVS_CHECKOUT}")
message("Initial cmake: ${CTEST_CMAKE_COMMAND}")
message("CTest command: ${CTEST_COMMAND}")

# this is the initial cache to use for the binary tree, be careful to escape
# any quotes inside of this string if you use it
file(WRITE "${CTEST_BINARY_DIRECTORY}/CMakeCache.txt" "${CACHE_CONTENTS}")

# Select the model (Nightly, Experimental, Continuous).
if(NOT DEFINED dashboard_model)
  set(dashboard_model Nightly)
endif()
if(NOT "${dashboard_model}" MATCHES "^(Nightly|Experimental|Continuous)$")
  message(FATAL_ERROR "dashboard_model must be Nightly, Experimental, or Continuous")
endif()

message("Start dashboard...")
ctest_start(${dashboard_model})
#ctest_start(Experimental)
message("  Update")
ctest_update(SOURCE "${CTEST_SOURCE_DIRECTORY}" RETURN_VALUE res)
message("  Configure")
ctest_configure(BUILD "${CTEST_BINARY_DIRECTORY}" RETURN_VALUE res)
message("read custom files after configure")
ctest_read_custom_files("${CTEST_BINARY_DIRECTORY}")
message("  Build")
ctest_build(BUILD "${CTEST_BINARY_DIRECTORY}" RETURN_VALUE res)
message("  Test")
ctest_test(BUILD "${CTEST_BINARY_DIRECTORY}" RETURN_VALUE res)
#ctest_test(BUILD "${CTEST_BINARY_DIRECTORY}" INCLUDE "Summary")
message("  Submit")
ctest_submit(RETURN_VALUE res)
message("  All done")


