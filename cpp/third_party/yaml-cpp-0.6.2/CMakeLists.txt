###
### CMake settings
###
## Due to Mac OSX we need to keep compatibility with CMake 2.6
# see http://www.cmake.org/Wiki/CMake_Policies
cmake_minimum_required(VERSION 2.6)
# see http://www.cmake.org/cmake/help/cmake-2-8-docs.html#policy:CMP0012
if(POLICY CMP0012)
	cmake_policy(SET CMP0012 OLD)
endif()
# see http://www.cmake.org/cmake/help/cmake-2-8-docs.html#policy:CMP0015
if(POLICY CMP0015)
	cmake_policy(SET CMP0015 OLD)
endif()
# see https://cmake.org/cmake/help/latest/policy/CMP0042.html
if(POLICY CMP0042)
	# Enable MACOSX_RPATH by default.
	cmake_policy(SET CMP0042 NEW)
endif()

include(CheckCXXCompilerFlag)


###
### Project settings
###
project(YAML_CPP)

set(YAML_CPP_VERSION_MAJOR "0")
set(YAML_CPP_VERSION_MINOR "6")
set(YAML_CPP_VERSION_PATCH "2")
set(YAML_CPP_VERSION "${YAML_CPP_VERSION_MAJOR}.${YAML_CPP_VERSION_MINOR}.${YAML_CPP_VERSION_PATCH}")

enable_testing()


###
### Project options
###
## Project stuff
option(YAML_CPP_BUILD_TESTS "Enable testing" ON)
option(YAML_CPP_BUILD_TOOLS "Enable parse tools" ON)
option(YAML_CPP_BUILD_CONTRIB "Enable contrib stuff in library" ON)

## Build options
# --> General
# see http://www.cmake.org/cmake/help/cmake2.6docs.html#variable:BUILD_SHARED_LIBS
#     http://www.cmake.org/cmake/help/cmake2.6docs.html#command:add_library
option(BUILD_SHARED_LIBS "Build Shared Libraries" OFF)

# --> Apple
option(APPLE_UNIVERSAL_BIN "Apple: Build universal binary" OFF)

# --> Microsoft Visual C++
# see http://msdn.microsoft.com/en-us/library/aa278396(v=VS.60).aspx
#     http://msdn.microsoft.com/en-us/library/2kzt1wy3(v=VS.71).aspx
option(MSVC_SHARED_RT "MSVC: Build with shared runtime libs (/MD)" ON)
option(MSVC_STHREADED_RT "MSVC: Build with single-threaded static runtime libs (/ML until VS .NET 2003)" OFF)

###
### Sources, headers, directories and libs
###

# From http://www.cmake.org/pipermail/cmake/2010-March/035992.html:
# function to collect all the sources from sub-directories
# into a single list
function(add_sources)
  get_property(is_defined GLOBAL PROPERTY SRCS_LIST DEFINED)
  if(NOT is_defined)
    define_property(GLOBAL PROPERTY SRCS_LIST
      BRIEF_DOCS "List of source files"
      FULL_DOCS "List of all source files in the entire project")
  endif()
  # make absolute paths
  set(SRCS)
  foreach(s IN LISTS ARGN)
    if(NOT IS_ABSOLUTE "${s}")
      get_filename_component(s "${s}" ABSOLUTE)
    endif()
    list(APPEND SRCS "${s}")
  endforeach()
  # append to global list
  set_property(GLOBAL APPEND PROPERTY SRCS_LIST "${SRCS}")
endfunction(add_sources)

set(header_directory "include/yaml-cpp/")

file(GLOB sources "src/[a-zA-Z]*.cpp")
file(GLOB_RECURSE public_headers "include/yaml-cpp/[a-zA-Z]*.h")
file(GLOB private_headers "src/[a-zA-Z]*.h")

if(YAML_CPP_BUILD_CONTRIB)
	file(GLOB contrib_sources "src/contrib/[a-zA-Z]*.cpp")
	file(GLOB contrib_public_headers "include/yaml-cpp/contrib/[a-zA-Z]*.h")
	file(GLOB contrib_private_headers "src/contrib/[a-zA-Z]*.h")
else()
	add_definitions(-DYAML_CPP_NO_CONTRIB)
endif()

set(library_sources
  ${sources}
  ${public_headers}
  ${private_headers}
  ${contrib_sources}
  ${contrib_public_headers}
  ${contrib_private_headers}
)
add_sources(${library_sources})

if(VERBOSE)
	message(STATUS "sources: ${sources}")
	message(STATUS "public_headers: ${public_headers}")
	message(STATUS "private_headers: ${private_headers}")
	message(STATUS "contrib_sources: ${contrib_sources}")
	message(STATUS "contrib_public_headers: ${contrib_public_headers}")
	message(STATUS "contrib_private_headers: ${contrib_private_headers}")
endif()

include_directories(${YAML_CPP_SOURCE_DIR}/src)
include_directories(${YAML_CPP_SOURCE_DIR}/include)



###
### General compilation settings
###
set(yaml_c_flags ${CMAKE_C_FLAGS})
set(yaml_cxx_flags ${CMAKE_CXX_FLAGS})

if(BUILD_SHARED_LIBS)
	set(LABEL_SUFFIX "shared")
else()
	set(LABEL_SUFFIX "static")
endif()

if(APPLE)
	if(APPLE_UNIVERSAL_BIN)
		set(CMAKE_OSX_ARCHITECTURES ppc;i386)
	endif()
endif()

if(IPHONE)
	set(CMAKE_OSX_SYSROOT "iphoneos4.2")
	set(CMAKE_OSX_ARCHITECTURES "armv6;armv7")
endif()

if(WIN32)
	if(BUILD_SHARED_LIBS)
		add_definitions(-D${PROJECT_NAME}_DLL)	# use or build Windows DLL
	endif()
	if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
		set(CMAKE_INSTALL_PREFIX "C:/")
	endif()
endif()

# GCC or Clang or Intel Compiler specialities
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU" OR
   CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR
   CMAKE_CXX_COMPILER_ID MATCHES "Intel")

	### General stuff
	if(WIN32)
		set(CMAKE_SHARED_LIBRARY_PREFIX "")	# DLLs do not have a "lib" prefix
		set(CMAKE_IMPORT_LIBRARY_PREFIX "")	# same for DLL import libs
		set(CMAKE_LINK_DEF_FILE_FLAG "")	# CMake workaround (2.8.3)
	endif()

	### Project stuff
	if(NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
		set(CMAKE_BUILD_TYPE Release)
	endif()
	#
	set(CMAKE_CXX_FLAGS_RELEASE "-O2")
	set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O2 -g")
	set(CMAKE_CXX_FLAGS_DEBUG "-g")
	set(CMAKE_CXX_FLAGS_MINSIZEREL "-Os")
	#
	set(GCC_EXTRA_OPTIONS "")
	#
    # caiyd 20180313, enable "-fPIC" make option to build yaml-cpp.a,
    # because zdb_fdw.o is a shared library.
    #if(BUILD_SHARED_LIBS)
		set(GCC_EXTRA_OPTIONS "${GCC_EXTRA_OPTIONS} -fPIC")
    #endif()
	#
	set(FLAG_TESTED "-Wextra")
	check_cxx_compiler_flag(${FLAG_TESTED} FLAG_WEXTRA)
	if(FLAG_WEXTRA)
		set(GCC_EXTRA_OPTIONS "${GCC_EXTRA_OPTIONS} ${FLAG_TESTED}")
	endif()
	#
	set(yaml_cxx_flags "-Wall ${GCC_EXTRA_OPTIONS} -pedantic -Wno-long-long -std=c++11 ${yaml_cxx_flags}")

	### Make specific
	if(${CMAKE_BUILD_TOOL} MATCHES make OR ${CMAKE_BUILD_TOOL} MATCHES gmake)
		add_custom_target(debuggable $(MAKE) clean
			COMMAND ${CMAKE_COMMAND} -DCMAKE_BUILD_TYPE=Debug ${CMAKE_SOURCE_DIR}
			COMMENT "Adjusting settings for debug compilation"
			VERBATIM)
		add_custom_target(releasable $(MAKE) clean
			COMMAND ${CMAKE_COMMAND} -DCMAKE_BUILD_TYPE=Release ${CMAKE_SOURCE_DIR}
			COMMENT "Adjusting settings for release compilation"
			VERBATIM)
	endif()
endif()

# Microsoft VisualC++ specialities
if(MSVC)
	### General stuff
	# a) Change MSVC runtime library settings (/MD[d], /MT[d], /ML[d] (single-threaded until VS 2003))
	#    plus set lib suffix for later use and project label accordingly
	# see http://msdn.microsoft.com/en-us/library/aa278396(v=VS.60).aspx
	#     http://msdn.microsoft.com/en-us/library/2kzt1wy3(v=VS.71).aspx
	set(LIB_RT_SUFFIX "md")	# CMake defaults to /MD for MSVC
	set(LIB_RT_OPTION "/MD")
	#
	if(NOT MSVC_SHARED_RT)	# User wants to have static runtime libraries (/MT, /ML)
		if(MSVC_STHREADED_RT)	# User wants to have old single-threaded static runtime libraries
			set(LIB_RT_SUFFIX "ml")
			set(LIB_RT_OPTION "/ML")
			if(NOT ${MSVC_VERSION} LESS 1400)
				message(FATAL_ERROR "Single-threaded static runtime libraries (/ML) only available until VS .NET 2003 (7.1).")
			endif()
		else()
			set(LIB_RT_SUFFIX "mt")
			set(LIB_RT_OPTION "/MT")
		endif()

		# correct linker options
		foreach(flag_var  CMAKE_C_FLAGS  CMAKE_CXX_FLAGS)
			foreach(config_name  ""  DEBUG  RELEASE  MINSIZEREL  RELWITHDEBINFO)
				set(var_name "${flag_var}")
				if(NOT "${config_name}" STREQUAL "")
					set(var_name "${var_name}_${config_name}")
				endif()
				string(REPLACE "/MD" "${LIB_RT_OPTION}" ${var_name} "${${var_name}}")
				set(${var_name} "${${var_name}}" CACHE STRING "" FORCE)
			endforeach()
		endforeach()
	endif()
	#
	set(LABEL_SUFFIX "${LABEL_SUFFIX} ${LIB_RT_SUFFIX}")

	# b) Change prefix for static libraries
	set(CMAKE_STATIC_LIBRARY_PREFIX "lib")	# to distinguish static libraries from DLL import libs

	# c) Correct suffixes for static libraries
	if(NOT BUILD_SHARED_LIBS)
		### General stuff
		set(LIB_TARGET_SUFFIX "${LIB_SUFFIX}${LIB_RT_SUFFIX}")
	endif()

	### Project stuff
	# /W3 = set warning level; see http://msdn.microsoft.com/en-us/library/thxezb7y.aspx
	# /wd4127 = disable warning C4127 "conditional expression is constant"; see http://msdn.microsoft.com/en-us/library/6t66728h.aspx
	# /wd4355 = disable warning C4355 "'this' : used in base member initializer list"; http://msdn.microsoft.com/en-us/library/3c594ae3.aspx
	set(yaml_cxx_flags "/W3 /wd4127 /wd4355 ${yaml_cxx_flags}")
endif()


###
### General install settings
###
set(INCLUDE_INSTALL_ROOT_DIR include)

set(INCLUDE_INSTALL_DIR ${INCLUDE_INSTALL_ROOT_DIR}/yaml-cpp)
set(LIB_INSTALL_DIR "lib${LIB_SUFFIX}")

set(_INSTALL_DESTINATIONS
	RUNTIME DESTINATION bin
	LIBRARY DESTINATION ${LIB_INSTALL_DIR}
	ARCHIVE DESTINATION ${LIB_INSTALL_DIR}
)


###
### Library
###
add_library(yaml-cpp ${library_sources})
set_target_properties(yaml-cpp PROPERTIES
  COMPILE_FLAGS "${yaml_c_flags} ${yaml_cxx_flags}"
)

set_target_properties(yaml-cpp PROPERTIES
	VERSION "${YAML_CPP_VERSION}"
	SOVERSION "${YAML_CPP_VERSION_MAJOR}.${YAML_CPP_VERSION_MINOR}"
	PROJECT_LABEL "yaml-cpp ${LABEL_SUFFIX}"
)

if(IPHONE)
	set_target_properties(yaml-cpp PROPERTIES
		XCODE_ATTRIBUTE_IPHONEOS_DEPLOYMENT_TARGET "3.0"
	)
endif()

if(MSVC)
	if(NOT BUILD_SHARED_LIBS)
		# correct library names
		set_target_properties(yaml-cpp PROPERTIES
			DEBUG_POSTFIX "${LIB_TARGET_SUFFIX}d"
			RELEASE_POSTFIX "${LIB_TARGET_SUFFIX}"
			MINSIZEREL_POSTFIX "${LIB_TARGET_SUFFIX}"
			RELWITHDEBINFO_POSTFIX "${LIB_TARGET_SUFFIX}"
		)
	endif()
endif()

install(TARGETS yaml-cpp EXPORT yaml-cpp-targets ${_INSTALL_DESTINATIONS})
install(
	DIRECTORY ${header_directory}
	DESTINATION ${INCLUDE_INSTALL_DIR}
	FILES_MATCHING PATTERN "*.h"
)

export(
    TARGETS yaml-cpp
    FILE "${PROJECT_BINARY_DIR}/yaml-cpp-targets.cmake")
export(PACKAGE yaml-cpp)
set(EXPORT_TARGETS yaml-cpp CACHE INTERNAL "export targets")

set(CONFIG_INCLUDE_DIRS "${YAML_CPP_SOURCE_DIR}/include")
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/yaml-cpp-config.cmake.in
	"${PROJECT_BINARY_DIR}/yaml-cpp-config.cmake" @ONLY)

if(WIN32 AND NOT CYGWIN)
	set(INSTALL_CMAKE_DIR CMake)
else()
	set(INSTALL_CMAKE_DIR ${LIB_INSTALL_DIR}/cmake/yaml-cpp)
endif()

file(RELATIVE_PATH REL_INCLUDE_DIR "${CMAKE_INSTALL_PREFIX}/${INSTALL_CMAKE_DIR}" "${CMAKE_INSTALL_PREFIX}/${INCLUDE_INSTALL_ROOT_DIR}")
set(CONFIG_INCLUDE_DIRS "\${YAML_CPP_CMAKE_DIR}/${REL_INCLUDE_DIR}")
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/yaml-cpp-config.cmake.in
	"${PROJECT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/yaml-cpp-config.cmake" @ONLY)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/yaml-cpp-config-version.cmake.in
	"${PROJECT_BINARY_DIR}/yaml-cpp-config-version.cmake" @ONLY)

install(FILES
	"${PROJECT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/yaml-cpp-config.cmake"
	"${PROJECT_BINARY_DIR}/yaml-cpp-config-version.cmake"
	DESTINATION "${INSTALL_CMAKE_DIR}" COMPONENT dev)
install(EXPORT yaml-cpp-targets DESTINATION ${INSTALL_CMAKE_DIR})

if(UNIX)
	set(PC_FILE ${CMAKE_BINARY_DIR}/yaml-cpp.pc)
	configure_file("yaml-cpp.pc.cmake" ${PC_FILE} @ONLY)
	install(FILES ${PC_FILE} DESTINATION ${LIB_INSTALL_DIR}/pkgconfig)
endif()


###
### Extras
###
if(YAML_CPP_BUILD_TESTS)
	add_subdirectory(test)
endif()
if(YAML_CPP_BUILD_TOOLS)
	add_subdirectory(util)
endif()

### Formatting
get_property(all_sources GLOBAL PROPERTY SRCS_LIST)
add_custom_target(format
	COMMAND clang-format --style=file -i ${all_sources}
	COMMENT "Running clang-format"
	VERBATIM)
