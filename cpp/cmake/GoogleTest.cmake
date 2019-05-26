#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Tries to find GTest headers and libraries.
#
# Usage of this module as follows:
#
#  find_package(GTest)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  GTest_HOME - When set, this path is inspected instead of standard library
#                locations as the root of the GTest installation.
#                The environment variable GTEST_HOME overrides this veriable.
#
# This module defines
#  GTEST_INCLUDE_DIR, directory containing headers
#  GTEST_LIBS, directory containing gtest libraries
#  GTEST_STATIC_LIB, path to libgtest.a
#  GTEST_SHARED_LIB, path to libgtest's shared library
#  GTEST_FOUND, whether gtest has been found

if( NOT "${GTEST_HOME}" STREQUAL "")
    file( TO_CMAKE_PATH "${GTEST_HOME}" _native_path )
    list( APPEND _gtest_roots ${_native_path} )
elseif ( GTest_HOME )
    list( APPEND _gtest_roots ${GTest_HOME} )
endif()

# Try the parameterized roots, if they exist
if ( _gtest_roots )
    find_path( GTEST_INCLUDE_DIR NAMES gtest/gtest.h
            PATHS ${_gtest_roots} NO_DEFAULT_PATH
            PATH_SUFFIXES "include" )
    find_library( GTEST_LIBRARIES NAMES gtest gtest_main
            PATHS ${_gtest_roots} NO_DEFAULT_PATH
            PATH_SUFFIXES "lib" )
else ()
    find_path( GTEST_INCLUDE_DIR NAMES gtest/gtest.h )
    find_library( GTEST_LIBRARIES NAMES gtest )
endif ()


if (GTEST_INCLUDE_DIR AND GTEST_LIBRARIES)
    set(GTEST_FOUND TRUE)
    get_filename_component( GTEST_LIBS ${GTEST_LIBRARIES} PATH )
    set(GTEST_LIB_NAME gtest)
    set(GTEST_STATIC_LIB ${GTEST_LIBS}/${CMAKE_STATIC_LIBRARY_PREFIX}${GTEST_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX})
    set(GTEST_MAIN_STATIC_LIB ${GTEST_LIBS}/${CMAKE_STATIC_LIBRARY_PREFIX}${GTEST_LIB_NAME}_main${CMAKE_STATIC_LIBRARY_SUFFIX})
    set(GTEST_SHARED_LIB ${GTEST_LIBS}/${CMAKE_SHARED_LIBRARY_PREFIX}${GTEST_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
else ()
    set(GTEST_FOUND FALSE)
endif ()

if (GTEST_FOUND)
    if (NOT GTest_FIND_QUIETLY)
        message(STATUS "Found the GTest library: ${GTEST_LIBRARIES}")
    endif ()
else ()
    if (NOT GTest_FIND_QUIETLY)
        set(GTEST_ERR_MSG "Could not find the GTest library. Looked in ")
        if ( _gtest_roots )
            set(GTEST_ERR_MSG "${GTEST_ERR_MSG} in ${_gtest_roots}.")
        else ()
            set(GTEST_ERR_MSG "${GTEST_ERR_MSG} system search paths.")
        endif ()
        if (GTest_FIND_REQUIRED)
            message(FATAL_ERROR "${GTEST_ERR_MSG}")
        else (GTest_FIND_REQUIRED)
            message(STATUS "${GTEST_ERR_MSG}")
        endif (GTest_FIND_REQUIRED)
    endif ()
endif ()

mark_as_advanced(
        GTEST_INCLUDE_DIR
        GTEST_LIBS
        GTEST_LIBRARIES
        GTEST_STATIC_LIB
        GTEST_SHARED_LIB
)
