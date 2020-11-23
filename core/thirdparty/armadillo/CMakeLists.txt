
#-------------------------------------------------------------------------------
# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License.
#-------------------------------------------------------------------------------

if (DEFINED ENV{MILVUS_ARMADILLO_URL})
    set(ARMADILLO_SOURCE_URL  "$ENV{MILVUS_ARMADILLO_URL}")
else ()
   # set(ARMADILLO_SOURCE_URL  "https://gitlab.com/conradsnicta/armadillo-code/-/archive/9.900.x/armadillo-code-9.900.x.tar.gz")
   set(ARMADILLO_SOURCE_URL  "/home/aiteam/Downloads/armadillo-code-9.900.x.tar.gz")
endif ()

macro(build_armadillo)
    message(STATUS "Building armadillo 9.9.x from source")
    set(ARMADILLO_PREFIX "${CMAKE_BINARY_DIR}/3rdparty_download/armadillo-subbuild")
    set(ARMADILLO_INCLUDE_DIR "${ARMADILLO_PREFIX}/include")
    set(ARMADILLO_CMAKE_ARGS "-DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/thirdparty/armadillo")

    ExternalProject_Add(
            armadillo_ep           
            PREFIX ${ARMADILLO_PREFIX}
            DOWNLOAD_DIR   ${THIRDPARTY_DOWNLOAD_PATH}
            INSTALL_DIR    ${CMAKE_BINARY_DIR}/thirdparty/armadillo 
            URL ${ARMADILLO_SOURCE_URL} 
            CMAKE_ARGS  ${ARMADILLO_CMAKE_ARGS}
            BUILD_COMMAND ${MAKE} ${MAKE_BUILD_ARGS}
            INSTALL_COMMAND ${MAKE} install 
            BUILD_BYPRODUCTS
            ${ARMADILLO_SHARED_LIB}
            )
        ExternalProject_Get_Property( armadillo_ep INSTALL_DIR )
        if( NOT IS_DIRECTORY ${INSTALL_DIR}/include )
            file( MAKE_DIRECTORY "${INSTALL_DIR}/include" )
        endif()
        add_library(armadillo SHARED IMPORTED)
        ExTernalProject_Get_Property(armadillo_ep INSTALL_DIR)
    set_target_properties(armadillo
        PROPERTIES
            IMPORTED_GLOBAL    TRUE
            IMPORTED_LOCATION "${INSTALL_DIR}/lib/libarmadillo.so"
            INTERFACE_INCLUDE_DIRECTORIES "${INSTALL_DIR}/include")

    add_dependencies(armadillo armadillo_ep)
endmacro()

build_armadillo()

if(MILVUS_FPGA_VERSION)  
    install(FILES
            ${INSTALL_DIR}/lib/libarmadillo.so
            ${INSTALL_DIR}/lib/libarmadillo.so.9
            ${INSTALL_DIR}/lib/libarmadillo.so.9.900.4
            DESTINATION lib)
    
endif()