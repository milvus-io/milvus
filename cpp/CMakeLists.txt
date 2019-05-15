#-------------------------------------------------------------------------------
# Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited.
# Proprietary and confidential.
#-------------------------------------------------------------------------------

cmake_minimum_required(VERSION 3.12)

project(vecwise_engine LANGUAGES CUDA CXX)

find_package(CUDA)
set(CUDA_NVCC_FLAGS "${CUDA_NVCC_FLAGS} -Xcompiler -fPIC -std=c++11 -D_FORCE_INLINES -arch sm_60 --expt-extended-lambda")
set(CUDA_NVCC_FLAGS "${CUDA_NVCC_FLAGS} -O0 -g")
message("CUDA_TOOLKIT_ROOT_DIR=${CUDA_TOOLKIT_ROOT_DIR}")
message("CUDA_NVCC_FLAGS=${CUDA_NVCC_FLAGS}")

if (GPU_VERSION STREQUAL "ON")
    add_definitions("-DGPU_VERSION")
endif ()

set(CMAKE_CXX_STANDARD 14)

if(CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
    message("building vecwise_engine on x86 architecture")
    set(VECWISE_BUILD_ARCH x86_64)
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "(ppc)")
    message("building vecwise_engine on ppc architecture")
    set(VECWISE_BUILD_ARCH ppc64le)
else()
    message("unknown processor type")
    message("CMAKE_SYSTEM_PROCESSOR=${CMAKE_SYSTEM_PROCESSOR}")
    set(VECWISE_BUILD_ARCH unknown)
endif()

if(DEFINED UNIX)
    message("building vecwise on Unix")
    set(VECWISE_BUILD_SYSTEM macos)
elseif(DEFINED APPLE)
    message("building vecwise on MacOS")
    set(VECWISE_BUILD_SYSTEM unix)
else()
    message("unknown OS")
    set(VECWISE_BUILD_SYSTEM unknown)
endif ()

if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -fPIC -DELPP_THREAD_SAFE")
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g -fPIC -DELPP_THREAD_SAFE")
endif()

if (GPU_VERSION STREQUAL "ON")
    set(ENABLE_LICENSE "ON")
    add_definitions("-DENABLE_LICENSE")
endif ()

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Modules/" ${CMAKE_MODULE_PATH})

set(VECWISE_ENGINE_INCLUDE ${PROJECT_SOURCE_DIR}/include)
set(VECWISE_ENGINE_SRC ${PROJECT_SOURCE_DIR}/src)
set(VECWISE_THIRD_PARTY ${CMAKE_CURRENT_SOURCE_DIR}/third_party)
set(VECWISE_THIRD_PARTY_BUILD ${CMAKE_CURRENT_SOURCE_DIR}/third_party/build)

add_compile_definitions(PROFILER=${PROFILER})

include_directories(${VECWISE_ENGINE_INCLUDE})
include_directories(${VECWISE_ENGINE_SRC})
include_directories(${VECWISE_THIRD_PARTY_BUILD}/include)

link_directories(${CMAKE_CURRRENT_BINARY_DIR})
link_directories(${VECWISE_THIRD_PARTY_BUILD}/lib)
link_directories(${VECWISE_THIRD_PARTY_BUILD}/lib64)
#execute_process(COMMAND bash build.sh
#                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/third_party)

add_subdirectory(src)
add_subdirectory(test_client)

if (BUILD_UNIT_TEST)
    add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/unittest)
endif(BUILD_UNIT_TEST)

add_custom_target(Clean-All COMMAND ${CMAKE_BUILD_TOOL} clean)

#install
install(FILES
        scripts/start_server.sh
        scripts/stop_server.sh
        DESTINATION
        scripts)
install(FILES
        conf/server_config.yaml
        conf/vecwise_engine_log.conf
        DESTINATION
        conf)
