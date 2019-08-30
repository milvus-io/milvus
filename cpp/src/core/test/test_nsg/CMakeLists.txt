##############################
include_directories(/usr/local/include/gperftools)
link_directories(/usr/local/lib)

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")
add_definitions(-std=c++11 -O3 -lboost -march=native -Wall -DINFO)

find_package(OpenMP)
if (OPENMP_FOUND)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
else ()
    message(FATAL_ERROR "no OpenMP supprot")
endif ()
message(${OpenMP_CXX_FLAGS})

include_directories(${CMAKE_SOURCE_DIR}/src/knowhere/index/vector_index/nsg)
#include_directories(/home/zilliz/opt/app/pyenv/versions/3.6.8/include/python3.6m)
#include_directories(/home/zilliz/opt/app/pyenv/versions/3.6.8/envs/megasearch_testframework_dev/lib/python3.6/site-packages/numpy/core/include)

aux_source_directory(${CMAKE_SOURCE_DIR}/src/knowhere/index/vector_index/nsg nsg_src)

#add_library(nsg_raw SHARED ${nsg_src})
#target_link_libraries(nsg_raw
#        gomp)

set(interface_src
        ${CMAKE_SOURCE_DIR}/src/knowhere/index/vector_index/ivf.cpp
        ${CMAKE_SOURCE_DIR}/src/knowhere/index/vector_index/gpu_ivf.cpp
        ${CMAKE_SOURCE_DIR}/src/knowhere/index/vector_index/cloner.cpp
        ${CMAKE_SOURCE_DIR}/src/knowhere/index/vector_index/idmap.cpp
        ${CMAKE_SOURCE_DIR}/src/knowhere/index/vector_index/nsg_index.cpp
        ${CMAKE_SOURCE_DIR}/src/knowhere/adapter/structure.cpp
        ${CMAKE_SOURCE_DIR}/src/knowhere/common/exception.cpp
        ${CMAKE_SOURCE_DIR}/src/knowhere/common/timer.cpp
        ../utils.cpp
        )

add_executable(test_nsg
        test_nsg.cpp
        ${interface_src}
        ${nsg_src}
        )
#target_link_libraries(test_nsg
#        #         libprofiler.so
##        -ltcmalloc
##        gomp
#        )
target_link_libraries(test_nsg ${depend_libs} ${unittest_libs} ${basic_libs})
##############################