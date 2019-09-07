##############################
include_directories(/usr/local/include/gperftools)
link_directories(/usr/local/lib)

add_definitions(-std=c++11 -O3 -lboost -march=native -Wall -DINFO)

find_package(OpenMP)
if (OPENMP_FOUND)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
else ()
    message(FATAL_ERROR "no OpenMP supprot")
endif ()
message(${OpenMP_CXX_FLAGS})

include_directories(${CORE_SOURCE_DIR}/src/knowhere/index/vector_index/nsg)

aux_source_directory(${CORE_SOURCE_DIR}/src/knowhere/index/vector_index/nsg nsg_src)

set(interface_src
        ${CORE_SOURCE_DIR}/src/knowhere/index/vector_index/ivf.cpp
        ${CORE_SOURCE_DIR}/src/knowhere/index/vector_index/gpu_ivf.cpp
        ${CORE_SOURCE_DIR}/src/knowhere/index/vector_index/cloner.cpp
        ${CORE_SOURCE_DIR}/src/knowhere/index/vector_index/idmap.cpp
        ${CORE_SOURCE_DIR}/src/knowhere/index/vector_index/nsg_index.cpp
        ${CORE_SOURCE_DIR}/src/knowhere/adapter/structure.cpp
        ${CORE_SOURCE_DIR}/src/knowhere/common/exception.cpp
        ${CORE_SOURCE_DIR}/src/knowhere/common/timer.cpp
        ../utils.cpp
        )

if(NOT TARGET test_nsg)
    add_executable(test_nsg
            test_nsg.cpp
            ${interface_src}
            ${nsg_src}
            )
endif()

target_link_libraries(test_nsg ${depend_libs} ${unittest_libs} ${basic_libs})
##############################

install(TARGETS test_nsg DESTINATION unittest)