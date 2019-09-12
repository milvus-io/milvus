include_directories(${CORE_SOURCE_DIR}/thirdparty)
include_directories(${CORE_SOURCE_DIR}/thirdparty/SPTAG/AnnService)
include_directories(${CORE_SOURCE_DIR}/include)
include_directories(${CORE_SOURCE_DIR}/thirdparty/jsoncons-0.126.0/include)
include_directories(/usr/local/cuda/include)
link_directories(/usr/local/cuda/lib64)
link_directories(${CORE_SOURCE_DIR}/thirdparty/tbb)

set(unittest_libs
        gtest gmock gtest_main gmock_main)

message(STATUS "arrow prefix: ${ARROW_PREFIX}")
message(STATUS "libjemalloc_pic path: ${ARROW_PREFIX}/lib/libjemalloc_pic.a")
set(depend_libs
        faiss openblas lapack
        arrow "${ARROW_PREFIX}/lib/libjemalloc_pic.a"
        tbb
        )

set(basic_libs
        cudart cublas
        gomp gfortran pthread
        )


#<IVF-TEST>
set(ivf_srcs
        ${CORE_SOURCE_DIR}/src/knowhere/index/vector_index/ivf.cpp
        ${CORE_SOURCE_DIR}/src/knowhere/index/vector_index/gpu_ivf.cpp
        ${CORE_SOURCE_DIR}/src/knowhere/index/vector_index/cloner.cpp
        ${CORE_SOURCE_DIR}/src/knowhere/index/vector_index/idmap.cpp
        ${CORE_SOURCE_DIR}/src/knowhere/adapter/structure.cpp
        ${CORE_SOURCE_DIR}/src/knowhere/common/exception.cpp
        ${CORE_SOURCE_DIR}/src/knowhere/common/timer.cpp
        utils.cpp
        )
if(NOT TARGET test_ivf)
    add_executable(test_ivf test_ivf.cpp ${ivf_srcs})
endif()
target_link_libraries(test_ivf ${depend_libs} ${unittest_libs} ${basic_libs})

#<IDMAP-TEST>
set(idmap_srcs
        ${CORE_SOURCE_DIR}/src/knowhere/index/vector_index/idmap.cpp
        ${CORE_SOURCE_DIR}/src/knowhere/index/vector_index/ivf.cpp
        ${CORE_SOURCE_DIR}/src/knowhere/index/vector_index/cloner.cpp
        ${CORE_SOURCE_DIR}/src/knowhere/index/vector_index/gpu_ivf.cpp
        ${CORE_SOURCE_DIR}/src/knowhere/adapter/structure.cpp
        ${CORE_SOURCE_DIR}/src/knowhere/common/exception.cpp
        ${CORE_SOURCE_DIR}/src/knowhere/common/timer.cpp
        utils.cpp
        )
if(NOT TARGET test_idmap)
    add_executable(test_idmap test_idmap.cpp ${idmap_srcs})
endif()
target_link_libraries(test_idmap ${depend_libs} ${unittest_libs} ${basic_libs})

#<KDT-TEST>
set(kdt_srcs
        ${CORE_SOURCE_DIR}/src/knowhere/index/preprocessor/normalize.cpp
        ${CORE_SOURCE_DIR}/src/knowhere/index/vector_index/kdt_parameters.cpp
        ${CORE_SOURCE_DIR}/src/knowhere/index/vector_index/cpu_kdt_rng.cpp
        ${CORE_SOURCE_DIR}/src/knowhere/adapter/structure.cpp
        ${CORE_SOURCE_DIR}/src/knowhere/adapter/sptag.cpp
        ${CORE_SOURCE_DIR}/src/knowhere/common/exception.cpp
        ${CORE_SOURCE_DIR}/src/knowhere/adapter/arrow.cpp
        ${CORE_SOURCE_DIR}/src/knowhere/common/timer.cpp
        utils.cpp
        )
if(NOT TARGET test_kdt)
    add_executable(test_kdt test_kdt.cpp ${kdt_srcs})
endif()
target_link_libraries(test_kdt
        SPTAGLibStatic
        ${depend_libs} ${unittest_libs} ${basic_libs})

install(TARGETS test_ivf DESTINATION unittest)
install(TARGETS test_idmap DESTINATION unittest)
install(TARGETS test_kdt DESTINATION unittest)

#add_subdirectory(faiss_ori)
#add_subdirectory(test_nsg)

