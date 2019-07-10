include_directories("${CUDA_TOOLKIT_ROOT_DIR}/include")
link_directories("${CUDA_TOOLKIT_ROOT_DIR}/lib64")

aux_source_directory(${MILVUS_ENGINE_SRC}/wrapper/knowhere knowhere_src)

set(helper
        utils.cpp)

set(knowhere_libs
        knowhere
        SPTAGLibStatic
        arrow
        jemalloc_pic
        faiss
        openblas
        lapack
        tbb
        cudart
        cublas
        )

add_executable(knowhere_test knowhere_test.cpp ${knowhere_src} ${helper})
target_link_libraries(knowhere_test ${knowhere_libs} ${unittest_libs})

install(TARGETS knowhere_test DESTINATION bin)