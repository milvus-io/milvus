include_directories("${CUDA_TOOLKIT_ROOT_DIR}/include")
link_directories("${CUDA_TOOLKIT_ROOT_DIR}/lib64")

set(knowhere_src
        ${MILVUS_ENGINE_SRC}/wrapper/knowhere/data_transfer.cpp
        ${MILVUS_ENGINE_SRC}/wrapper/knowhere/vec_impl.cpp
        ${MILVUS_ENGINE_SRC}/wrapper/knowhere/vec_index.cpp)

set(helper
        utils.cpp)

set(knowhere_libs
        knowhere
        cudart
        cublas
        )

add_executable(knowhere_test knowhere_test.cpp ${knowhere_src} ${helper})
target_link_libraries(knowhere_test ${knowhere_libs} ${unittest_libs})

install(TARGETS knowhere_test DESTINATION unittest)