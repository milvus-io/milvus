add_executable(xexample1_CBLAS cblas_example1.c)
add_executable(xexample2_CBLAS cblas_example2.c)

target_link_libraries(xexample1_CBLAS cblas)
target_link_libraries(xexample2_CBLAS cblas ${BLAS_LIBRARIES})

add_test(example1_CBLAS ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/xexample1_CBLAS)
add_test(example2_CBLAS ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/xexample2_CBLAS)
