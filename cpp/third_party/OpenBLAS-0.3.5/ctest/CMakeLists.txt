include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_BINARY_DIR})

enable_language(Fortran)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DADD${BU} -DCBLAS")

FILE(WRITE ${CMAKE_CURRENT_BINARY_DIR}/test_cblas_helper.sh
"$1 < $2\n"
)

foreach(float_type ${FLOAT_TYPES})
  string(SUBSTRING ${float_type} 0 1 float_char_upper)
  string(TOLOWER ${float_char_upper} float_char)
  #level1
  add_executable(x${float_char}cblat1
    c_${float_char}blat1.f
    c_${float_char}blas1.c)
  target_link_libraries(x${float_char}cblat1 ${OpenBLAS_LIBNAME})
  add_test(NAME "x${float_char}cblat1"
    COMMAND "${CMAKE_CURRENT_BINARY_DIR}/x${float_char}cblat1")

  #level2
  add_executable(x${float_char}cblat2
    c_${float_char}blat2.f
    c_${float_char}blas2.c
    c_${float_char}2chke.c
    auxiliary.c
    c_xerbla.c
    constant.c)
  target_link_libraries(x${float_char}cblat2 ${OpenBLAS_LIBNAME})
  add_test(NAME "x${float_char}cblat2"
    COMMAND sh "${CMAKE_CURRENT_BINARY_DIR}/test_cblas_helper.sh" "${CMAKE_CURRENT_BINARY_DIR}/x${float_char}cblat2" "${PROJECT_SOURCE_DIR}/ctest/${float_char}in2")

  #level3
  add_executable(x${float_char}cblat3
    c_${float_char}blat3.f
    c_${float_char}blas3.c
    c_${float_char}3chke.c
    auxiliary.c
    c_xerbla.c
    constant.c)
  target_link_libraries(x${float_char}cblat3 ${OpenBLAS_LIBNAME})
  add_test(NAME "x${float_char}cblat3"
    COMMAND sh "${CMAKE_CURRENT_BINARY_DIR}/test_cblas_helper.sh" "${CMAKE_CURRENT_BINARY_DIR}/x${float_char}cblat3" "${PROJECT_SOURCE_DIR}/ctest/${float_char}in3")

endforeach()
