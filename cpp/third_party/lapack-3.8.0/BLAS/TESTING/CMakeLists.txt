macro(add_blas_test name src)
  get_filename_component(baseNAME ${src} NAME_WE)
  set(TEST_INPUT "${CMAKE_CURRENT_SOURCE_DIR}/${baseNAME}.in")
  add_executable(${name} ${src})
  target_link_libraries(${name} blas)
  if(EXISTS "${TEST_INPUT}")
    add_test(NAME BLAS-${name} COMMAND "${CMAKE_COMMAND}"
      -DTEST=$<TARGET_FILE:${name}>
      -DINPUT=${TEST_INPUT}
      -DINTDIR=${CMAKE_CFG_INTDIR}
      -P "${LAPACK_SOURCE_DIR}/TESTING/runtest.cmake")
  else()
    add_test(NAME BLAS-${name} COMMAND "${CMAKE_COMMAND}"
      -DTEST=$<TARGET_FILE:${name}>
      -DINTDIR=${CMAKE_CFG_INTDIR}
      -P "${LAPACK_SOURCE_DIR}/TESTING/runtest.cmake")
  endif()
endmacro()

if(BUILD_SINGLE)
  add_blas_test(xblat1s sblat1.f)
  add_blas_test(xblat2s sblat2.f)
  add_blas_test(xblat3s sblat3.f)
endif()

if(BUILD_DOUBLE)
  add_blas_test(xblat1d dblat1.f)
  add_blas_test(xblat2d dblat2.f)
  add_blas_test(xblat3d dblat3.f)
endif()

if(BUILD_COMPLEX)
  add_blas_test(xblat1c cblat1.f)
  add_blas_test(xblat2c cblat2.f)
  add_blas_test(xblat3c cblat3.f)
endif()

if(BUILD_COMPLEX16)
  add_blas_test(xblat1z zblat1.f)
  add_blas_test(xblat2z zblat2.f)
  add_blas_test(xblat3z zblat3.f)
endif()
