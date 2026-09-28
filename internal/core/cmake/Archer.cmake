# Licensed under the Apache License, Version 2.0.
include_guard(GLOBAL)

set(MILVUS_LLVM_ROOT "$ENV{MILVUS_LLVM_ROOT}" CACHE PATH "LLVM 20 toolchain with libomp and Archer")
if(NOT MILVUS_LLVM_ROOT)
    set(MILVUS_LLVM_ROOT "/usr/lib/llvm-20" CACHE PATH "LLVM toolchain" FORCE)
endif()
if(NOT CMAKE_C_COMPILER_ID STREQUAL "Clang" OR
   NOT CMAKE_CXX_COMPILER_ID STREQUAL "Clang" OR
   CMAKE_C_COMPILER_VERSION VERSION_LESS 20 OR
   NOT CMAKE_C_COMPILER_VERSION VERSION_LESS 21 OR
   NOT CMAKE_C_COMPILER_VERSION VERSION_EQUAL CMAKE_CXX_COMPILER_VERSION)
    message(FATAL_ERROR "USE_TSAN requires matching LLVM 20 C/C++ compilers and Archer")
endif()

execute_process(COMMAND "${CMAKE_C_COMPILER}"
    "-print-file-name=libclang_rt.tsan-${CMAKE_SYSTEM_PROCESSOR}.so"
    OUTPUT_VARIABLE MILVUS_TSAN_RUNTIME OUTPUT_STRIP_TRAILING_WHITESPACE
    RESULT_VARIABLE tsan_runtime_result)
if(NOT tsan_runtime_result EQUAL 0)
    message(FATAL_ERROR "Cannot locate the LLVM TSan runtime")
endif()
set(MILVUS_ARCHER_LIBRARY "${MILVUS_LLVM_ROOT}/lib/libarcher.so")
get_filename_component(MILVUS_OPENMP_LIBRARY "${MILVUS_LLVM_ROOT}/lib/libomp.so" REALPATH)
foreach(library MILVUS_TSAN_RUNTIME MILVUS_ARCHER_LIBRARY MILVUS_OPENMP_LIBRARY)
    if(NOT EXISTS "${${library}}")
        message(FATAL_ERROR "Missing ${library}: ${${library}}")
    endif()
endforeach()
get_filename_component(MILVUS_TSAN_RUNTIME_DIR "${MILVUS_TSAN_RUNTIME}" DIRECTORY)

# Use one LLVM OpenMP runtime throughout Core and its source dependencies.
foreach(lang C CXX)
    set(OpenMP_${lang}_FLAGS "-fopenmp=libomp" CACHE STRING "LLVM OpenMP flags" FORCE)
    set(OpenMP_${lang}_LIB_NAMES "omp" CACHE STRING "LLVM OpenMP libraries" FORCE)
endforeach()
set(OpenMP_omp_LIBRARY "${MILVUS_OPENMP_LIBRARY}" CACHE FILEPATH "LLVM OpenMP runtime" FORCE)
find_package(OpenMP REQUIRED COMPONENTS C CXX)
link_directories("${MILVUS_LLVM_ROOT}/lib" "${MILVUS_TSAN_RUNTIME_DIR}")
add_link_options("-Wl,-rpath,${MILVUS_TSAN_RUNTIME_DIR}")

# libomp loads Archer using OMPT, so ldd alone cannot discover this dependency.
install(FILES "${MILVUS_ARCHER_LIBRARY}" "${MILVUS_TSAN_RUNTIME}" DESTINATION lib)
install(FILES "${MILVUS_OPENMP_LIBRARY}" DESTINATION lib RENAME libomp.so.5)
install(FILES "${MILVUS_OPENMP_LIBRARY}" DESTINATION lib RENAME libomp.so)
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/milvus-archer" "llvm-20\n")
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/milvus-archer" DESTINATION lib)
