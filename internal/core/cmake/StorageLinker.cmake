# Optional, target-local linker selection. Do not change compiler or global ld.
# Direct CMake: -DMILVUS_STORAGE_USE_LLD=ON
# Make wrapper: CMAKE_EXTRA_ARGS=-DMILVUS_STORAGE_USE_LLD=ON make jobs=14 install USE_ASAN=ON
# Install ld.lld separately; the default OFF path needs no LLVM tools.
option(MILVUS_STORAGE_USE_LLD "Use LLVM lld to link milvus-storage on Linux" OFF)

function(milvus_configure_storage_linker target)
    include(CheckLinkerFlag)
    if(MILVUS_STORAGE_USE_LLD)
        if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux" OR
           NOT CMAKE_CXX_COMPILER_ID MATCHES "^(GNU|Clang)$")
            message(FATAL_ERROR "MILVUS_STORAGE_USE_LLD requires Linux with GCC or Clang")
        endif()
        find_program(MILVUS_STORAGE_LLD_PROGRAM NAMES ld.lld)
        if(NOT MILVUS_STORAGE_LLD_PROGRAM)
            message(FATAL_ERROR "MILVUS_STORAGE_USE_LLD=ON requires ld.lld on PATH")
        endif()
        execute_process(COMMAND "${MILVUS_STORAGE_LLD_PROGRAM}" --version
                        RESULT_VARIABLE lld_status OUTPUT_VARIABLE lld_version)
        if(NOT lld_status EQUAL 0 OR NOT lld_version MATCHES "LLD")
            message(FATAL_ERROR "Selected ld.lld does not identify itself as LLVM LLD")
        endif()
        get_filename_component(lld_directory "${MILVUS_STORAGE_LLD_PROGRAM}" DIRECTORY)
        set(lld_options "-B${lld_directory}/;-fuse-ld=lld")
        # Avoid stale success if the selected tool changes in a reused build tree.
        unset(MILVUS_STORAGE_LLD_WORKS CACHE)
        check_linker_flag(CXX "${lld_options}" MILVUS_STORAGE_LLD_WORKS)
        if(NOT MILVUS_STORAGE_LLD_WORKS)
            message(FATAL_ERROR "The C++ compiler cannot link with the selected LLVM lld")
        endif()
        target_link_options(${target} PRIVATE ${lld_options})
        string(STRIP "${lld_version}" lld_version)
        message(STATUS "milvus-storage linker: ${MILVUS_STORAGE_LLD_PROGRAM} (${lld_version})")
    elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND
           CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64)$")
        # Preserve GNU ld's large Rust archive / ASAN veneer-range workaround.
        check_linker_flag(CXX "LINKER:--stub-group-size=67108864"
                         MILVUS_STORAGE_HAS_STUB_GROUP_SIZE)
        if(MILVUS_STORAGE_HAS_STUB_GROUP_SIZE)
            target_link_options(${target} PRIVATE "LINKER:--stub-group-size=67108864")
        endif()
    endif()
endfunction()
