set(BITSET_SRCS
    detail/platform/dynamic.cpp
)

if (${CMAKE_SYSTEM_PROCESSOR} STREQUAL "x86_64")
    list(APPEND BITSET_SRCS
        detail/platform/x86/avx2-inst.cpp
        detail/platform/x86/avx512-inst.cpp
        detail/platform/x86/instruction_set.cpp
    )

    set_source_files_properties(detail/platform/x86/avx512-inst.cpp PROPERTIES COMPILE_FLAGS "-mavx512f -mavx512bw -mavx512vl -mavx512dq")
    set_source_files_properties(detail/platform/x86/avx2-inst.cpp PROPERTIES COMPILE_FLAGS "-mavx2 -mavx -mfma")

    # set_source_files_properties(detail/platform/dynamic.cpp PROPERTIES COMPILE_FLAGS "-mavx512f -mavx512bw -mavx512vl -mavx512dq")
    # set_source_files_properties(detail/platform/dynamic.cpp PROPERTIES COMPILE_FLAGS "-mavx2 -mavx -mfma")
elseif (${CMAKE_SYSTEM_PROCESSOR} MATCHES "arm*")
    list(APPEND BITSET_SRCS
        detail/platform/arm/neon-inst.cpp
        detail/platform/arm/sve-inst.cpp
    )

    # targeting AWS graviton, 
    #  https://github.com/aws/aws-graviton-getting-started/blob/main/c-c%2B%2B.md

    # let dynamic.cpp know that SVE is available
    # comment it out for now
    # set_source_files_properties(detail/platform/dynamic.cpp PROPERTIES COMPILE_FLAGS "-mcpu=neoverse-v1")
    
    set_source_files_properties(detail/platform/arm/sve-inst.cpp PROPERTIES COMPILE_FLAGS "-mcpu=neoverse-v1")
endif()

add_library(milvus_bitset ${BITSET_SRCS})
