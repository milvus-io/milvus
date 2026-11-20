# Licensed under the Apache License, Version 2.0.

# Check the selected C++ compiler, including cached and custom toolchains.
# CUDA builds retain their existing host compiler requirements.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND
   CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND NOT WITH_GPU AND
   CMAKE_CXX_COMPILER_VERSION VERSION_LESS "15.3")
    string(CONCAT _gcc_message
        "Linux CPU builds require GCC 15.3 or newer. Selected ${CMAKE_CXX_COMPILER} (${CMAKE_CXX_COMPILER_VERSION}). "
        "Use an updated CPU builder or upgrade your compiler and regenerate the Conan profile in a clean build directory. "
        "Set MILVUS_ALLOW_UNSUPPORTED_GCC=1 to force an unsupported build. "
        "Build success and runtime correctness are not guaranteed.")
    if("$ENV{MILVUS_ALLOW_UNSUPPORTED_GCC}" STREQUAL "1")
        message(WARNING "${_gcc_message} Continuing because MILVUS_ALLOW_UNSUPPORTED_GCC=1.")
    else()
        message(FATAL_ERROR "${_gcc_message}")
    endif()
    unset(_gcc_message)
endif()
