set(TANTIVY_FEATURES_LIST "" CACHE STRING "List of Cargo features to enable")
string(REPLACE ";" "," TANTIVY_FEATURES "${TANTIVY_FEATURES_LIST}")

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CARGO_CMD cargo +1.89 build --verbose)
    set(TARGET_DIR "debug")
else ()
    set(CARGO_CMD cargo +1.89 build --release --verbose)
    set(TARGET_DIR "release")
endif ()

if (TANTIVY_FEATURES)
    set(CARGO_CMD ${CARGO_CMD} --features ${TANTIVY_FEATURES})
endif ()
message("Cargo command: ${CARGO_CMD}")

set(TOKENIZER_PROTO ${CMAKE_BINARY_DIR}/thirdparty/milvus-proto/proto/tokenizer.proto)

set(TANTIVY_LIB_DIR "${CMAKE_INSTALL_PREFIX}/lib")
set(TANTIVY_INCLUDE_DIR "${CMAKE_INSTALL_PREFIX}/include")
set(TANTIVY_NAME "libtantivy_binding${CMAKE_STATIC_LIBRARY_SUFFIX}")

set(LIB_FILE "${CMAKE_CURRENT_BINARY_DIR}/${TARGET_DIR}/${TANTIVY_NAME}")
set(LIB_HEADER_FOLDER "${CMAKE_CURRENT_SOURCE_DIR}/tantivy-binding/include")

# In fact, cargo was already installed on our builder environment.
# Below settings are used to suit for first local development.
set(HOME_VAR $ENV{HOME})
set(PATH_VAR $ENV{PATH})
set(ENV{PATH} ${HOME_VAR}/.cargo/bin:${PATH_VAR})
message($ENV{PATH})

# Installed library path (final destination)
set(INSTALLED_LIB_FILE "${TANTIVY_LIB_DIR}/${TANTIVY_NAME}")

# Collect Rust source files for dependency tracking
file(GLOB_RECURSE TANTIVY_RUST_SOURCES
        "${CMAKE_CURRENT_SOURCE_DIR}/tantivy-binding/src/*.rs"
        "${CMAKE_CURRENT_SOURCE_DIR}/tantivy-binding/Cargo.toml"
        "${CMAKE_CURRENT_SOURCE_DIR}/tantivy-binding/Cargo.lock"
)

# Step 1: Compile tantivy binding using cargo
# OUTPUT is the actual library file so ninja can track the dependency
add_custom_command(
        OUTPUT ${LIB_FILE}
        COMMENT "Compiling tantivy binding"
        COMMAND ${CMAKE_COMMAND} -E env
                "PATH=${HOME_VAR}/.cargo/bin:${PATH_VAR}"
                "TOKENIZER_PROTO=${TOKENIZER_PROTO}"
                "CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR}"
                ${CARGO_CMD}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/tantivy-binding
        DEPENDS ${TANTIVY_RUST_SOURCES}
)

# Step 2: Install the compiled library and headers
# OUTPUT is the installed library file so ninja knows when to rebuild
add_custom_command(
        OUTPUT ${INSTALLED_LIB_FILE}
        COMMENT "Installing tantivy: ${LIB_FILE} -> ${TANTIVY_LIB_DIR}"
        COMMAND ${CMAKE_COMMAND} -E make_directory ${TANTIVY_LIB_DIR}
        COMMAND ${CMAKE_COMMAND} -E make_directory ${TANTIVY_INCLUDE_DIR}
        COMMAND ${CMAKE_COMMAND} -E copy ${LIB_FILE} ${TANTIVY_LIB_DIR}/
        COMMAND ${CMAKE_COMMAND} -E copy ${LIB_HEADER_FOLDER}/tantivy-binding.h ${TANTIVY_INCLUDE_DIR}/
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/tantivy-wrapper.h ${TANTIVY_INCLUDE_DIR}/
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/rust-binding.h ${TANTIVY_INCLUDE_DIR}/
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/rust-array.h ${TANTIVY_INCLUDE_DIR}/
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/rust-hashmap.h ${TANTIVY_INCLUDE_DIR}/
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/tokenizer.h ${TANTIVY_INCLUDE_DIR}/
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/token-stream.h ${TANTIVY_INCLUDE_DIR}/
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/phrase_match.h ${TANTIVY_INCLUDE_DIR}/
        COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/time_recorder.h ${TANTIVY_INCLUDE_DIR}/
        DEPENDS ${LIB_FILE}
)

# Create a custom target that other targets can depend on
add_custom_target(install_tantivy_target DEPENDS ${INSTALLED_LIB_FILE})

# Create the imported library target
add_library(tantivy_binding STATIC IMPORTED GLOBAL)
add_dependencies(tantivy_binding install_tantivy_target)

set_target_properties(tantivy_binding
        PROPERTIES
        IMPORTED_LOCATION "${INSTALLED_LIB_FILE}"
        INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_INSTALL_PREFIX}/include")

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
        add_compile_options(-fno-stack-protector -fno-omit-frame-pointer -fno-var-tracking -fsanitize=address)
        add_link_options(-fno-stack-protector -fno-omit-frame-pointer -fno-var-tracking -fsanitize=address)
endif()

# TODO: move these below tests to ut.

option(BUILD_TANTIVY_WITH_UT "compile tantivy with ut" OFF)

if (BUILD_TANTIVY_WITH_UT)
    message(STATUS "compile tantivy with ut")

    add_executable(test_tantivy test.cpp)
    target_link_libraries(test_tantivy
            tantivy_binding
            boost_filesystem
            dl
            )

    add_executable(bench_tantivy bench.cpp)
    target_link_libraries(bench_tantivy
            tantivy_binding
            boost_filesystem
            dl
            )

    add_executable(ffi_demo ffi_demo.cpp)
    target_link_libraries(ffi_demo
            tantivy_binding
            dl
            )

    add_executable(tokenizer_demo tokenizer_demo.cpp)
    target_link_libraries(tokenizer_demo
            tantivy_binding
            dl
            )

    add_executable(text_demo text_demo.cpp)
    target_link_libraries(text_demo
            tantivy_binding
            dl
        )

    add_executable(jieba_demo jieba_demo.cpp)
    target_link_libraries(jieba_demo
            tantivy_binding
            dl
        )
else ()
endif ()
        
set( TANTIVY_INCLUDE_DIR ${LIB_HEADER_FOLDER};${CMAKE_CURRENT_SOURCE_DIR} CACHE INTERNAL "Path to tantivy include directory" )
