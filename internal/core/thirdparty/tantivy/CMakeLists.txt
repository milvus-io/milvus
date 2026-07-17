set(TANTIVY_FEATURES_LIST "" CACHE STRING "List of Cargo features to enable")
string(REPLACE ";" "," TANTIVY_FEATURES "${TANTIVY_FEATURES_LIST}")

set(CARGO_SEARCH_PATHS "$ENV{HOME}/.cargo/bin")
if (NOT "$ENV{CARGO_HOME}" STREQUAL "")
    list(PREPEND CARGO_SEARCH_PATHS "$ENV{CARGO_HOME}/bin")
endif ()
find_program(CARGO_EXECUTABLE
        NAMES cargo
        HINTS ${CARGO_SEARCH_PATHS}
        REQUIRED)

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CARGO_CMD ${CARGO_EXECUTABLE} +1.89 build --verbose)
    set(TARGET_DIR "debug")
else ()
    set(CARGO_CMD ${CARGO_EXECUTABLE} +1.89 build --release --verbose)
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

add_custom_command(
        OUTPUT ${LIB_FILE}
        COMMENT "Compiling tantivy binding"
        COMMAND ${CMAKE_COMMAND} -E env
                "TOKENIZER_PROTO=${TOKENIZER_PROTO}"
                "CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR}"
                ${CARGO_CMD}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/tantivy-binding
        VERBATIM)
add_custom_target(tantivy_binding_target DEPENDS ${LIB_FILE})

set(TANTIVY_HEADERS
        ${LIB_HEADER_FOLDER}/tantivy-binding.h
        ${CMAKE_CURRENT_SOURCE_DIR}/rust-array.h
        ${CMAKE_CURRENT_SOURCE_DIR}/rust-binding.h
        ${CMAKE_CURRENT_SOURCE_DIR}/rust-hashmap.h
        ${CMAKE_CURRENT_SOURCE_DIR}/tantivy-wrapper.h
        ${CMAKE_CURRENT_SOURCE_DIR}/time_recorder.h
        ${CMAKE_CURRENT_SOURCE_DIR}/token-stream.h
        ${CMAKE_CURRENT_SOURCE_DIR}/tokenizer.h)
set(INSTALLED_LIB_FILE "${TANTIVY_LIB_DIR}/${TANTIVY_NAME}")

add_custom_command(
        OUTPUT ${INSTALLED_LIB_FILE}
        COMMENT "Install tantivy target ${LIB_FILE} to ${TANTIVY_LIB_DIR}"
        COMMAND ${CMAKE_COMMAND} -E make_directory ${TANTIVY_INCLUDE_DIR}
        COMMAND ${CMAKE_COMMAND} -E make_directory ${TANTIVY_LIB_DIR}
        COMMAND ${CMAKE_COMMAND} -E copy_if_different ${TANTIVY_HEADERS} ${TANTIVY_INCLUDE_DIR}/
        COMMAND ${CMAKE_COMMAND} -E copy_if_different ${LIB_FILE} ${TANTIVY_LIB_DIR}/
        DEPENDS ${LIB_FILE} ${TANTIVY_HEADERS}
        VERBATIM)
add_custom_target(install_tantivy_target DEPENDS ${INSTALLED_LIB_FILE})

add_library(tantivy_binding STATIC IMPORTED)
add_dependencies(tantivy_binding
        install_tantivy_target
        )

set_target_properties(tantivy_binding
        PROPERTIES
        IMPORTED_GLOBAL TRUE
        IMPORTED_LOCATION "${TANTIVY_LIB_DIR}/${TANTIVY_NAME}"
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
