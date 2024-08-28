if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CARGO_CMD cargo build)
    set(TARGET_DIR "debug")
else ()
    set(CARGO_CMD cargo build --release)
    set(TARGET_DIR "release")
endif ()

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

add_custom_command(OUTPUT ls_cargo
        COMMENT "ls cargo"
        COMMAND ls ${HOME_VAR}/.cargo/bin/
        )
add_custom_target(ls_cargo_target DEPENDS ls_cargo)

add_custom_command(OUTPUT compile_tantivy
        COMMENT "Compiling tantivy binding"
        COMMAND CARGO_TARGET_DIR=${CMAKE_CURRENT_BINARY_DIR} ${CARGO_CMD}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/tantivy-binding)
add_custom_target(tantivy_binding_target DEPENDS compile_tantivy ls_cargo_target)

set(INSTALL_COMMAND
        cp ${LIB_HEADER_FOLDER}/tantivy-binding.h ${TANTIVY_INCLUDE_DIR}/ &&
        cp ${CMAKE_CURRENT_SOURCE_DIR}/*.h ${TANTIVY_INCLUDE_DIR}/ &&
        cp ${LIB_FILE} ${TANTIVY_LIB_DIR}/)
add_custom_command(OUTPUT install_tantivy
        COMMENT "Install tantivy target ${LIB_FILE} to ${TANTIVY_LIB_DIR}"
        COMMAND ${INSTALL_COMMAND}
        )
add_custom_target(install_tantivy_target DEPENDS install_tantivy tantivy_binding_target)

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

set( TANTIVY_INCLUDE_DIR ${LIB_HEADER_FOLDER};${CMAKE_CURRENT_SOURCE_DIR} CACHE INTERNAL "Path to tantivy include directory" )
