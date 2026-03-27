add_library(knowhere SHARED
    src/placeholder.cpp
)
target_include_directories(knowhere PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_compile_features(knowhere PUBLIC cxx_std_17)
find_path(KNOWHERE_RS_SHIM_NLOHMANN_INCLUDE_DIR nlohmann/json.hpp)
if (KNOWHERE_RS_SHIM_NLOHMANN_INCLUDE_DIR)
    target_include_directories(knowhere PUBLIC ${KNOWHERE_RS_SHIM_NLOHMANN_INCLUDE_DIR})
endif ()
set(KNOWHERE_RS_ROOT "" CACHE PATH "Path to the knowhere-rs repository root")
find_file(KNOWHERE_RS_LIBRARY
    NAMES libknowhere_rs.dylib libknowhere_rs.so libknowhere_rs.a
    PATHS
        ${KNOWHERE_RS_ROOT}/target/debug
        ${KNOWHERE_RS_ROOT}/target/release
        ENV KNOWHERE_RS_LIB_DIR
)
if (KNOWHERE_RS_LIBRARY)
    target_sources(knowhere PRIVATE src/hnsw_rust_node.cpp)
    target_link_libraries(knowhere PRIVATE ${KNOWHERE_RS_LIBRARY})
endif ()
set(KNOWHERE_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include CACHE INTERNAL "Path to knowhere include directory")

add_executable(knowhere_rs_compile_surface_smoke tests/compile_surface_smoke.cpp)
target_compile_features(knowhere_rs_compile_surface_smoke PRIVATE cxx_std_17)
target_link_libraries(knowhere_rs_compile_surface_smoke PRIVATE knowhere)

add_executable(knowhere_rs_hnsw_roundtrip_smoke tests/hnsw_roundtrip_smoke.cpp)
target_compile_features(knowhere_rs_hnsw_roundtrip_smoke PRIVATE cxx_std_17)
target_link_libraries(knowhere_rs_hnsw_roundtrip_smoke PRIVATE knowhere)
