add_library(knowhere SHARED src/placeholder.cpp)
target_include_directories(knowhere PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
set(KNOWHERE_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include CACHE INTERNAL "Path to knowhere include directory")

add_executable(knowhere_rs_compile_surface_smoke tests/compile_surface_smoke.cpp)
target_link_libraries(knowhere_rs_compile_surface_smoke PRIVATE knowhere)
