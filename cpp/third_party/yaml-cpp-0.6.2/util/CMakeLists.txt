add_sources(parse.cpp)
add_executable(parse parse.cpp)
target_link_libraries(parse yaml-cpp)
set_target_properties(parse PROPERTIES COMPILE_FLAGS "-std=c++11")

add_sources(sandbox.cpp)
add_executable(sandbox sandbox.cpp)
target_link_libraries(sandbox yaml-cpp)
set_target_properties(sandbox PROPERTIES COMPILE_FLAGS "-std=c++11")

add_sources(read.cpp)
add_executable(read read.cpp)
target_link_libraries(read yaml-cpp)
set_target_properties(read PROPERTIES COMPILE_FLAGS "-std=c++11")
