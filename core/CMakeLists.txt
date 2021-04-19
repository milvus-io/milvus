project(sulvim_core)
cmake_minimum_required(VERSION 3.16)
set( CMAKE_CXX_STANDARD 17 )
set( CMAKE_CXX_STANDARD_REQUIRED on )


include_directories(src)
add_subdirectory(src)
add_subdirectory(unittest)
