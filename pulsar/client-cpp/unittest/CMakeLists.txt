include_directories(${CMAKE_SOURCE_DIR}/src)
include_directories(.)
enable_testing()
set(unittest_srcs
        unittest_entry.cpp
        consumer_test.cpp producer_test.cpp)

add_executable(test ${unittest_srcs})

target_link_libraries(test
        client_cpp
        pulsar
        gtest)

install(TARGETS test DESTINATION unittest)