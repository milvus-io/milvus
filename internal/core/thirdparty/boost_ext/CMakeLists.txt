find_package(Boost REQUIRED)
add_library(boost_bitset_ext dynamic_bitset_ext.cpp)
target_link_libraries(boost_bitset_ext PUBLIC Boost::boost)
install(TARGETS boost_bitset_ext DESTINATION "${CMAKE_INSTALL_LIBDIR}")
