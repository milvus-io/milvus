// Copyright 2013 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>
#include <jsoncons/detail/parse_number.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <cwchar>
#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("test_string_to_double")
{
    std::cout << "sizeof(json): " << sizeof(json) << std::endl; 

    const char* s1 = "0.0";
    json j1 = json::parse(s1);
    double expected1 = 0.0;
    CHECK( j1.as<double>() == expected1);

    const char* s2 = "0.123456789";
    json j2 = json::parse(s2);
    double expected2 = 0.123456789;
    CHECK( j2.as<double>() == expected2);

    const char* s3 = "123456789.123456789";
    json j3 = json::parse(s3);
    char* end3 = nullptr;
    double expected3 = strtod(s3,&end3);
    CHECK( j3.as<double>() == expected3);
}

TEST_CASE("test_exponent")
{
    jsoncons::detail::string_to_double reader;
    const char* begin = "1.15507e-173";
    char* endptr = nullptr;
    const double value1 = 1.15507e-173;
    const double value2 = strtod(begin, &endptr );
    const double value3 = reader(begin,endptr-begin);

    CHECK(value1 == value2);
    CHECK(value2 == value3);

    const char* s1 = "1.15507e+173";
    json j1 = json::parse(s1);
    double expected1 = 1.15507e+173;
    CHECK( j1.as<double>() == expected1);

}

