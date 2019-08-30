// Copyright 2013 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json_exception.hpp>
#include <jsoncons/detail/print_number.hpp>
#include <catch/catch.hpp>
#include <iostream>
#include <cstdio>

using namespace jsoncons;

static void check_safe_dtoa(double x, const std::vector<std::string>& expected)
{
    std::string s;
    bool result = jsoncons::detail::dtoa(x, '.', s, std::false_type());
    if (!result)
    {
        std::cout << "safe dtoa failed " << s << "\n";
    }
    REQUIRE(result);

    bool accept = false;
    for (size_t i = 0; !accept && i < expected.size(); ++i)
    {
        accept = s == expected[i];
    }
    if (!accept)
    {
        std::cout << "safe dtoa does not match expected " << x << " " << s << "\n";
    }

    CHECK(accept);
}

static void check_dtoa(double x, const std::vector<std::string>& expected)
{
    std::string s;
    bool result = jsoncons::detail::dtoa(x, '.', s);
    if (!result)
    {
        std::cout << "dtoa failed " << s << "\n";
    }
    REQUIRE(result);

    bool accept = false;
    for (size_t i = 0; !accept && i < expected.size(); ++i)
    {
        accept = s == expected[i];
    }
    if (!accept)
    {
        std::cout << "dtoa does not match expected " << x << " " << s << "\n";
    }
    CHECK(accept);

    check_safe_dtoa(x,expected);
}

TEST_CASE("test grisu3")
{
    check_dtoa(1.0e100, {"1e+100"});
    check_dtoa(1.0e-100, {"1e-100"});
    check_dtoa(0.123456789e-100, {"1.23456789e-101"});
    check_dtoa(0.123456789e100, {"1.23456789e+99"});

    check_dtoa(1234563, {"1234563.0"});

    check_dtoa(0.0000001234563, {"1.234563e-07"});

    check_dtoa(-1.0e+100, {"-1e+100"});

    check_dtoa(-1.0e-100, {"-1e-100"});

    check_dtoa(0, {"0.0"});
    check_dtoa(-0, {"0.0"});
    check_dtoa(1, {"1.0"});
    check_dtoa(0.1, {"0.1"});

    check_dtoa(1.1, {"1.1"});

    check_dtoa(-1, {"-1.0"});
    check_dtoa(10, {"10.0"});
    check_dtoa(-10, {"-10.0"});
    check_dtoa(-11, {"-11.0"}); 

    check_dtoa(12.272727012634277, {"12.272727012634277"}); 

    check_dtoa(4094.1111111111113, {"4094.1111111111113"}); 

    check_dtoa(0.119942, {"0.119942"}); 

    check_dtoa(-36.973846435546875, {"-36.973846435546875"}); 

    check_dtoa(42.229999999999997, {"42.23"}); 
    check_dtoa(9.0099999999999998, {"9.01"}); 
    check_dtoa(13.449999999999999, {"13.45"}); 

    check_dtoa(0.000071, {"7.1e-05"}); 
}

