// Copyright 2013 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>
#include <catch/catch.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <map>

using namespace jsoncons;

void check_swap(const json& j1, const json& j2)
{
    json j3 = j1;
    json j4 = j2;

    j3.swap(j4);
    CHECK(j1 == j4);
    CHECK(j2 == j3);
}

TEST_CASE("test_swap")
{
    json j1 = json::null();
    json j2 = false;
    json j3 = -2000;
    json j4 = 2000U;
    json j5 = 2000.1234;
    json j6 = "Small";
    json j7 = "String too large for small string";
    json j8 = json::parse("[1,2,3,4]");
    json j9;
    json j10 = json::object();
    j10["Name"] = "John Smith";

    check_swap(j1,j1);
    check_swap(j1,j2);
    check_swap(j1,j3);
    check_swap(j1,j4);
    check_swap(j1,j5);
    check_swap(j1,j6);
    check_swap(j1,j7);
    check_swap(j1,j8);
    check_swap(j1,j9);
    check_swap(j1,j10);

    check_swap(j2,j1);
    check_swap(j2,j2);
    check_swap(j2,j3);
    check_swap(j2,j4);
    check_swap(j2,j5);
    check_swap(j2,j6);
    check_swap(j2,j7);
    check_swap(j2,j8);
    check_swap(j2,j9);
    check_swap(j2,j10);

    check_swap(j3,j1);
    check_swap(j3,j2);
    check_swap(j3,j3);
    check_swap(j3,j4);
    check_swap(j3,j5);
    check_swap(j3,j6);
    check_swap(j3,j7);
    check_swap(j3,j8);
    check_swap(j3,j9);
    check_swap(j3,j10);

    check_swap(j4,j1);
    check_swap(j4,j2);
    check_swap(j4,j3);
    check_swap(j4,j4);
    check_swap(j4,j5);
    check_swap(j4,j6);
    check_swap(j4,j7);
    check_swap(j4,j8);
    check_swap(j4,j9);
    check_swap(j4,j10);

    check_swap(j5,j1);
    check_swap(j5,j2);
    check_swap(j5,j3);
    check_swap(j5,j4);
    check_swap(j5,j5);
    check_swap(j5,j6);
    check_swap(j5,j7);
    check_swap(j5,j8);
    check_swap(j5,j9);
    check_swap(j5,j10);

    check_swap(j6,j1);
    check_swap(j6,j2);
    check_swap(j6,j3);
    check_swap(j6,j4);
    check_swap(j6,j5);
    check_swap(j6,j6);
    check_swap(j6,j7);
    check_swap(j6,j8);
    check_swap(j6,j9);
    check_swap(j6,j10);

    check_swap(j7,j1);
    check_swap(j7,j2);
    check_swap(j7,j3);
    check_swap(j7,j4);
    check_swap(j7,j5);
    check_swap(j7,j6);
    check_swap(j7,j7);
    check_swap(j7,j8);
    check_swap(j7,j9);
    check_swap(j7,j10);

    check_swap(j8,j1);
    check_swap(j8,j2);
    check_swap(j8,j3);
    check_swap(j8,j4);
    check_swap(j8,j5);
    check_swap(j8,j6);
    check_swap(j8,j7);
    check_swap(j8,j8);
    check_swap(j8,j9);
    check_swap(j8,j10);

    check_swap(j9,j1);
    check_swap(j9,j2);
    check_swap(j9,j3);
    check_swap(j9,j4);
    check_swap(j9,j5);
    check_swap(j9,j6);
    check_swap(j9,j7);
    check_swap(j9,j8);
    check_swap(j9,j9);
    check_swap(j9,j10);

    check_swap(j10,j1);
    check_swap(j10,j2);
    check_swap(j10,j3);
    check_swap(j10,j4);
    check_swap(j10,j5);
    check_swap(j10,j6);
    check_swap(j10,j7);
    check_swap(j10,j8);
    check_swap(j10,j9);
    check_swap(j10,j10);
}

