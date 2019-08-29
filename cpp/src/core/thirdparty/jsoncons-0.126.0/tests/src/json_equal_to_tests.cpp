// Copyright 2013 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>
#include <catch/catch.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>

using namespace jsoncons;

TEST_CASE("test_object_equals_basic")
{
    json o1;
    o1["a"] = 1;
    o1["b"] = 2;
    o1["c"] = 3;

    json o2;
    o2["c"] = 3;
    o2["a"] = 1;
    o2["b"] = 2;

    CHECK(o1 == o2);
    CHECK(o2 == o1);
    CHECK_FALSE((o1 != o2));
    CHECK_FALSE((o2 != o1));
}

TEST_CASE("test_object_equals_diff_vals")
{
    json o1;
    o1["a"] = 1;
    o1["b"] = 2;
    o1["c"] = 3;

    json o2;
    o2["a"] = 1;
    o2["b"] = 4;
    o2["c"] = 3;

    CHECK_FALSE((o1 == o2));
    CHECK_FALSE((o2 == o1));
    CHECK(o1 != o2);
    CHECK(o2 != o1);
}

TEST_CASE("test_object_equals_diff_el_names")
{
    json o1;
    o1["a"] = 1;
    o1["b"] = 2;
    o1["c"] = 3;

    json o2;
    o2["d"] = 1;
    o2["e"] = 2;
    o2["f"] = 3;

    CHECK_FALSE((o1 == o2));
    CHECK_FALSE((o2 == o1));
    CHECK(o1 != o2);
    CHECK(o2 != o1);
}

TEST_CASE("test_object_equals_diff_sizes")
{
    json o1;
    o1["a"] = 1;
    o1["b"] = 2;
    o1["c"] = 3;

    json o2;
    o2["a"] = 1;
    o2["b"] = 2;

    CHECK_FALSE((o1 == o2));
    CHECK_FALSE((o2 == o1));
    CHECK(o1 != o2);
    CHECK(o2 != o1);
}

TEST_CASE("test_object_equals_subtle_offsets")
{
    json o1;
    o1["a"] = 1;
    o1["b"] = 1;

    json o2;
    o2["b"] = 1;
    o2["c"] = 1;

    CHECK_FALSE((o1 == o2));
    CHECK_FALSE((o2 == o1));
    CHECK(o1 != o2);
    CHECK(o2 != o1);
}

TEST_CASE("test_object_equals_empty_objects")
{
    json def_constructed_1;
    json def_constructed_2;
    json parsed_1 = json::parse("{}");
    json parsed_2 = json::parse("{}");
    json type_constructed_1 = json::object();
    json type_constructed_2 = json::object();

    CHECK(def_constructed_1 == def_constructed_1);
    CHECK(parsed_1 == parsed_2);
    CHECK(type_constructed_1 == type_constructed_2);

    CHECK(def_constructed_1 == parsed_1);
    CHECK(def_constructed_1 == type_constructed_1);
    CHECK(parsed_1 == type_constructed_1);
}

TEST_CASE("test_object_equals_empty_arrays")
{
    json parsed_1 = json::parse("[]");
    json parsed_2 = json::parse("[]");
    json type_constructed_1 = json::array();
    json type_constructed_2 = json::array();

    CHECK(parsed_1 == parsed_2);
    CHECK(type_constructed_1 == type_constructed_2);

    CHECK(parsed_1 == type_constructed_1);
}

TEST_CASE("test_empty_object_equal")
{
    CHECK(json() == json(json::object()));
    CHECK(json(json::object()) == json());
}

TEST_CASE("test_string_not_equals_empty_object")
{
    json o1("42");
    json o2;

    CHECK(o1 != o2);
    CHECK(o2 != o1);
}

TEST_CASE("test_byte_strings_equal")
{
    json o1(byte_string("123456789"));
    json o2(byte_string{'1','2','3','4','5','6','7','8','9'});
    json o3(byte_string{'1','2','3','4','5','6','7','8'});

    CHECK(o1 == o2);
    CHECK(o2 == o1);
    CHECK(o3 != o1);
    CHECK(o2 != o3);
}


