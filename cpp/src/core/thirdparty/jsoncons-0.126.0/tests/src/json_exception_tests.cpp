
// Copyright 2016 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>
#include <jsoncons/json_reader.hpp>
#include <catch/catch.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>

using namespace jsoncons;

TEST_CASE("test_object_at")
{
    json a;
    REQUIRE_THROWS_AS(a.at("key1"), std::out_of_range);
    REQUIRE_THROWS_AS(static_cast<const json&>(a).at("key1"), std::out_of_range);

    a["key1"] = "value1";
    REQUIRE_THROWS_AS(a.at("key2"), std::out_of_range);
    REQUIRE_THROWS_AS(static_cast<const json&>(a).at("key2"), std::out_of_range);

    json b = json::array();
    REQUIRE_THROWS_AS(b.at("key1"), std::runtime_error);
    REQUIRE_THROWS_AS(static_cast<const json&>(b).at("key1"), std::runtime_error);
}

TEST_CASE("test_object_find")
{
    json b = json::array();
    b.resize(3);
    REQUIRE_THROWS_AS(b.find("key1"), std::runtime_error);
    REQUIRE_THROWS_AS(static_cast<const json&>(b).find("key1"), std::runtime_error);
    REQUIRE_THROWS_AS(b.find(std::string("key1")), std::runtime_error);
    REQUIRE_THROWS_AS(static_cast<const json&>(b).find(std::string("key1")), std::runtime_error);
}

TEST_CASE("test_array_at")
{
    json a = json::array();
    REQUIRE_THROWS_AS(a.at(0), std::out_of_range);
    REQUIRE_THROWS_AS(static_cast<const json&>(a).at(0), std::out_of_range);

    a.resize(3);
    REQUIRE_THROWS_AS(a.at(3), std::out_of_range);
    REQUIRE_THROWS_AS(static_cast<const json&>(a).at(3), std::out_of_range);
}

TEST_CASE("test_object_set")
{
    json b = json::array();
    b.resize(3);
    REQUIRE_THROWS_AS(b.insert_or_assign("key1","value1"), std::runtime_error);
}

TEST_CASE("test_array_add")
{
    json b;
    b["key1"] = "value1";
    REQUIRE_THROWS_AS(b.push_back(0), std::runtime_error);
}

TEST_CASE("test_object_index")
{
    json b;
    REQUIRE_THROWS_AS(b["key1"].as<std::string>(), std::out_of_range);

    b["key1"] = "value1";
    REQUIRE_THROWS_AS(b["key2"].as<std::string>(), std::out_of_range);
}

