// Copyright 2018 Daniel Parker
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

TEST_CASE("json(string_view)")
{
    json::string_view_type sv("Hello world.");

    json j(sv);

    CHECK(j.as<json::string_view_type>() == sv);
    CHECK(j.as_string_view() == sv);
}

TEST_CASE("json(string, semantic_tag::datetime)")
{
    std::string s("2015-05-07 12:41:07-07:00");

    json j(s, semantic_tag::datetime);

    CHECK(j.get_semantic_tag() == semantic_tag::datetime);
    CHECK(j.as<std::string>() == s);
}


TEST_CASE("json(string, semantic_tag::timestamp)")
{
    SECTION("positive integer")
    {
        int t = 10000;
        json j(t, semantic_tag::timestamp);

        CHECK(j.get_semantic_tag() == semantic_tag::timestamp);
        CHECK(j.as<int>() == t);
    }
    SECTION("negative integer")
    {
        int t = -10000;
        json j(t, semantic_tag::timestamp);

        CHECK(j.get_semantic_tag() == semantic_tag::timestamp);
        CHECK(j.as<int>() == t);
    }
    SECTION("floating point")
    {
        double t = 10000.1;
        json j(t, semantic_tag::timestamp);

        CHECK(j.get_semantic_tag() == semantic_tag::timestamp);
        CHECK(j.as<double>() == t);
    }

}

TEST_CASE("json get_allocator() tests")
{
    SECTION("short string")
    {
        json j("short");

        CHECK(j.get_allocator() == json::allocator_type());
    }
    SECTION("long string")
    {
        json::allocator_type alloc;
        json j("string too long for short string", alloc);

        CHECK(j.get_allocator() == alloc);
    }
    SECTION("byte string")
    {
        json::allocator_type alloc;
        json j(byte_string({'H','e','l','l','o'}),alloc);

        CHECK(j.get_allocator() == alloc);
    }
    SECTION("array")
    {
        json::allocator_type alloc;
        json j = json::array(alloc);

        CHECK(j.get_allocator() == alloc);
    }
    SECTION("object")
    {
        json::allocator_type alloc;
        json j(alloc);

        CHECK(j.get_allocator() == alloc);
    }
}

