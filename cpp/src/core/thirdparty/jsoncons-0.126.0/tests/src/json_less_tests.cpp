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

TEST_CASE("json null less")
{
    json j1 = null_type();

    SECTION("empty object")
    {
        json j2;

        CHECK(j1 < j2);
        CHECK_FALSE(j2 < j1);
    }

    SECTION("object")
    {
        json j2;
        j2["a"] = 1;
        j2["b"] = 3;
        j2["c"] = 3;

        CHECK(j1 < j2);
        CHECK_FALSE(j2 < j1);
    }
}

TEST_CASE("json empty object less")
{
    json j1;

    SECTION("empty object")
    {
        json j2;

        CHECK_FALSE(j1 < j2);
    }

    SECTION("object with no members")
    {
        json j2 = json::object();

        CHECK_FALSE(j1 < j2);
    }

    SECTION("object with members")
    {
        json j2;
        j2["a"] = 1;
        j2["b"] = 3;
        j2["c"] = 3;

        CHECK(j1 < j2);
    }
}

TEST_CASE("json bool less")
{
    json jtrue = true;
    json jfalse = false;

    SECTION("bool")
    {
        CHECK(jfalse < jtrue);
        CHECK_FALSE(jtrue < jfalse);
    }

    SECTION("null")
    {
        json j = null_type();
        CHECK(j < jfalse);
        CHECK(j < jtrue);
    }
}

TEST_CASE("json int64 less")
{
    json j1 = -100;
}

TEST_CASE("json short string less")
{
    json j1 = "bcd";

    SECTION("short string")
    {
        json j2 = "cde";
        CHECK(j1 < j2);
        CHECK_FALSE(j2 < j1);
        json j3 = "bcda";
        CHECK(j1 < j3);
        CHECK_FALSE(j3 < j1);
    }

    SECTION("long string")
    {
        json j2 = "string too long for short string";
        CHECK(j1 < j2);
        CHECK_FALSE(j2 < j1);

        json j3 = "a string too long for short string";
        CHECK(j3 < j1);
        CHECK_FALSE(j1 < j3);
    }
}

TEST_CASE("json long string less")
{
    json j1 = "a string too long for short string";

    SECTION("short string")
    {
        json j2 = "a s";
        CHECK(j2 < j1);
        CHECK_FALSE(j1 < j2);
        json j3 = "bcd";
        CHECK(j1 < j3);
        CHECK_FALSE(j3 < j1);
    }

    SECTION("long string")
    {
        json j2 = "string too long for short string";
        CHECK(j1 < j2);
        CHECK_FALSE(j2 < j1);
    }
}

TEST_CASE("json array of string less")
{
    json j1 = json::array();
    j1.push_back("b");
    j1.push_back("c");
    j1.push_back("d");

    SECTION("array")
    {
        json j2 = json::array();
        j2.push_back("a");
        j2.push_back("b");
        j2.push_back("c");

        CHECK(j2 < j1);
        CHECK_FALSE(j1 < j2);
    }
}

