// Copyright 2017 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>
#include <catch/catch.hpp>
#include <iostream>
#include <sstream>
#include <vector>
#include <map>
#include <utility>
#include <ctime>
#include <new>

using namespace jsoncons;
using namespace jsoncons::literals;

void check_get_with_const_ref(const json& example, const std::string& pointer, const json& expected)
{

    std::error_code ec;
    const json& result = jsonpointer::get(example,pointer,ec);
    CHECK_FALSE(ec);
    CHECK(result == expected);
}

void check_contains(const json& example, const std::string& pointer, bool expected)
{
    bool result = jsonpointer::contains(example,pointer);
    CHECK(result == expected);
}

void check_insert_or_assign(json& example, const std::string& path, const json& value, const json& expected)
{
    std::error_code ec;
    jsonpointer::insert_or_assign(example, path, value, ec);
    CHECK_FALSE(ec);
    CHECK(example == expected);
}

void check_replace(json& example, const std::string& path, const json& value, const json& expected)
{
    std::error_code ec;
    jsonpointer::replace(example, path, value, ec);
    CHECK_FALSE(ec);
    CHECK(example == expected);
}

void check_remove(json& example, const std::string& path, const json& expected)
{
    std::error_code ec;
    jsonpointer::remove(example, path, ec);
    CHECK_FALSE(ec);
    CHECK(example == expected);
}

TEST_CASE("get_with_const_ref_test")
{
// Example from RFC 6901
const json example = json::parse(R"(
   {
      "foo": ["bar", "baz"],
      "": 0,
      "a/b": 1,
      "c%d": 2,
      "e^f": 3,
      "g|h": 4,
      "i\\j": 5,
      "k\"l": 6,
      " ": 7,
      "m~n": 8
   }
)");

    check_contains(example,"",true);
    check_contains(example,"/foo",true);
    check_contains(example,"/foo/0",true);
    check_contains(example,"/",true);
    check_contains(example,"/a~1b",true);
    check_contains(example,"/c%d",true);
    check_contains(example,"/e^f",true);
    check_contains(example,"/g|h",true);
    check_contains(example,"/i\\j",true);
    check_contains(example,"/k\"l",true);
    check_contains(example,"/ ",true);
    check_contains(example,"/m~0n",true);

    check_get_with_const_ref(example,"",example);
    check_get_with_const_ref(example,"/foo",json::parse("[\"bar\", \"baz\"]"));
    check_get_with_const_ref(example,"/foo/0",json("bar"));
    check_get_with_const_ref(example,"/",json(0));
    check_get_with_const_ref(example,"/a~1b",json(1));
    check_get_with_const_ref(example,"/c%d",json(2));
    check_get_with_const_ref(example,"/e^f",json(3));
    check_get_with_const_ref(example,"/g|h",json(4));
    check_get_with_const_ref(example,"/i\\j",json(5));
    check_get_with_const_ref(example,"/k\"l",json(6));
    check_get_with_const_ref(example,"/ ",json(7));
    check_get_with_const_ref(example,"/m~0n",json(8));
}

TEST_CASE("get_with_ref_test")
{
// Example from RFC 6901
json example = json::parse(R"(
   {
      "foo": ["bar", "baz"]
   }
)");

    std::error_code ec;
    json& result = jsonpointer::get(example,"/foo/0",ec);
    CHECK_FALSE(ec);

    result = "bat";

    //std::cout << example << std::endl;
}

TEST_CASE("get_with_nonexistent_target")
{
    json example = R"(
        { "foo": "bar" }
    )"_json;

    check_contains(example,"/baz",false);
}

// insert_or_assign

TEST_CASE("test_add_object_member")
{
    json example = json::parse(R"(
    { "foo": "bar"}
    )");

    const json expected = json::parse(R"(
    { "foo": "bar", "baz" : "qux"}
    )");

    check_insert_or_assign(example,"/baz", json("qux"), expected);
}

TEST_CASE("test_add_array_element")
{
    json example = json::parse(R"(
    { "foo": [ "bar", "baz" ] }
    )");

    const json expected = json::parse(R"(
    { "foo": [ "bar", "qux", "baz" ] }
    )");

    check_insert_or_assign(example,"/foo/1", json("qux"), expected);
}

TEST_CASE("test_add_array_value")
{
    json example = json::parse(R"(
     { "foo": ["bar"] }
    )");

    const json expected = json::parse(R"(
    { "foo": ["bar", ["abc", "def"]] }
    )");

    check_insert_or_assign(example,"/foo/-", json::array({"abc", "def"}), expected);
}

// remove

TEST_CASE("test_remove_object_member")
{
    json example = json::parse(R"(
    { "foo": "bar", "baz" : "qux"}
    )");

    const json expected = json::parse(R"(
        { "foo": "bar"}
    )");

    check_remove(example,"/baz", expected);
}

TEST_CASE("test_remove_array_element")
{
    json example = json::parse(R"(
        { "foo": [ "bar", "qux", "baz" ] }
    )");

    const json expected = json::parse(R"(
        { "foo": [ "bar", "baz" ] }
    )");

    check_remove(example,"/foo/1", expected);
}

// replace

TEST_CASE("test_replace_object_value")
{
    json example = json::parse(R"(
        {
          "baz": "qux",
          "foo": "bar"
        }
    )");

    const json expected = json::parse(R"(
        {
          "baz": "boo",
          "foo": "bar"
        }
    )");

    check_replace(example,"/baz", json("boo"), expected);
}
TEST_CASE("test_replace_array_value")
{
    json example = json::parse(R"(
        { "foo": [ "bar", "baz" ] }
    )");

    const json expected = json::parse(R"(
        { "foo": [ "bar", "qux" ] }
    )");

    check_replace(example,"/foo/1", json("qux"), expected);
}

TEST_CASE("jsonpointer path tests")
{
    SECTION("/a~1b")
    {
        jsonpointer::address p("/a~1b");

        auto it = p.begin();
        auto end = p.end();

        CHECK((*it++ == "a/b"));
        CHECK(it == end);
    }
    SECTION("/a~1b")
    {
        jsonpointer::address p("/m~0n");

        auto it = p.begin();
        auto end = p.end();

        CHECK((*it++ == "m~n"));
        CHECK(it == end);
    }
    SECTION("/0/1")
    {
        jsonpointer::address p("/0/1");

        auto it = p.begin();
        auto end = p.end();

        CHECK((*it++ == "0"));
        CHECK((*it++ == "1"));
        CHECK(it == end);
    }
}

TEST_CASE("jsonpointer concatenation")
{
    // Example from RFC 6901
    json example = json::parse(R"(
       {
          "a/b": ["bar", "baz"],
          "m~n": ["foo", "qux"]
       }
    )");

    SECTION("path append a/b")
    {
        jsonpointer::address p;
        p /= "a/b";
        p /= "0";

        auto it = p.begin();
        auto end = p.end();

        CHECK((*it++ == "a/b"));
        CHECK((*it++ == "0"));
        CHECK(it == end);

        std::error_code ec;
        json j = jsonpointer::get(example, p, ec);
        std::cout << j << "\n";
    }

    SECTION("concatenate two paths")
    {
        jsonpointer::address p1;
        p1 /= "m~n";
        jsonpointer::address p2;
        p2 /= "1";
        jsonpointer::address p = p1 + p2;

        auto it = p.begin();
        auto end = p.end();

        CHECK((*it++ == "m~n"));
        CHECK((*it++ == "1"));
        CHECK(it == end);

        json j = jsonpointer::get(example, p);
        std::cout << j << "\n";
    }
}

