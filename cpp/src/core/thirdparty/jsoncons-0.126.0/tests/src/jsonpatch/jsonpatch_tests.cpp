// Copyright 2013 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif
#include <catch/catch.hpp>
#include <iostream>
#include <sstream>
#include <vector>
#include <map>
#include <utility>
#include <ctime>
#include <new>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpatch/jsonpatch.hpp>

using namespace jsoncons;
using namespace jsoncons::literals;

void check_patch(json& target, const json& patch, std::error_code expected_ec, const json& expected)
{
    std::error_code ec;
    jsonpatch::apply_patch(target, patch, ec);
    if (ec != expected_ec || expected != target)
    {
        std::cout << "target:\n" << target << std::endl;
    }
    CHECK(ec == expected_ec);
    CHECK(target == expected);
}

TEST_CASE("add_an_object_member")
{
    json target = R"(
        { "foo": "bar"}
    )"_json;

    json patch = R"(
    [
        { "op": "add", "path": "/baz", "value": "qux" }
    ]
    )"_json;

    json expected = R"(
        {"baz":"qux","foo":"bar"}
    )"_json;

    check_patch(target,patch,std::error_code(),expected);
}

TEST_CASE("add_an_array_element")
{
    json target = R"(
        { "foo": [ "bar", "baz" ] }
    )"_json;

    json patch = R"(
    [
        { "op": "add", "path": "/foo/1", "value": "qux" }
    ]
    )"_json;

    json expected = R"(
        { "foo": [ "bar", "qux", "baz" ] }
    )"_json;

    check_patch(target,patch,std::error_code(),expected);
}

TEST_CASE("remove_an_object_member")
{
    json target = R"(
        {
            "baz": "qux",
            "foo": "bar"
        }
    )"_json;

    json patch = R"(
    [
        { "op": "remove", "path": "/baz" }
    ]
    )"_json;

    json expected = R"(
        { "foo": "bar" }
    )"_json;

    check_patch(target,patch,std::error_code(),expected);
}

TEST_CASE("remove_an_array_element")
{
    json target = R"(
        { "foo": [ "bar", "qux", "baz" ] }
    )"_json;

    json patch = R"(
    [
        { "op": "remove", "path": "/foo/1" }
    ]
    )"_json;

    json expected = R"(
        { "foo": [ "bar", "baz" ] }
    )"_json;

    check_patch(target,patch,std::error_code(),expected);
}

TEST_CASE("replace_a_value")
{
    json target = R"(
        {
            "baz": "qux",
            "foo": "bar"
        }
    )"_json;

    json patch = R"(
        [
            { "op": "replace", "path": "/baz", "value": "boo" }
        ]
    )"_json;

    json expected = R"(
        {
            "baz": "boo",
            "foo": "bar"
        }
    )"_json;

    check_patch(target,patch,std::error_code(),expected);
}

TEST_CASE("move_a_value")
{
    json target = R"(
        {
            "foo": {
                "bar": "baz",
                "waldo": "fred"
            },
            "qux": {
                "corge": "grault"
            }
        }
    )"_json;

    json patch = R"(
    [
        { "op": "move", "from": "/foo/waldo", "path": "/qux/thud" }
    ]
    )"_json;

    json expected = R"(
       {
           "foo": {
                   "bar": "baz"
                  },
           "qux": {
                     "corge": "grault",
                     "thud": "fred"
                  }
       }
    )"_json;

    check_patch(target,patch,std::error_code(),expected);
}

TEST_CASE("move_an_array_element")
{
    json target = R"(
        { "foo": [ "all", "grass", "cows", "eat" ] }
    )"_json;

    json patch = R"(
        [
           { "op": "move", "from": "/foo/1", "path": "/foo/3" }
        ]
    )"_json;

    json expected = R"(
        { "foo": [ "all", "cows", "eat", "grass" ] }    
    )"_json;

    check_patch(target,patch,std::error_code(),expected);
}

TEST_CASE("add_to_nonexistent_target")
{
    json target = R"(
        { "foo": "bar" }
    )"_json;

    json patch = R"(
        [
           { "op": "add", "path": "/baz/bat", "value": "qux" }
        ]
    )"_json;

    json expected = target;

    check_patch(target,patch,jsonpatch::jsonpatch_errc::add_failed,expected);
}

TEST_CASE("testing_a_value_success")
{
    json target = R"(
        {
            "baz": "qux",
            "foo": [ "a", 2, "c" ]
        }
    )"_json;

    json patch = R"(
        [
           { "op": "test", "path": "/baz", "value": "qux" },
           { "op": "test", "path": "/foo/1", "value": 2 }
        ]
    )"_json;

    json expected = target;

    check_patch(target,patch,std::error_code(),expected);
}

TEST_CASE("testing_a_value_error")
{
    json target = R"(
        { "baz": "qux" }

    )"_json;

    json patch = R"(
        [
           { "op": "test", "path": "/baz", "value": "bar" }
        ]
    )"_json;

    json expected = target;

    check_patch(target,patch,jsonpatch::jsonpatch_errc::test_failed,expected);
}

TEST_CASE("adding_nested_member_object")
{
    json target = R"(
        { "foo": "bar" }

    )"_json;

    json patch = R"(
        [
            { "op": "add", "path": "/child", "value": { "grandchild": { } } }
        ]
    )"_json;

    json expected = R"(
        {
            "foo": "bar",
            "child": {
                    "grandchild": {
                }
            }
        }

    )"_json;

    check_patch(target,patch,std::error_code(),expected);
}

TEST_CASE("tilde_escape_ordering")
{
    json target = R"(
        {
            "/": 9,
            "~1": 10
        }

    )"_json;

    json patch = R"(
        [
             {"op": "test", "path": "/~01", "value": 10}
        ]
    )"_json;

    json expected = R"(
        {
            "/": 9,
            "~1": 10
        }

    )"_json;

    check_patch(target,patch,std::error_code(),expected);
}

TEST_CASE("comparing_strings_and_numbers")
{
    json target = R"(
        {
            "/": 9,
            "~1": 10
        }

    )"_json;

    json patch = R"(
        [
            {"op": "test", "path": "/~01", "value": "10"}
        ]
    )"_json;

    json expected = target;

    check_patch(target,patch,jsonpatch::jsonpatch_errc::test_failed,expected);
}

TEST_CASE("adding_an_array_value")
{
    json target = R"(
        { "foo": ["bar"] }

    )"_json;

    json patch = R"(
        [
            { "op": "add", "path": "/foo/-", "value": ["abc", "def"] }
        ]
    )"_json;

    json expected = R"(
        { "foo": ["bar", ["abc", "def"]] }

    )"_json;

    check_patch(target,patch,std::error_code(),expected);
}

TEST_CASE("test_add_add")
{
    json target = R"(
        { "foo": "bar"}
    )"_json;

    json patch = R"(
        [
            { "op": "add", "path": "/baz", "value": "qux" },
            { "op": "add", "path": "/foo", "value": [ "bar", "baz" ] }
        ]
    )"_json;

    json expected = R"(
        { "baz":"qux", "foo": [ "bar", "baz" ]}
    )"_json;

    check_patch(target,patch,std::error_code(),expected);
}

TEST_CASE("test_add_add_add_fail")
{
    json target = R"(
        { "foo": "bar"}
    )"_json;

    json patch = R"(
        [
            { "op": "add", "path": "/baz", "value": "qux" },
            { "op": "add", "path": "/foo", "value": [ "bar", "baz" ] },
            { "op": "add", "path": "/baz/bat", "value": "qux" } // nonexistent target
        ]
    )"_json;

    json expected = target;

    check_patch(target,patch,jsonpatch::jsonpatch_errc::add_failed,expected);
}

TEST_CASE("test_add_remove_remove_fail")
{
    json target = R"(
        {
            "baz": "boo",
            "foo": [ "bar", "qux", "baz" ]
        }
    )"_json;

    json patch = R"(
        [
            { "op": "add", "path": "/baz", "value": "qux" },
            { "op": "remove", "path": "/foo/2" },
            { "op": "remove", "path": "/foo/2" } // nonexistent target
        ]
    )"_json;

    json expected = target;

    check_patch(target,patch,jsonpatch::jsonpatch_errc::remove_failed,expected);
}

TEST_CASE("test_move_copy_replace_remove_fail")
{
    json target = R"(
        {
            "baz": ["boo"],
            "foo": [ "bar", "qux", "baz" ]
        }
    )"_json;

    json patch = R"(
        [
            { "op": "add", "path": "/baz/-", "value": "xyz" },
            { "op": "move", "from": "/foo/1", "path" : "/baz/-" },
            { "op": "copy", "from": "/baz/0", "path" : "/foo/-" },
            { "op": "replace", "path": "/foo/2", "value" : "qux" },
            { "op": "remove", "path": "/foo/3" } // nonexistent target
        ]
    )"_json;

    json expected = target;

    check_patch(target,patch,jsonpatch::jsonpatch_errc::remove_failed,expected);

}

TEST_CASE("test_diff1")
{
    json source = R"(
        {"/": 9, "~1": 10, "foo": "bar"}
    )"_json;

    json target = R"(
        { "baz":"qux", "foo": [ "bar", "baz" ]}
    )"_json;

    auto patch = jsonpatch::from_diff(source, target);

    check_patch(source,patch,std::error_code(),target);
}

TEST_CASE("test_diff2")
{
    json source = R"(
        { 
            "/": 3,
            "foo": "bar"
        }
    )"_json;

    json target = R"(
        {
            "/": 9,
            "~1": 10
        }
    )"_json;

    auto patch = jsonpatch::from_diff(source, target);

    check_patch(source,patch,std::error_code(),target);
}

TEST_CASE("add_when_new_items_in_target_array1")
{
    jsoncons::json source = R"(
        {"/": 9, "foo": [ "bar"]}
    )"_json;

    jsoncons::json target = R"(
        { "baz":"qux", "foo": [ "bar", "baz" ]}
    )"_json;

    jsoncons::json patch = jsoncons::jsonpatch::from_diff(source, target); 

    check_patch(source,patch,std::error_code(),target);
}

TEST_CASE("add_when_new_items_in_target_array2")
{
    jsoncons::json source = R"(
        {"/": 9, "foo": [ "bar", "bar"]}
    )"_json;

    jsoncons::json target = R"(
        { "baz":"qux", "foo": [ "bar", "baz" ]}
    )"_json;

    jsoncons::json patch = jsoncons::jsonpatch::from_diff(source, target); 

    check_patch(source,patch,std::error_code(),target);
}



