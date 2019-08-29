// Copyright 2013 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/json_query.hpp>
#include <catch/catch.hpp>
#include <iostream>
#include <sstream>
#include <vector>
#include <map>
#include <utility>
#include <ctime>
#include <new>

using namespace jsoncons;

TEST_CASE("jsonpath function tests")
{
    const json store = json::parse(R"(
    {
        "store": {
            "book": [
                {
                    "category": "reference",
                    "author": "Nigel Rees",
                    "title": "Sayings of the Century",
                    "price": 8.95
                },
                {
                    "category": "fiction",
                    "author": "Evelyn Waugh",
                    "title": "Sword of Honour",
                    "price": 12.99
                },
                {
                    "category": "fiction",
                    "author": "Herman Melville",
                    "title": "Moby Dick",
                    "isbn": "0-553-21311-3",
                    "price": 8.99
                },
                {
                    "category": "fiction",
                    "author": "J. R. R. Tolkien",
                    "title": "The Lord of the Rings",
                    "isbn": "0-395-19395-8",
                    "price": 22.99
                }
            ],
            "bicycle": {
                "color": "red",
                "price": 19.95
            }
        }
    }
    )");

    SECTION("count")
    {
        json result = jsonpath::json_query(store,"count($.store.book[*])");

        size_t expected = 4;

        REQUIRE(result.size() == 1);
        CHECK(result[0].as<size_t>() == expected);
    }
#if 0
    SECTION("keys")
    {
        json result = json_query(store,"keys($.store.book[0])[*]");

        json expected = json::parse("[\"author\",\"category\",\"price\",\"title\"]");

        REQUIRE(result.size() == 4);
        CHECK(result == expected);
    }
    SECTION("sum")
    {
        json result = json_query(store,"sum($.store.book[*].price)");
        double expected = 53.92;
        REQUIRE(result.size() == 1);
        CHECK(result[0].as<double>() == Approx(expected).epsilon(0.000001));
    }
    SECTION("sum in filter")
    {
        json result = json_query(store,"$.store.book[?(@.price > sum($.store.book[*].price) / count($.store.book[*]))].title");
        std::string expected = "The Lord of the Rings";
        REQUIRE(result.size() == 1);
        CHECK(result[0].as<std::string>() == expected);
    }
    SECTION("avg")
    {
        json result = json_query(store,"avg($.store.book[*].price)");

        double expected = 13.48;

        REQUIRE(result.size() == 1);
        CHECK(result[0].as<double>() == Approx(expected).epsilon(0.000001));
    }
    SECTION("avg in filter")
    {
        json result = json_query(store,"$.store.book[?(@.price > avg($.store.book[*].price))].title");
        std::string expected = "The Lord of the Rings";
        REQUIRE(result.size() == 1);
        CHECK(result[0].as<std::string>() == expected);
    }

    SECTION("prod")
    {
        json result = json_query(store,"prod($.store.book[*].price)");

        double expected = 24028.731766049998;

        REQUIRE(result.size() == 1);
        CHECK(result[0].as<double>() == Approx(expected).epsilon(0.000001));
    }

    SECTION("min")
    {
        json result = json_query(store,"min($.store.book[*].price)");

        double expected = 8.95;

        REQUIRE(result.size() == 1);
        CHECK(result[0].as<double>() == Approx(expected).epsilon(0.000001));
    }

    SECTION("max")
    {
        json result = json_query(store,"max($.store.book[*].price)");

        double expected = 22.99;

        REQUIRE(result.size() == 1);
        CHECK(result[0].as<double>() == Approx(expected).epsilon(0.000001));
    }

    SECTION("max in filter")
    {
        std::string path = "$.store.book[?(@.price < max($.store.book[*].price))].title";

        json expected = json::parse(R"(
    ["Sayings of the Century","Sword of Honour","Moby Dick"]
        )");

        json result = json_query(store,path);

        CHECK(result == expected);
    }
#if !(defined(__GNUC__) && (__GNUC__ == 4 && __GNUC_MINOR__ < 9))
// GCC 4.8 has broken regex support: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=53631
    SECTION("tokenize")
    {
        json j("The cat sat on the mat");
        json result = json_query(j,"tokenize($,'\\\\s+')[*]");

        json expected = json::parse("[\"The\",\"cat\",\"sat\",\"on\",\"the\",\"mat\"]");

        CHECK(result == expected);
    }
    SECTION("tokenize in filter")
    {
        json j = json::parse("[\"The cat sat on the mat\",\"The cat on the mat\"]");
        json result = json_query(j,"$[?(tokenize(@,'\\\\s+')[2]=='sat')]");

        CHECK(result[0] == j[0]);
    }
    SECTION("tokenize in filter 2")
    {
        json result = json_query(store,"$.store.book[?(tokenize(@.author,'\\\\s+')[1] == 'Waugh')].title");
        std::string expected = "Sword of Honour";
        REQUIRE(result.size() == 1);
        CHECK(result[0].as<std::string>() == expected);
    }
#endif
#endif
}


