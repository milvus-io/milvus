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
using namespace jsoncons::jsonpath;

// https://jsonpath.herokuapp.com/

TEST_CASE("store normalized path tests")
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
        },
        "expensive": 10
    }
    )");

    SECTION("$.store.book[0].category")
    {
        const json expected = json::parse(R"(
        [
           "$['store']['book'][0]['category']"
        ]
        )");

        std::string path = "$.store.book[0].category";

        json result = json_query(store,"$.store.book[0].category",result_type::path);
        CHECK(result == expected);
    }
    SECTION("test_string_index")
    {

    const json expected = json::parse(R"(
    [
       "$['store']['book'][0]['category'][0]",
       "$['store']['book'][0]['category'][2]"
    ]
    )");

        std::string path = "$.store.book.0.category[0,2]";

        json result = json_query(store,path,result_type::path);
        //CHECK(result == expected); // revisit

        //json result2 = json_query(store,path,result_type::value);
        //std::cout << pretty_print(result2) << std::endl;
    }

    SECTION("test_array_length")
    {

    const json expected = json::parse(R"(
    [
        "$['store']['book']['length']"
    ]
    )");

        std::string path = "$.store.book.length";
        json result = json_query(store,path,result_type::path);
        CHECK(result == expected);

        std::string path2 = "$.store.book['length']";
        json result2 = json_query(store, path, result_type::path);
        CHECK(result2 == expected);
    }

    SECTION("test_price_filter")
    {

    const json expected = json::parse(R"(
    [
        "$['store']['book'][0]['title']",
        "$['store']['book'][2]['title']"
    ]
    )");

        std::string path = "$.store.book[?(@.price < 10)].title";
        json result = json_query(store,path,result_type::path);
        CHECK(result == expected);
    }

    SECTION("test_length_expression")
    {

    const json expected = json::parse(R"(
    [
         "$['store']['book'][3]['title']"
    ]
    )");

        std::string path = "$.store.book[(@.length-1)].title";
        json result = json_query(store,path,result_type::path);
        CHECK(result == expected);
    }
}

TEST_CASE("normalized path test")
{
    json test = json::parse(R"(
    {
      "books": [
        {"title": "b1", "chapters":[{"subtitle":"b1c1"}]},
        {"title": "b2", "chapters":[{"subtitle":"b2c1"}]}
      ]
    } )");

    json expected = json::parse(R"([{"subtitle":"b2c1"}])");
    std::string expected_path = "$['books'][1]['chapters'][0]"; 

    SECTION("$.books[*].chapters[?(@.subtitle == 'b2c1')]")
    {
        std::string path = "$.books[*].chapters[?(@.subtitle == 'b2c1')]";
        json result = json_query(test, path);

        json path_result = json_query(test, path, result_type::path);
        CHECK(result == expected);

        REQUIRE(path_result.size() == 1);
        CHECK(path_result[0].as<std::string>() == expected_path);
    }
    SECTION("$..books[*].chapters[?(@.subtitle == 'b2c1')]")
    {
        std::string path = "$..books[*].chapters[?(@.subtitle == 'b2c1')]";
        json result = json_query(test, path);
        CHECK(result == expected);

        json path_result = json_query(test, path, result_type::path);

        REQUIRE(path_result.size() == 1);
        CHECK(path_result[0].as<std::string>() == expected_path);
    }
    SECTION("$..[?(@.subtitle == 'b2c1')]")
    {
        std::string path = "$..[?(@.subtitle == 'b2c1')]";
        json result = json_query(test, path);
        CHECK(result == expected);

        json path_result = json_query(test, path, result_type::path);

        REQUIRE(path_result.size() == 1);
        CHECK(path_result[0].as<std::string>() == expected_path);
    }
}

TEST_CASE("jsonpath object union normalized path test")
{
    json test = json::parse(R"(
    {
      "books": [
        {"title": "b1", "chapters":[{"subtitle":"b1c1"}]},
        {"title": "b2", "chapters":[{"subtitle":"b2c1"}]}
      ]
    } )");


    SECTION("$..[chapters[?(@.subtitle == 'b2c1')],chapters[?(@.subtitle == 'b2c1')]]")
    {
        json expected = json::parse(R"([{"subtitle":"b2c1"}])");
        json expected_path = json::parse(R"(["$['books'][1]['chapters[?(@.subtitle == 'b2c1')]']"])"); 

        std::string path = "$..[chapters[?(@.subtitle == 'b2c1')],chapters[?(@.subtitle == 'b2c1')]]";
        json result = json_query(test, path);
        CHECK(result == expected);

        json path_result = json_query(test, path, result_type::path);

        CHECK(path_result == expected_path);
    }
}

TEST_CASE("jsonpath array union normalized path test")
{
    json root = json::parse(R"(
[[1,2,3,4,1,2,3,4],[0,1,2,3,4,5,6,7,8,9],[0,1,2,3,4,5,6,7,8,9]]
)");

    SECTION("$[0,1,2]")
    {
        json expected = json::parse(R"([[0,1,2,3,4,5,6,7,8,9],[1,2,3,4,1,2,3,4]])");
        json expected_path = json::parse(R"( ["$[1]","$[0]"])"); 

        std::string path = "$[0,1,2]";
        json result = json_query(root, path);
        CHECK(result == expected);

        json path_result = json_query(root, path, result_type::path);
        CHECK(path_result == expected_path);
    }

    SECTION("$[0][0:4,2:8]")
    {
        json expected = json::parse(R"([1,2,3,4])");
        json expected_path = json::parse(R"(["$[0][0]","$[0][1]","$[0][2]","$[0][3]"])"); 

        std::string path = "$[0][0:4,2:8]";
        json result = json_query(root, path);
        CHECK(result == expected);

        json path_result = json_query(root, path, result_type::path);
        CHECK(path_result == expected_path);
    }
}

