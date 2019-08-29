// Copyright 2013 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/json_query.hpp>
#include <catch/catch.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <new>

using namespace jsoncons;

struct jsonpath_fixture
{
    static const char* store_text()
    {
        static const char* text = "{ \"store\": {\"book\": [ { \"category\": \"reference\",\"author\": \"Nigel Rees\",\"title\": \"Sayings of the Century\",\"price\": 8.95},{ \"category\": \"fiction\",\"author\": \"Evelyn Waugh\",\"title\": \"Sword of Honour\",\"price\": 12.99},{ \"category\": \"fiction\",\"author\": \"Herman Melville\",\"title\": \"Moby Dick\",\"isbn\": \"0-553-21311-3\",\"price\": 8.99},{ \"category\": \"fiction\",\"author\": \"J. R. R. Tolkien\",\"title\": \"The Lord of the Rings\",\"isbn\": \"0-395-19395-8\",\"price\": 22.99}],\"bicycle\": {\"color\": \"red\",\"price\": 19.95}}}";
        return text;
    }
    static const char* store_text_empty_isbn()
    {
        static const char* text = "{ \"store\": {\"book\": [ { \"category\": \"reference\",\"author\": \"Nigel Rees\",\"title\": \"Sayings of the Century\",\"price\": 8.95},{ \"category\": \"fiction\",\"author\": \"Evelyn Waugh\",\"title\": \"Sword of Honour\",\"price\": 12.99},{ \"category\": \"fiction\",\"author\": \"Herman Melville\",\"title\": \"Moby Dick\",\"isbn\": \"0-553-21311-3\",\"price\": 8.99},{ \"category\": \"fiction\",\"author\": \"J. R. R. Tolkien\",\"title\": \"The Lord of the Rings\",\"isbn\": \"\",\"price\": 22.99}],\"bicycle\": {\"color\": \"red\",\"price\": 19.95}}}";
        return text;
    }
    static const char* book_text()
    {
        static const char* text = "{ \"category\": \"reference\",\"author\": \"Nigel Rees\",\"title\": \"Sayings of the Century\",\"price\": 8.95}";
        return text;
    }

    json book()
    {
        json root = json::parse(jsonpath_fixture::store_text());
        json book = root["store"]["book"];
        return book;
    }

    json bicycle()
    {
        json root = json::parse(jsonpath_fixture::store_text());
        json bicycle = root["store"]["bicycle"];
        return bicycle;
    }
};

void test_error_code(const json& root, const std::string& path, std::error_code value, size_t line, size_t column)
{
    REQUIRE_THROWS_AS(jsonpath::json_query(root,path),jsonpath::jsonpath_error);
    try
    {
        json result = jsonpath::json_query(root,path);
    }
    catch (const jsonpath::jsonpath_error& e)
    {
        if (e.code() != value)
        {
            std::cout << path << "\n";
        }
        CHECK(e.code() == value);
        CHECK(e.line() == line);
        CHECK(e.column() == column);
    }
}

/*TEST_CASE("test_root_error")
{
    json root = json::parse(jsonpath_fixture::store_text());
    test_error_code(root, "..*", jsonpath_errc::expected_root,1,1);`
}*/

TEST_CASE("test_right_bracket_error")
{

    json root = json::parse(jsonpath_fixture::store_text());
    test_error_code(root, "$['store']['book'[*]", jsonpath::jsonpath_errc::unexpected_end_of_input,1,21);
}

TEST_CASE("jsonpath missing dot")
{
    json root = json::parse(jsonpath_fixture::store_text());
    test_error_code(root, "$.store.book[?(@category == 'fiction')][?(@.price < 15)].title", jsonpath::jsonpath_errc::expected_separator,1,17);
}

TEST_CASE("test_dot_dot_dot")
{

    json root = json::parse(jsonpath_fixture::store_text());
    test_error_code(root, "$.store...price", jsonpath::jsonpath_errc::expected_name,1,10);
}

TEST_CASE("test_dot_star_name")
{

    json root = json::parse(jsonpath_fixture::store_text());
    test_error_code(root, "$.store.*price", jsonpath::jsonpath_errc::expected_separator,1,10);
}
TEST_CASE("test_filter_error")
{
    json root = json::parse(jsonpath_fixture::store_text());
    std::string path = "$..book[?(.price<10)]";
    test_error_code(root, path, jsonpath::jsonpath_errc::parse_error_in_filter,1,17);
}

TEST_CASE("jsonpath slice errors")
{
    json root = json::parse(R"(
[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19]
)");

    SECTION("$[")
    {
        std::string path = "$[";
        test_error_code(root, path, jsonpath::jsonpath_errc::unexpected_end_of_input,1,3);
    }
    SECTION("$[1")
    {
        std::string path = "$[1";
        test_error_code(root, path, jsonpath::jsonpath_errc::unexpected_end_of_input,1,4);
    }
    SECTION("$[1:")
    {
        std::string path = "$[1:";
        test_error_code(root, path, jsonpath::jsonpath_errc::unexpected_end_of_input,1,5);
    }
    SECTION("$[1:1")
    {
        std::string path = "$[1:1";
        test_error_code(root, path, jsonpath::jsonpath_errc::unexpected_end_of_input,1,6);
    }

    SECTION("$[-:]")
    {
        std::string path = "$[-:]";
        test_error_code(root, path, jsonpath::jsonpath_errc::expected_slice_start,1,4);
    }

    SECTION("$[-1:-]")
    {
        std::string path = "$[-1:-]";
        test_error_code(root, path, jsonpath::jsonpath_errc::expected_slice_end,1,7);
    }

    SECTION("$[-1:-1:-]")
    {
        std::string path = "$[-1:-1:-]";
        test_error_code(root, path, jsonpath::jsonpath_errc::expected_slice_step,1,10);
    }
}



