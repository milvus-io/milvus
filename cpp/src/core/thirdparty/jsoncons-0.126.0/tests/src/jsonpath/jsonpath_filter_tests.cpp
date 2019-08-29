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
using namespace jsoncons::jsonpath;
using namespace jsoncons::jsonpath::detail;

struct jsonpath_filter_fixture
{
    static const char* store_text()
    {
        static const char* text = "{ \"store\": {\"book\": [ { \"category\": \"reference\",\"author\": \"Nigel Rees\",\"title\": \"Sayings of the Century\",\"price\": 8.95},{ \"category\": \"fiction\",\"author\": \"Evelyn Waugh\",\"title\": \"Sword of Honour\",\"price\": 12.99},{ \"category\": \"fiction\",\"author\": \"Herman Melville\",\"title\": \"Moby Dick\",\"isbn\": \"0-553-21311-3\",\"price\": 8.99},{ \"category\": \"fiction\",\"author\": \"J. R. R. Tolkien\",\"title\": \"The Lord of the Rings\",\"isbn\": \"0-395-19395-8\",\"price\": 22.99}],\"bicycle\": {\"color\": \"red\",\"price\": 19.95}}}";
        return text;
    }
    static const char* book_text()
    {
        static const char* text = "{ \"category\": \"reference\",\"author\": \"Nigel Rees\",\"title\": \"Sayings of the Century\",\"price\": 8.95}";
        return text;
    }

    json book()
    {
        json root = json::parse(jsonpath_filter_fixture::store_text());
        json book = root["store"]["book"];
        return book;
    }

    json bicycle()
    {
        json root = json::parse(jsonpath_filter_fixture::store_text());
        json bicycle = root["store"]["bicycle"];
        return bicycle;
    }
};

TEST_CASE("test_div")
{
    const char* pend;
    jsonpath_filter_parser<json> parser;

    json context = json::array();
    context.push_back(3);

    std::string s1 = "(3/1)";
    auto expr1 = parser.parse(context,s1.c_str(), s1.c_str()+ s1.length(), &pend);
    auto result1 = expr1.eval(context);
    CHECK(json(3) == result1);

    std::string s2 = "(3/@.length)";
    auto expr2 = parser.parse(context,s2.c_str(), s2.c_str()+ s2.length(), &pend);
    auto result2 = expr2.eval(context);
    CHECK(json(3) == result2);

    std::string s3 = "(5/2)";
    auto expr3 = parser.parse(context,s3.c_str(), s3.c_str()+ s3.length(), &pend);
    auto result3 = expr3.eval(context);
    CHECK(json(2.5) == result3);

    std::string s4 = "(@.length/3)";
    auto expr4 = parser.parse(context,s4.c_str(), s4.c_str()+ s4.length(), &pend);
    auto result4 = expr4.eval(context);
    CHECK(0.333333 == Approx(result4.as<double>()).epsilon(0.001));

    std::string s5 = "(@.0/@.length)";
    auto expr5 = parser.parse(context,s5.c_str(), s5.c_str()+ s5.length(), &pend);
    auto result5 = expr5.eval(context);
    CHECK(json(3) == result5);
}

TEST_CASE("test_mult")
{
    const char* pend;
    jsonpath_filter_parser<json> parser;

    json context = json::array();
    context.push_back(1);
    context.push_back(2);

    std::string s1 = "(3*1)";
    auto expr1 = parser.parse(context, s1.c_str(), s1.c_str()+ s1.length(), &pend);
    auto result1 = expr1.eval(context);
    CHECK(json(3) == result1);

    std::string s2 = "(3*@.length)";
    auto expr2 = parser.parse(context, s2.c_str(), s2.c_str()+ s2.length(), &pend);
    auto result2 = expr2.eval(context);
    CHECK(json(6) == result2);

    std::string s3 = "(5*2)";
    auto expr3 = parser.parse(context, s3.c_str(), s3.c_str()+ s3.length(), &pend);
    auto result3 = expr3.eval(context);
    CHECK(json(10) == result3);

    std::string s4 = "(@.length*3)";
    auto expr4 = parser.parse(context, s4.c_str(), s4.c_str()+ s4.length(), &pend);
    auto result4 = expr4.eval(context);
    CHECK(json(6) == result4);

    std::string s5 = "(@.length*@.1)";
    auto expr5 = parser.parse(context, s5.c_str(), s5.c_str()+ s5.length(), &pend);
    auto result5 = expr5.eval(context);
    CHECK(json(4) == result5);
}

TEST_CASE("test_minus")
{
    const char* pend;
    jsonpath_filter_parser<json> parser;

    json context = json::array();
    context.push_back(10.0);

    std::string s1 = "(3-1)";
    auto expr1 = parser.parse(context, s1.c_str(), s1.c_str()+ s1.length(), &pend);
    auto result1 = expr1.eval(context);
    CHECK(json(2) == result1);

    std::string s2 = "(3-@.length)";
    auto expr2 = parser.parse(context, s2.c_str(), s2.c_str()+ s2.length(), &pend);
    auto result2 = expr2.eval(context);
    CHECK(json(2) == result2);

    std::string s3 = "(3.5-1.0)";
    auto expr3 = parser.parse(context, s3.c_str(), s3.c_str()+ s3.length(), &pend);
    auto result3 = expr3.eval(context);
    CHECK(json(2.5) == result3);

    std::string s4 = "(@.length-3)";
    auto expr4 = parser.parse(context, s4.c_str(), s4.c_str()+ s4.length(), &pend);
    auto result4 = expr4.eval(context);
    CHECK(json(-2) ==result4);

    std::string s5 = "(@.length-@.0)";
    auto expr5 = parser.parse(context, s5.c_str(), s5.c_str()+ s5.length(), &pend);
    auto result5 = expr5.eval(context);
    CHECK(json(-9) ==result5);
}

TEST_CASE("test_lt")
{
    const char* pend;
    jsonpath_filter_parser<json> parser;

    json context = json::array();
    context.push_back(100);
    context.push_back(1);

    std::string s1 = "(3 < 1)";
    auto expr1 = parser.parse(context, s1.c_str(), s1.c_str()+ s1.length(), &pend);
    auto result1 = expr1.eval(context);
    CHECK(result1 == json(false));

    std::string s2 = "(3 < @.length)";
    auto expr2 = parser.parse(context, s2.c_str(), s2.c_str()+ s2.length(), &pend);
    auto result2 = expr2.eval(context);
    CHECK(result2 == json(false));

    std::string s3 = "(@.length < 3)";
    auto expr3 = parser.parse(context, s3.c_str(), s3.c_str()+ s3.length(), &pend);
    auto result3 = expr3.eval(context);
    CHECK(result3 == json(true));

    std::string s4 = "(@.length < @.length)";
    auto expr4 = parser.parse(context, s4.c_str(), s4.c_str()+ s4.length(), &pend);
    auto result4 = expr4.eval(context);
    CHECK(result4 == json(false));

    std::string s5 = "(@.length < @.0)";
    auto expr5 = parser.parse(context, s5.c_str(), s5.c_str()+ s5.length(), &pend);
    auto result5 = expr5.eval(context);
    CHECK(json(true) == result5);

    std::string s6 = "(@.length < @.1)";
    auto expr6 = parser.parse(context, s6.c_str(), s6.c_str()+ s6.length(), &pend);
    auto result6 = expr6.eval(context);
    CHECK(json(false) == result6);
}

TEST_CASE("test_lte")
{
    const char* pend;
    jsonpath_filter_parser<json> parser;

    json context = json::array();
    context.push_back(1);

    std::string s1 = "(3 <= 1)";
    auto expr1 = parser.parse(context, s1.c_str(), s1.c_str()+ s1.length(), &pend);
    auto result1 = expr1.eval(context);
    CHECK(result1 == json(false));

    std::string s2 = "(3 <= @.length)";
    auto expr2 = parser.parse(context, s2.c_str(), s2.c_str()+ s2.length(), &pend);
    auto result2 = expr2.eval(context);
    CHECK(result2 == json(false));
}

TEST_CASE("test_gt")
{
    const char* pend;
    jsonpath_filter_parser<json> parser;

    json context = json::array();
    context.push_back(1);

    std::string s1 = "(3 > 1)";
    auto expr1 = parser.parse(context, s1.c_str(), s1.c_str()+ s1.length(), &pend);
    auto result1 = expr1.eval(context);
    CHECK(result1 == json(true));

    std::string s2 = "(3 > @.length)";
    auto expr2 = parser.parse(context, s2.c_str(), s2.c_str()+ s2.length(), &pend);
    auto result2 = expr2.eval(context);
    CHECK(result2 == json(true));
}

TEST_CASE("test_gte")
{
    const char* pend;
    jsonpath_filter_parser<json> parser;

    json context = json::array();
    context.push_back(1);

    std::string s1 = "(3 >= 1)";
    auto expr1 = parser.parse(context, s1.c_str(), s1.c_str()+ s1.length(), &pend);
    auto result1 = expr1.eval(context);
    CHECK(result1 == json(true));

    std::string s2 = "(3 >= @.length)";
    auto expr2 = parser.parse(context, s2.c_str(), s2.c_str()+ s2.length(), &pend);
    auto result2 = expr2.eval(context);
    CHECK(result2 == json(true));
}

TEST_CASE("test_eq")
{
    const char* pend;
    jsonpath_filter_parser<json> parser;

    json context = json::array();
    context.push_back(1);

    std::string s1 = "(3 == 1)";
    auto expr1 = parser.parse(context, s1.c_str(), s1.c_str()+ s1.length(), &pend);
    auto result1 = expr1.eval(context);
    CHECK(result1 == json(false));

    std::string s2 = "(3 == @.length)";
    auto expr2 = parser.parse(context, s2.c_str(), s2.c_str()+ s2.length(), &pend);
    auto result2 = expr2.eval(context);
    CHECK(result2 == json(false));

    std::string s3 = "(1 == 1)";
    auto expr3 = parser.parse(context, s3.c_str(), s3.c_str()+ s3.length(), &pend);
    auto result3 = expr3.eval(context);
    CHECK(result3 == json(true));

    std::string s4 = "(1 == @.length)";
    auto expr4 = parser.parse(context, s4.c_str(), s4.c_str()+ s4.length(), &pend);
    auto result4 = expr4.eval(context);
    CHECK(result4 == json(true));
}

TEST_CASE("test_precedence")
{
    const char* pend;
    jsonpath_filter_parser<json> parser;

    json context = json::array();
    context.push_back(1);
    context.push_back(2);

    std::string s1 = "(@.0 == 1 && @.1 == 2)";
    auto expr1 = parser.parse(context, s1.c_str(), s1.c_str()+ s1.length(), &pend);
    auto result1 = expr1.eval(context);
    CHECK(result1 == json(true));

    std::string s2 = "((@.0 == 1) && (@.1 == 2))";
    auto expr2 = parser.parse(context, s2.c_str(), s2.c_str()+ s2.length(), &pend);
    auto result2 = expr2.eval(context);
    CHECK(result2 == json(true));

    std::string s3 = "(@.0 == 2 && @.1 == 2)";
    auto expr3 = parser.parse(context, s3.c_str(), s3.c_str()+ s3.length(), &pend);
    auto result3 = expr3.eval(context);
    CHECK(result3 == json(false));

    std::string s4 = "((@.0 == 1) && (@.1 == 1))";
    auto expr4 = parser.parse(context, s4.c_str(), s4.c_str()+ s4.length(), &pend);
    auto result4 = expr4.eval(context);
    CHECK(result4 == json(false));
}

TEST_CASE("test_ne")
{
    const char* pend;
    jsonpath_filter_parser<json> parser;

    json context = json::array();
    context.push_back(1);

    std::string s1 = "(3 != 1)";
    auto expr1 = parser.parse(context, s1.c_str(), s1.c_str()+ s1.length(), &pend);
    auto result1 = expr1.eval(context);
    CHECK(result1 == json(true));

    std::string s2 = "(3 != @.length)";
    auto expr2 = parser.parse(context, s2.c_str(), s2.c_str()+ s2.length(), &pend);
    auto result2 = expr2.eval(context);
    CHECK(result2 == json(true));

    std::string s3 = "(1 != 1)";
    auto expr3 = parser.parse(context, s3.c_str(), s3.c_str()+ s3.length(), &pend);
    auto result3 = expr3.eval(context);
    CHECK(result3 == json(false));

    std::string s4 = "(1 != @.length)";
    auto expr4 = parser.parse(context, s4.c_str(), s4.c_str()+ s4.length(), &pend);
    auto result4 = expr4.eval(context);
    CHECK(result4 == json(false));
}

TEST_CASE("test_jsonpath_filter")
{
    const char* pend;
    jsonpath_filter_parser<json> parser;
    json parent = json::array();
    parent.push_back(1);
    parent.push_back(2);

    std::string expr1 = "(1 + 1)";
    auto res1 = parser.parse(parent, expr1.c_str(), expr1.c_str()+ expr1.length(), &pend);
    auto result1 = res1.eval(parent);
    CHECK(json(2) == result1);

    std::string expr2 = "(1 - 1)";
    auto res2 = parser.parse(parent, expr2.c_str(), expr2.c_str()+ expr2.length(), &pend);
    auto result2 = res2.eval(parent);
    CHECK(json(0) == result2);

    std::string expr3 = "(@.length - 1)";
    auto res3 = parser.parse(parent, expr3.c_str(), expr3.c_str()+ expr3.length(), &pend);
    auto result3 = res3.eval(parent);
    CHECK(json(1) == result3);

}

TEST_CASE("test_jsonpath_filter_exclaim")
{
    const char* pend;
    jsonpath_filter_parser<json> parser;
    json parent = json::array();
    parent.push_back(1);
    parent.push_back(2);

    std::string expr1 = "(!(1 + 1))";
    auto res1 = parser.parse(parent, expr1.c_str(), expr1.c_str()+ expr1.length(), &pend);
    auto result1 = res1.eval(parent);
    CHECK(result1 == json(false));

    std::string expr2 = "(!0)";
    auto res2 = parser.parse(parent, expr2.c_str(), expr2.c_str()+ expr2.length(), &pend);
    auto result2= res2.eval(parent);
    CHECK(result2 == json(true));
}


TEST_CASE("test_jsonpath_index_expression")
{
    json root = json::parse(jsonpath_filter_fixture::store_text());
    //std::cout << pretty_print(root) << std::endl;
    //std::cout << "$..book[(@.length-1)]" << std::endl;

    json result = json_query(root,"$..book[(@.length-1)]");

    CHECK(1 == result.size());
    CHECK(root["store"]["book"][3] == result[0]);

    //    std::cout << pretty_print(result) << std::endl;
}

TEST_CASE("test_jsonpath_filter_negative_numbers")
{
    const char* pend;
    jsonpath_filter_parser<json> parser;
    json parent = json::array();
    parent.push_back(1);
    parent.push_back(2);

    std::string expr1 = "(-1 + 1)";
    auto res1 = parser.parse(parent, expr1.c_str(), expr1.c_str()+ expr1.length(), &pend);
    auto result1 = res1.eval(parent);
    CHECK(json(0) == result1);

    std::string expr2 = "(1 + -1)";
    auto res2 = parser.parse(parent, expr2.c_str(), expr2.c_str()+ expr2.length(), &pend);
    auto result2 = res2.eval(parent);
    CHECK(json(0) == result2);

    std::string expr3 = "(-1 - -1)";
    auto res3 = parser.parse(parent, expr3.c_str(), expr3.c_str()+ expr3.length(), &pend);
    auto result3 = res3.eval(parent);
    CHECK(json(0) == result3);

    std::string expr4 = "(-1 - -3)";
    auto res4 = parser.parse(parent, expr4.c_str(), expr4.c_str()+ expr4.length(), &pend);
    auto result4 = res4.eval(parent);
    CHECK(json(2) == result4);

    std::string expr5 = "((-2 < -1) && (-3 > -4))";
    auto res5 = parser.parse(parent, expr5.c_str(), expr5.c_str()+ expr5.length(), &pend);
    auto result5 = res5.eval(parent);
    CHECK(json(true) == result5);

    std::string expr6 = "((-2 < -1) || (-4 > -3))";
    auto res6 = parser.parse(parent, expr6.c_str(), expr6.c_str()+ expr6.length(), &pend);
    auto result6 = res6.eval(parent);
    CHECK(json(true) == result6);

    std::string expr7 = "(-2 < -1 && -3 > -4)";
    auto res7 = parser.parse(parent, expr7.c_str(), expr7.c_str()+ expr7.length(), &pend);
    auto result7 = res7.eval(parent);
    CHECK(json(true) == result7);

    std::string expr8 = "(-2 < -1 || -4 > -3)";
    auto res8 = parser.parse(parent, expr8.c_str(), expr8.c_str()+ expr8.length(), &pend);
    auto result8 = res8.eval(parent);
    CHECK(json(true) == result8);
}

TEST_CASE("test_jsonpath_filter_uni")
{
    const char* pend;
    jsonpath_filter_parser<json> parser;
    json parent = json::array();
    parent.push_back(1);
    parent.push_back(2);

    std::string expr1 = "(0)";
    auto res = parser.parse(parent, expr1.c_str(), expr1.c_str()+ expr1.length(), &pend);
    auto result1 = res.eval(parent);

    //std::cout << (int)result1.data_type() << std::endl;
    CHECK(result1 == json(0));

    CHECK(json(0) == result1);
}

#if !(defined(__GNUC__) && (__GNUC__ == 4 && __GNUC_MINOR__ < 9))
// GCC 4.8 has broken regex support: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=53631
TEST_CASE("test_jsonpath_filter_regex")
{
    const char* pend;
    jsonpath_filter_parser<json> parser;
    json parent = json::array();
    parent.push_back(1);
    parent.push_back(2);

    std::string expr1 = "('today I go' =~ /today.*?/)";
    auto res1 = parser.parse(parent, expr1.c_str(), expr1.c_str()+ expr1.length(), &pend);
    auto result1 = res1.eval(parent);
    CHECK(result1 == json(true));

    std::string expr2 = "('today I go' =~ /Today.*?/)";
    auto res2 = parser.parse(parent, expr2.c_str(), expr2.c_str()+ expr2.length(), &pend);
    auto result2 = res2.eval(parent);
    CHECK(result2 == json(false));

    std::string expr3 = "('today I go' =~ /Today.*?/i)";
    auto res3 = parser.parse(parent, expr3.c_str(), expr3.c_str()+ expr3.length(), &pend);
    auto result3 = res3.eval(parent);
    CHECK(result3 == json(true));
}
#endif

