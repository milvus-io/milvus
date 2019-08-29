// Copyright 2013 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <string>
#include <catch/catch.hpp>

using namespace jsoncons;

#if 0

BOOST_AUTO_TEST_CASE( test_surrogate_pair )
{
    std::string input = "[\"\\u8A73\\u7D30\\u95B2\\u89A7\\uD800\\uDC01\\u4E00\"]";
    json value = json::parse(input);
    json_options options;
    options.escape_all_non_ascii(true);
    std::string output;
    value.dump(output,options);

    CHECK(input == output);
}

TEST_CASE("test_wide_surrogate_pair")
{
    std::wstring input = L"[\"\\u8A73\\u7D30\\u95B2\\u89A7\\uD800\\uDC01\\u4E00\"]";
    wjson value = wjson::parse(input);
    wjson_options options;
    options.escape_all_non_ascii(true);
    std::wstring output;
    value.dump(output,options);

    CHECK(input == output);
}

BOOST_AUTO_TEST_CASE( test1 )
{
    std::istringstream is("{\"unicode_string_1\":\"\\uD800\\uDC00\"}");

    json root = json::parse(is);
    CHECK(root.is_object());
    CHECK(root.is<json::object>());

    root["double_1"] = 10.0;

    json double_1 = root["double_1"];

    BOOST_CHECK_CLOSE(double_1.as<double>(), 10.0, 0.000001);

    BOOST_CHECK_CLOSE(double_1.as<double>(), 10.0, 0.000001);

    json copy(root);
}
#endif

