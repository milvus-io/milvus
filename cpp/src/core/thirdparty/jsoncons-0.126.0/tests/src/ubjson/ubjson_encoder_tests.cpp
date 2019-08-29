// Copyright 2016 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif
#include <jsoncons/json.hpp>
#include <jsoncons_ext/ubjson/ubjson.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <limits>
#include <catch/catch.hpp>

using namespace jsoncons;
using namespace jsoncons::ubjson;

TEST_CASE("serialize array to ubjson")
{
    std::vector<uint8_t> v;
    ubjson::ubjson_bytes_encoder encoder(v);
    encoder.begin_array(3);
    encoder.bool_value(true);
    encoder.bool_value(false);
    encoder.null_value();
    encoder.end_array();
    encoder.flush();

    try
    {
        json result = decode_ubjson<json>(v);
        std::cout << result << std::endl;
    }
    catch (const std::exception& e)
    {
        std::cout << e.what() << std::endl;
    }
} 

TEST_CASE("Too many and too few items in UBJSON object or array")
{
    std::error_code ec{};
    std::vector<uint8_t> v;
    ubjson::ubjson_bytes_encoder encoder(v);

    SECTION("Too many items in array")
    {
        CHECK(encoder.begin_array(3));
        CHECK(encoder.bool_value(true));
        CHECK(encoder.bool_value(false));
        CHECK(encoder.null_value());
        CHECK(encoder.begin_array(2));
        CHECK(encoder.string_value("cat"));
        CHECK(encoder.string_value("feline"));
        CHECK(encoder.end_array());
        REQUIRE_THROWS_WITH(encoder.end_array(), ubjson_error_category_impl().message((int)ubjson_errc::too_many_items).c_str());
        encoder.flush();
    }
    SECTION("Too few items in array")
    {
        CHECK(encoder.begin_array(5));
        CHECK(encoder.bool_value(true));
        CHECK(encoder.bool_value(false));
        CHECK(encoder.null_value());
        CHECK(encoder.begin_array(2));
        CHECK(encoder.string_value("cat"));
        CHECK(encoder.string_value("feline"));
        CHECK(encoder.end_array());
        REQUIRE_THROWS_WITH(encoder.end_array(), ubjson_error_category_impl().message((int)ubjson_errc::too_few_items).c_str());
        encoder.flush();
    }
    SECTION("Too many items in object")
    {
        CHECK(encoder.begin_object(3));
        CHECK(encoder.name("a"));
        CHECK(encoder.bool_value(true));
        CHECK(encoder.name("b"));
        CHECK(encoder.bool_value(false));
        CHECK(encoder.name("c"));
        CHECK(encoder.null_value());
        CHECK(encoder.name("d"));
        CHECK(encoder.begin_array(2));
        CHECK(encoder.string_value("cat"));
        CHECK(encoder.string_value("feline"));
        CHECK(encoder.end_array());
        REQUIRE_THROWS_WITH(encoder.end_object(), ubjson_error_category_impl().message((int)ubjson_errc::too_many_items).c_str());
        encoder.flush();
    }
    SECTION("Too few items in object")
    {
        CHECK(encoder.begin_object(5));
        CHECK(encoder.name("a"));
        CHECK(encoder.bool_value(true));
        CHECK(encoder.name("b"));
        CHECK(encoder.bool_value(false));
        CHECK(encoder.name("c"));
        CHECK(encoder.null_value());
        CHECK(encoder.name("d"));
        CHECK(encoder.begin_array(2));
        CHECK(encoder.string_value("cat"));
        CHECK(encoder.string_value("feline"));
        CHECK(encoder.end_array());
        REQUIRE_THROWS_WITH(encoder.end_object(), ubjson_error_category_impl().message((int)ubjson_errc::too_few_items).c_str());
        encoder.flush();
    }
}
