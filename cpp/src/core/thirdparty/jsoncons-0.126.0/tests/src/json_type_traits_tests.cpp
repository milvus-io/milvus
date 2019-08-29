// Copyright 2013 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif
#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>
#include <catch/catch.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <cstdint>

using namespace jsoncons;
// own vector will always be of an even length 
struct own_vector : std::vector<int64_t> { using  std::vector<int64_t>::vector; };

namespace jsoncons {
template<class Json>
struct json_type_traits<Json, own_vector> {
    static bool is(const Json&) noexcept { return true; }
    static own_vector as(const Json&) { return own_vector(); }
    static Json to_json(const own_vector& val) {
        Json j;
        for (uint64_t i = 0; i<val.size(); i = i + 2) {
            j[std::to_string(val[i])] = val[i + 1];
        }
        return j;
    }
};
}

TEST_CASE("test_trait_type_erasure")
{
    json::object o;

    json val;

    val = o;

    val.insert_or_assign("A",o);
}

TEST_CASE("test_assign_non_const_cstring")
{
    json root;

    const char* p = "A string";
    char* q = const_cast<char*>(p);

    root["Test"] = q;
}

TEST_CASE("test_uint8_t")
{
    uint8_t x = 10;

    json o;
    o["u"] = x;

    CHECK(o["u"].is_number());

    uint8_t y = o["u"].as<uint8_t>();

    CHECK(y == 10);
}

TEST_CASE("test_float_assignment")
{
    float x = 10.5;

    json o;
    o["float"] = x;

    CHECK(o["float"].is_number());

    float y = o["float"].as<float>();

    CHECK(10.5 == Approx(y).epsilon(0.00001));
}

TEST_CASE("test_float")
{
    float x = 10.5;

    json o(x);

    CHECK(o.is<float>());

    float y = o.as<float>();

    CHECK(10.5 == Approx(y).epsilon(0.00001));
}

TEST_CASE("test_unsupported_type")
{
    json o;

    //o["u"] = Info; 
    // compile error
}

TEST_CASE("test_as_json_value")
{
    json a;

    a["first"] = "first"; 
    a["second"] = "second"; 

    CHECK(true == a.is<json>());
    
    json b = a.as<json>();
    CHECK("first" == b["first"].as<std::string>());
    CHECK("second" == b["second"].as<std::string>());
}

TEST_CASE("test_byte_string_as_vector")
{
    json a(byte_string{'H','e','l','l','o'});

    REQUIRE(a.is_byte_string());

    auto bs = a.as<byte_string>();

    REQUIRE(5 == bs.size());
    CHECK('H' == bs[0]);
    CHECK('e' == bs[1]);
    CHECK('l' == bs[2]);
    CHECK('l' == bs[3]);
    CHECK('o' == bs[4]);
}
/*
TEST_CASE("test_own_vector")
{
    jsoncons::json j = own_vector({0,9,8,7});
    std::cout << j;
}
*/

