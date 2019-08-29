// Copyright 2018 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>
#include <catch/catch.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <map>

TEST_CASE("json::as<jsoncons::string_view>()")
{
    std::string s1("Short");
    jsoncons::json j1(s1);
    CHECK(j1.as<jsoncons::string_view>() == jsoncons::string_view(s1));

    std::string s2("String to long for short string");
    jsoncons::json j2(s2);
    CHECK(j2.as<jsoncons::string_view>() == jsoncons::string_view(s2));
}

TEST_CASE("json::as<jsoncons::bignum>()")
{
    SECTION("from integer")
    {
        jsoncons::json j(-1000);
        CHECK(bool(j.as<jsoncons::bignum>() == jsoncons::bignum(-1000)));
    }
    SECTION("from unsigned integer")
    {
        jsoncons::json j(1000u);
        CHECK(bool(j.as<jsoncons::bignum>() == jsoncons::bignum(1000u)));
    }
    SECTION("from double")
    {
        jsoncons::json j(1000.0);
        CHECK(bool(j.as<jsoncons::bignum>() == jsoncons::bignum(1000.0)));
    }
    SECTION("from bignum")
    {
        std::string s = "-18446744073709551617";
        jsoncons::json j(s,  jsoncons::semantic_tag::bigint);
        CHECK(bool(j.as<jsoncons::bignum>() == jsoncons::bignum(s)));
    }
}

#if (defined(__GNUC__) || defined(__clang__)) && (!defined(__STRICT_ANSI__) && defined(_GLIBCXX_USE_INT128))
TEST_CASE("json::as<__int128>()")
{
    std::string s = "-18446744073709551617";

    jsoncons::detail::to_integer_result<__int128> result = jsoncons::detail::to_integer<__int128>(s.data(),s.size());
    REQUIRE(result.ec == jsoncons::detail::to_integer_errc());

    jsoncons::json j(s);

    __int128 val = j.as<__int128>();
    CHECK(result.value == val);
}

TEST_CASE("json::as<unsigned __int128>()")
{
    std::string s = "18446744073709551616";

    jsoncons::detail::to_integer_result<unsigned __int128> result = jsoncons::detail::to_integer<unsigned __int128>(s.data(),s.size());
    REQUIRE(result.ec == jsoncons::detail::to_integer_errc());

    jsoncons::json j(s);

    unsigned __int128 val = j.as<unsigned __int128>();
    CHECK(result.value == val);
}
#endif
