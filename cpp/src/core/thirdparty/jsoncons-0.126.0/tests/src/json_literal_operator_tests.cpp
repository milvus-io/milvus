// Copyright 2013 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>
#include <catch/catch.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <map>

using namespace jsoncons;
using namespace jsoncons::literals;

TEST_CASE("json_literal_operator_test1")
{
    json j = R"(
{
    "StartDate" : "2017-03-01",
    "MaturityDate" : "2020-12-30",
    "Currency" : "USD",
    "DiscountCurve" : "USD-LIBOR",
    "FixedRate" : 0.01,
    "PayFrequency" : "6M",
    "DayCountBasis" : "ACT/360",
    "Notional" : 1000000          
}
)"_json;

    CHECK(j["Currency"] == "USD");

}

TEST_CASE("ojson_literal_operator_test1")
{
    ojson j = R"(
{
    "StartDate" : "2017-03-01",
    "MaturityDate" : "2020-12-30",
    "Currency" : "USD",
    "DiscountCurve" : "USD-LIBOR",
    "FixedRate" : 0.01,
    "PayFrequency" : "6M",
    "DayCountBasis" : "ACT/360",
    "Notional" : 1000000          
}
)"_ojson;

    CHECK(j["Currency"] == "USD");

}

TEST_CASE("json_literal_operator_test2")
{
    wjson j = LR"(
{
    "StartDate" : "2017-03-01",
    "MaturityDate" : "2020-12-30",
    "Currency" : "USD",
    "DiscountCurve" : "USD-LIBOR",
    "FixedRate" : 0.01,
    "PayFrequency" : "6M",
    "DayCountBasis" : "ACT/360",
    "Notional" : 1000000          
}
)"_json;

    CHECK(j[L"Currency"] == L"USD");

}

TEST_CASE("ojson_literal_operator_test2")
{
    wojson j = LR"(
{
    "StartDate" : "2017-03-01",
    "MaturityDate" : "2020-12-30",
    "Currency" : "USD",
    "DiscountCurve" : "USD-LIBOR",
    "FixedRate" : 0.01,
    "PayFrequency" : "6M",
    "DayCountBasis" : "ACT/360",
    "Notional" : 1000000          
}
)"_ojson;

    CHECK(j[L"Currency"] == L"USD");

}

