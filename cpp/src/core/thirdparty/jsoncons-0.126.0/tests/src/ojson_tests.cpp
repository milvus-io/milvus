// Copyright 2013 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>
#include <catch/catch.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <new>

using namespace jsoncons;

TEST_CASE("test_index")
{
    ojson o = ojson::parse(R"(
    {
        "street_number" : "100",
        "street_name" : "Queen St W",
        "city" : "Toronto",
        "country" : "Canada"
    }
    )");

    CHECK("100" == o[0].as<std::string>());
    CHECK("Queen St W" == o[1].as<std::string>());
    CHECK("Toronto" ==o[2].as<std::string>());
    CHECK("Canada" ==o[3].as<std::string>());

    CHECK("100" == o.at(0).as<std::string>());
    CHECK("Queen St W" == o.at(1).as<std::string>());
    CHECK("Toronto" ==o.at(2).as<std::string>());
    CHECK("Canada" ==o.at(3).as<std::string>());
}

TEST_CASE("test_object")
{
    ojson o = ojson::parse(R"(
    {
        "street_number" : "100",
        "street_name" : "Queen St W",
        "city" : "Toronto",
        "country" : "Canada"
    }
    )");

    o.insert_or_assign("postal_code", "M5H 2N2");

    ojson o2 = o;
    CHECK(o == o2);

    ojson o3 = o;
    o3["street_name"] = "Queen St W";
    //CHECK(o == o3);

    //BOOST_CHECK_EQUAL("Queen St W",o["street_name"].as<std::string>());
    //CHECK(2 == o["city"].as<int>());
    //CHECK(4 == o["street_number"].as<int>());

    auto it = o.find("country");
    CHECK_FALSE((it == o.object_range().end()));
    o.insert_or_assign(it,"province","Ontario");

    o.insert_or_assign("unit_type","O");

    o.erase("unit_type");
}

TEST_CASE("test_object_emplace")
{
    ojson o = ojson::parse(R"(
    {
        "street_number" : "100",
        "street_name" : "Queen St W",
        "city" : "Toronto",
        "country" : "Canada"
    }
    )");

    o.try_emplace("postal_code", "M5H 2N2");

    ojson o2 = o;
    CHECK(o == o2);

    ojson o3 = o;
    o3["street_name"] = "Queen St W";
    //CHECK(o == o3);

    //BOOST_CHECK_EQUAL("Queen St W",o["street_name"].as<std::string>());
    //CHECK(2 == o["city"].as<int>());
    //CHECK(4 == o["street_number"].as<int>());

    auto it = o.find("country");
    CHECK_FALSE((it == o.object_range().end()));
    o.try_emplace(it,"province","Ontario");

    o.try_emplace("unit_type","O");

    o.erase("unit_type");
}

