// Copyright 2019 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <catch/catch.hpp>

#if defined(JSONCONS_HAS_STRING_VIEW)

#include <string_view>

using namespace jsoncons;

TEST_CASE("string_view tests")
{
    std::cout << "string_view tests\n";
    json j = json::parse(R"(
    {
        "a" : "2",
        "c" : [4,5,6]
    }
    )");

    auto s = j["a"].as<std::string_view>();
    CHECK(bool(s == "2"));
}

#endif

