// Copyright 2017 Daniel Parker
// Distributed under Boost license

#include "example_types.hpp"
#include <jsoncons/json.hpp>
#include <jsoncons_ext/ubjson/ubjson.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>
#include <jsoncons_ext/jsonpath/json_query.hpp>
#include <string>
#include <iomanip>
#include <cassert>

using namespace jsoncons;

void to_from_ubjson_using_basic_json()
{
    ojson j1 = ojson::parse(R"(
    {
       "application": "hiking",
       "reputons": [
       {
           "rater": "HikingAsylum.example.com",
           "assertion": "strong-hiker",
           "rated": "Marilyn C",
           "rating": 0.90
         }
       ]
    }
    )");

    // Encode a basic_json value to UBJSON
    std::vector<uint8_t> data;
    ubjson::encode_ubjson(j1, data);

    // Decode UBJSON to a basic_json value
    ojson j2 = ubjson::decode_ubjson<ojson>(data);
    std::cout << "(1)\n" << pretty_print(j2) << "\n\n";

    // Accessing the data items 

    const ojson& reputons = j2["reputons"];

    std::cout << "(2)\n";
    for (auto element : reputons.array_range())
    {
        std::cout << element.at("rated").as<std::string>() << ", ";
        std::cout << element.at("rating").as<double>() << "\n";
    }
    std::cout << std::endl;

    // Get a UBJSON value for a nested data item with jsonpointer
    std::error_code ec;
    auto const& rated = jsonpointer::get(j2, "/reputons/0/rated", ec);
    if (!ec)
    {
        std::cout << "(3) " << rated.as_string() << "\n";
    }

    std::cout << std::endl;
}

void to_from_ubjson_using_example_type()
{
    ns::reputation_object val("hiking", { ns::reputon{"HikingAsylum.example.com","strong-hiker","Marilyn C",0.90} });

    // Encode a ns::reputation_object value to UBJSON
    std::vector<uint8_t> data;
    ubjson::encode_ubjson(val, data);

    // Decode UBJSON to a ns::reputation_object value
    ns::reputation_object val2 = ubjson::decode_ubjson<ns::reputation_object>(data);

    assert(val2 == val);
}

void ubjson_examples()
{
    std::cout << "\nubjson examples\n\n";
    to_from_ubjson_using_basic_json();
    to_from_ubjson_using_example_type();

    std::cout << std::endl;
}

