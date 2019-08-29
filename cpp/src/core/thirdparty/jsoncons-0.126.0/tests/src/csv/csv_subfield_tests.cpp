// Copyright 2013 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif
//#include <jsoncons_ext/csv/csv_options.hpp>
#include <jsoncons_ext/csv/csv_reader.hpp>
#include <jsoncons_ext/csv/csv_encoder.hpp>
#include <jsoncons/json_reader.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <iostream>
#include <catch/catch.hpp>

using namespace jsoncons;
using namespace jsoncons::literals;

TEST_CASE("test_n_objects")
{
    const std::string s = R"(calculationPeriodCenters,paymentCenters,resetCenters
NY;LON,TOR,LON
NY,LON,TOR;LON
"NY";"LON","TOR","LON"
"NY","LON","TOR";"LON"
)";
    csv::csv_options options;
    options.assume_header(true)
           .subfield_delimiter(';');

    json expected = R"(
[
    {
        "calculationPeriodCenters": ["NY","LON"],
        "paymentCenters": "TOR",
        "resetCenters": "LON"
    },
    {
        "calculationPeriodCenters": "NY",
        "paymentCenters": "LON",
        "resetCenters": ["TOR","LON"]
    },
    {
        "calculationPeriodCenters": ["NY","LON"],
        "paymentCenters": "TOR",
        "resetCenters": "LON"
    },
    {
        "calculationPeriodCenters": "NY",
        "paymentCenters": "LON",
        "resetCenters": ["TOR","LON"]
    }
]
    )"_json;

    try
    {
        json j = csv::decode_csv<json>(s,options);
        CHECK(j == expected);
        //std::cout << pretty_print(j) << std::endl;
    }
    catch (const std::exception& e)
    {
        std::cerr << e.what() << std::endl;
    }
}

TEST_CASE("test_n_rows")
{
    const std::string s = R"(calculationPeriodCenters,paymentCenters,resetCenters
NY;LON,TOR,LON
NY,LON,TOR;LON
"NY";"LON","TOR","LON"
"NY","LON","TOR";"LON"
)";
    csv::csv_options options;
    options.mapping(csv::mapping_type::n_rows)
           .subfield_delimiter(';');

    json expected = R"(
[
    ["calculationPeriodCenters","paymentCenters","resetCenters"],
    [
        ["NY","LON"],"TOR","LON"
    ],
    ["NY","LON",
        ["TOR","LON"]
    ],
    [
        ["NY","LON"],"TOR","LON"
    ],
    ["NY","LON",
        ["TOR","LON"]
    ]
]
    )"_json;

    try
    {
        json j = csv::decode_csv<json>(s,options);
        CHECK(j == expected);
        //std::cout << pretty_print(j) << std::endl;
    }
    catch (const std::exception& e)
    {
        std::cerr << e.what() << std::endl;
    }
}

TEST_CASE("test_m_columns")
{
    const std::string s = R"(calculationPeriodCenters,paymentCenters,resetCenters
NY;LON,TOR,LON
NY,LON,TOR;LON
"NY";"LON","TOR","LON"
"NY","LON","TOR";"LON"
)";
    csv::csv_options options;
    options.assume_header(true)
           .mapping(csv::mapping_type::m_columns)
           .subfield_delimiter(';');

    json expected = R"(
{
    "calculationPeriodCenters": [
        ["NY","LON"],"NY",
        ["NY","LON"],"NY"
    ],
    "paymentCenters": ["TOR","LON","TOR","LON"],
    "resetCenters": ["LON",
        ["TOR","LON"],"LON",
        ["TOR","LON"]
    ]
}
    )"_json;

    try
    {
        json j = csv::decode_csv<json>(s,options);
        CHECK(j == expected);
        //std::cout << pretty_print(j) << std::endl;
    }
    catch (const std::exception& e)
    {
        std::cerr << e.what() << std::endl;
    }
}

