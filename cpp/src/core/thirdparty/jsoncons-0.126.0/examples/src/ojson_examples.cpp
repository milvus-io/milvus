// Copyright 2013 Daniel Parker
// Distributed under Boost license

#include <string>
#include <jsoncons/json.hpp>

using namespace jsoncons;

void ojson_examples()
{
    std::cout << "\nojson examples\n\n";

    ojson o = ojson::parse(R"(
    {
        "street_number" : "100",
        "street_name" : "Queen St W",
        "city" : "Toronto",
        "country" : "Canada"
    }
    )");

    std::cout << pretty_print(o) << std::endl;

    o.insert_or_assign("postal_code", "M5H 2N2");
    std::cout << pretty_print(o) << std::endl;

    ojson o2 = o;

    ojson o3 = o;
    o3["street_name"] = "Queen St W";

    auto it = o.find("country");
    o.insert_or_assign(it,"province","Ontario");

    o.insert_or_assign("unit_type","O");

    std::cout << pretty_print(o) << std::endl;

    o.erase("unit_type");

    std::cout << pretty_print(o) << std::endl;

    std::cout << std::endl;
}

