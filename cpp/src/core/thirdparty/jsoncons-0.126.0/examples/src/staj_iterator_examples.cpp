// Copyright 2013 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons/json_cursor.hpp>
#include <jsoncons/staj_iterator.hpp>
#include <string>
#include <sstream>

using namespace jsoncons;

// Example JSON text
const std::string array_example = R"(
[ 
  { 
      "employeeNo" : "101",
      "name" : "Tommy Cochrane",
      "title" : "Supervisor"
  },
  { 
      "employeeNo" : "102",
      "name" : "Bill Skeleton",
      "title" : "Line manager"
  }
]
)";

const std::string object_example = R"(
{
   "application": "hiking",
   "reputons": [
   {
       "rater": "HikingAsylum.array_example.com",
       "assertion": "strong-hiker",
       "rated": "Marilyn C",
       "rating": 0.90
     }
   ]
}
)";

void staj_array_iterator_example()
{
    std::istringstream is(array_example);

    json_cursor reader(is);

    staj_array_iterator<json> it(reader);

    for (const auto& j : it)
    {
        std::cout << pretty_print(j) << "\n";
    }
    std::cout << "\n\n";
}

struct employee
{
    std::string employeeNo;
    std::string name;
    std::string title;
};

namespace jsoncons
{
    template<class Json>
    struct json_type_traits<Json, employee>
    {
        static bool is(const Json& j) noexcept
        {
            return j.is_object() && j.contains("employeeNo") && j.contains("name") && j.contains("title");
        }
        static employee as(const Json& j)
        {
            employee val;
            val.employeeNo = j["employeeNo"].template as<std::string>();
            val.name = j["name"].template as<std::string>();
            val.title = j["title"].template as<std::string>();
            return val;
        }
        static Json to_json(const employee& val)
        {
            Json j;
            j["employeeNo"] = val.employeeNo;
            j["name"] = val.name;
            j["title"] = val.title;
            return j;
        }
    };
}

void staj_array_iterator_example2()
{
    std::istringstream is(array_example);

    json_cursor reader(is);

    staj_array_iterator<employee> it(reader);

    for (const auto& val : it)
    {
        std::cout << val.employeeNo << ", " << val.name << ", " << val.title << "\n";
    }
    std::cout << "\n\n";
}

void staj_object_iterator_example()
{

    std::istringstream is(object_example);

    json_cursor reader(is);

    staj_object_iterator<json> it(reader);

    for (const auto& kv : it)
    {
        std::cout << kv.first << ":\n" << pretty_print(kv.second) << "\n";
    }
    std::cout << "\n\n";
}

void staj_iterator_examples()
{
    std::cout << "\nstaj_iterator examples\n\n";

    staj_array_iterator_example();

    staj_array_iterator_example2();

    staj_object_iterator_example();

    std::cout << "\n";
}

