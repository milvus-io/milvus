// Copyright 2016 Daniel Parker
// Distributed under Boost license

#include "example_types.hpp"
#include <cassert>
#include <string>
#include <vector>
#include <list>
#include <iomanip>
#include <jsoncons/json.hpp>

using namespace jsoncons;

void book_extensibility_example()
{
    using jsoncons::json;

    ns::book book1{"Haruki Murakami", "Kafka on the Shore", 25.17};

    json j = book1;

    std::cout << "(1) " << std::boolalpha << j.is<ns::book>() << "\n\n";

    std::cout << "(2) " << pretty_print(j) << "\n\n";

    ns::book temp = j.as<ns::book>();
    std::cout << "(3) " << temp.author << "," 
                        << temp.title << "," 
                        << temp.price << "\n\n";

    ns::book book2{"Charles Bukowski", "Women: A Novel", 12.0};

    std::vector<ns::book> book_array{book1, book2};

    json ja = book_array;

    std::cout << "(4) " << std::boolalpha 
                        << ja.is<std::vector<ns::book>>() << "\n\n";

    std::cout << "(5)\n" << pretty_print(ja) << "\n\n";

    auto book_list = ja.as<std::list<ns::book>>();

    std::cout << "(6)" << std::endl;
    for (auto b : book_list)
    {
        std::cout << b.author << ", " 
                  << b.title << ", " 
                  << b.price << std::endl;
    }
}

void book_extensibility_example2()
{
    const std::string s = R"(
    [
        {
            "author" : "Haruki Murakami",
            "title" : "Kafka on the Shore",
            "price" : 25.17
        },
        {
            "author" : "Charles Bukowski",
            "title" : "Pulp",
            "price" : 22.48
        }
    ]
    )";

    std::vector<ns::book> book_list = decode_json<std::vector<ns::book>>(s);

    std::cout << "(1)\n";
    for (const auto& item : book_list)
    {
        std::cout << item.author << ", " 
                  << item.title << ", " 
                  << item.price << "\n";
    }

    std::cout << "\n(2)\n";
    encode_json(book_list, std::cout, indenting::indent);
    std::cout << "\n\n";
}

void reputons_extensibility_example()
{
    ns::reputation_object val("hiking", { ns::reputon{"HikingAsylum.example.com","strong-hiker","Marilyn C",0.90} });

    std::string s;
    encode_json(val, s, indenting::indent);
    std::cout << s << "\n";

    auto val2 = decode_json<ns::reputation_object>(s);

    assert(val2 == val);
}

//own vector will always be of an even length 
struct own_vector : std::vector<int64_t> { using  std::vector<int64_t>::vector; };

namespace jsoncons {

template<class Json>
struct json_type_traits<Json, own_vector> 
{
    static bool is(const Json& j) noexcept
    { 
        return j.is_object() && j.size() % 2 == 0;
    }
    static own_vector as(const Json& j)
    {   
        own_vector v;
        for (auto& item : j.object_range())
        {
            std::string s(item.key());
            v.push_back(std::strtol(s.c_str(),nullptr,10));
            v.push_back(item.value().template as<int64_t>());
        }
        return v;
    }
    static Json to_json(const own_vector& val){
		Json j;
		for(size_t i=0;i<val.size();i+=2){
			j[std::to_string(val[i])] = val[i + 1];
		}
		return j;
	}
};

template <> 
struct is_json_type_traits_declared<own_vector> : public std::true_type 
{}; 
} // jsoncons

void own_vector_extensibility_example()
{
    using jsoncons::json;

    json j = json::object{ {"1",2},{"3",4} };
    assert(j.is<own_vector>());
    auto v = j.as<own_vector>();
    json j2 = v;

    std::cout << j2 << "\n";
}

void type_extensibility_examples()
{
    std::cout << std::setprecision(6);

    std::cout << "\nType extensibility examples\n\n";

    book_extensibility_example();

    own_vector_extensibility_example();

    book_extensibility_example2();

    reputons_extensibility_example();

    std::cout << std::endl;
}
