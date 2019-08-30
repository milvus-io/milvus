// Copyright 2013 Daniel Parker
// Distributed under Boost license

#include <string>
#include <jsoncons/json.hpp>
#include <deque>
#include <map>
#include <unordered_map>

using namespace jsoncons;

void array_example1()
{
    {
        std::vector<int> v = {1,2,3,4};
        json a(v);
        std::cout << a << std::endl;
    }
    {
        json j = json::array{1,true,"last"};
        auto d = j.as<std::deque<std::string>>();
        for (auto x : d)
        {
            std::cout << x << std::endl;
        }
    }
    {
        std::map<std::string,int> m = {{"one",1},{"two",2},{"three",3}};
        json j(m);
        std::cout << j << std::endl;
    }
    {
        json j;
        j["one"] = 1;
        j["two"] = 2;
        j["three"] = 3;

        auto um = j.as<std::unordered_map<std::string,int>>();
        for (const auto& x : um)
        {
            std::cout << x.first << "=" << x.second << std::endl;
        }
    }
}

void accessing_a_json_value_as_a_vector()
{
    std::string s = "{\"my-array\" : [1,2,3,4]}";
    json val = json::parse(s);
    std::vector<int> v = val["my-array"].as<std::vector<int>>();
    for (size_t i = 0; i < v.size(); ++i)
    {
        if (i > 0)
        {
            std::cout << ",";
        }
        std::cout << v[i];
    }
    std::cout << std::endl;
}

void construct_json_from_vector()
{
    json root;

    std::vector<json> addresses;
    json address1;
    address1["city"] = "San Francisco";
    address1["state"] = "CA";
    address1["zip"] = "94107";
    address1["country"] = "USA";
    addresses.push_back(address1);

    json address2;
    address2["city"] = "Sunnyvale";
    address2["state"] = "CA";
    address2["zip"] = "94085";
    address2["country"] = "USA";
    addresses.push_back(address2);

    root["addresses"] = addresses;

    std::cout << pretty_print(root) << std::endl;

    std::cout << "size=" << root["addresses"].size() << std::endl;
    for (size_t i = 0; i < root["addresses"].size(); ++i)
    {
        std::cout << root["addresses"][i] << std::endl;
    }
}

void add_element_to_array()
{
    json cities = json::array();  // an empty array
    std::cout << cities << std::endl;  // output is "[]"
    cities.push_back("Toronto");
    cities.push_back("Vancouver");
    cities.insert(cities.array_range().begin(),"Montreal");  // inserts "Montreal" at beginning of array

    std::cout << cities << std::endl;
}

void reserve_array_capacity()
{
    json cities = json::array();
    cities.reserve(10);  // storage is allocated
    std::cout << "capacity=" << cities.capacity() << ", size=" << cities.size() << std::endl;

    cities.push_back("Toronto");
    cities.push_back("Vancouver");
    cities.insert(cities.array_range().begin(),"Montreal");
    std::cout << "capacity=" << cities.capacity() << ", size=" << cities.size() << std::endl;

    std::cout << cities << std::endl;
}

void make_empty_array()
{
    std::cout << "empty array" <<std::endl;
    json a = json::array();
    std::cout << pretty_print(a) << std::endl;
}

void array_range_based_for_loop()
{
    json book1;    
    book1["category"] = "Fiction";
    book1["title"] = "A Wild Sheep Chase: A Novel";
    book1["author"] = "Haruki Murakami";

    json book2;    
    book2["category"] = "History";
    book2["title"] = "Charlie Wilson's War";
    book2["author"] = "George Crile";

    json book3;    
    book3["category"] = "Fiction";
    book3["title"] = "Kafka on the Shore";
    book3["author"] = "Haruki Murakami";

    // Constructing a json array with an initializer-list 
    json booklist = json::array{book1, book2, book3};    

    for (const auto& book: booklist.array_range())
    {
        std::cout << book["title"].as<std::string>() << std::endl;
    } 
}

void make_1_dimensional_array_1()
{
    std::cout << "1 dimensional array 1" <<std::endl;
    json a = json::make_array<1>(10);
    a[1] = 1;
    a[2] = 2;
    std::cout << pretty_print(a) << std::endl;
}

void make_1_dimensional_array_2()
{
    std::cout << "1 dimensional array 2" <<std::endl;
    json a = json::make_array<1>(10,0);
    a[1] = 1;
    a[2] = 2;
    a[3] = json::array();
    std::cout << pretty_print(a) << std::endl;
}

void make_2_dimensional_array()
{
    std::cout << "2 dimensional array" <<std::endl;
    json a = json::make_array<2>(3,4,0);
    a[0][0] = "Tenor";
    a[0][1] = "ATM vol";
    a[0][2] = "25-d-MS";
    a[0][3] = "25-d-RR";
    a[1][0] = "1Y";
    a[1][1] = 0.20;
    a[1][2] = 0.009;
    a[1][3] = -0.006;
    a[2][0] = "2Y";
    a[2][1] = 0.18;
    a[2][2] = 0.009;
    a[2][3] = -0.005;

    std::cout << pretty_print(a) << std::endl;
}

void make_3_dimensional_array()
{
    std::cout << "3 dimensional array" <<std::endl;
    json a = json::make_array<3>(4,3,2,0);

    double val = 1.0;
    for (size_t i = 0; i < a.size(); ++i)
    {
        for (size_t j = 0; j < a[i].size(); ++j)
        {
            for (size_t k = 0; k < a[i][j].size(); ++k)
            {
                a[i][j][k] = val;
                val += 1.0;
            }
        }
    }
    std::cout << pretty_print(a) << std::endl;
}

void array_examples()
{
    std::cout << "\nArray examples\n\n";
    array_example1();

    construct_json_from_vector();
    add_element_to_array();
    reserve_array_capacity();
    accessing_a_json_value_as_a_vector();
    make_empty_array();
    array_range_based_for_loop();
    make_1_dimensional_array_1();
    make_1_dimensional_array_2();
    make_2_dimensional_array();
    make_3_dimensional_array();

    std::cout << std::endl;
}

