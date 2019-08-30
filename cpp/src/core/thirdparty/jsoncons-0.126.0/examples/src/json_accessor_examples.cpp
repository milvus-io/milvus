// Copyright 2013 Daniel Parker
// Distributed under Boost license

#include <stdexcept>
#include <string>
#include <jsoncons/json.hpp>
#include <fstream>

using namespace jsoncons;

void is_as_examples()
{
    json j = json::parse(R"(
    {
        "k1" : 2147483647,
        "k2" : 2147483648,
        "k3" : -10,
        "k4" : 10.5,
        "k5" : true,
        "k6" : "10.5"
    }
    )");

    std::cout << std::boolalpha << "(1) " << j["k1"].is<int32_t>() << '\n';
    std::cout << std::boolalpha << "(2) " << j["k2"].is<int32_t>() << '\n';
    std::cout << std::boolalpha << "(3) " << j["k2"].is<long long>() << '\n';
    std::cout << std::boolalpha << "(4) " << j["k3"].is<signed char>() << '\n';
    std::cout << std::boolalpha << "(5) " << j["k3"].is<uint32_t>() << '\n';
    std::cout << std::boolalpha << "(6) " << j["k4"].is<int32_t>() << '\n';
    std::cout << std::boolalpha << "(7) " << j["k4"].is<double>() << '\n';
    std::cout << std::boolalpha << "(8) " << j["k5"].is<int>() << '\n';
    std::cout << std::boolalpha << "(9) " << j["k5"].is<bool>() << '\n';
    std::cout << std::boolalpha << "(10) " << j["k6"].is<double>() << '\n';
    std::cout << '\n';
    std::cout << "(1) " << j["k1"].as<int32_t>() << '\n';
    std::cout << "(2) " << j["k2"].as<int32_t>() << '\n';
    std::cout << "(3) " << j["k2"].as<long long>() << '\n';
    std::cout << "(4) " << j["k3"].as<signed char>() << '\n';
    std::cout << "(5) " << j["k3"].as<uint32_t>() << '\n';
    std::cout << "(6) " << j["k4"].as<int32_t>() << '\n';
    std::cout << "(7) " << j["k4"].as<double>() << '\n';
    std::cout << std::boolalpha << "(8) " << j["k5"].as<int>() << '\n';
    std::cout << std::boolalpha << "(9) " << j["k5"].as<bool>() << '\n';
    std::cout << "(10) " << j["k6"].as<double>() << '\n';
}

void byte_string_from_initializer_list()
{
    json j(byte_string({'H','e','l','l','o'}));
    byte_string bs = j.as<byte_string>();

    std::cout << "(1) "<< bs << "\n\n";

    std::cout << "(2) ";
    for (auto b : bs)
    {
        std::cout << (char)b;
    }
    std::cout << "\n\n";

    std::cout << "(3) " << j << std::endl;
}

void byte_string_from_char_array()
{
    json j(byte_string("Hello"));
    byte_string bs = j.as<byte_string>();

    std::cout << "(1) "<< bs << "\n\n";

    std::cout << "(2) ";
    for (auto b : bs)
    {
        std::cout << (char)b;
    }
    std::cout << "\n\n";

    std::cout << "(3) " << j << std::endl;
}

void introspection_example()
{
    std::string path = "./input/books.json"; 
    std::fstream is(path);
    if (!is)
    {
        std::cout << "Cannot open " << path << std::endl;
        return;
    }
    json val = json::parse(is);
    std::cout << std::boolalpha;
    std::cout << "Is this an object? " << val.is<json::object>() << ", or an array? " << val.is<json::array>() << std::endl;

    if (val.is<json::array>())
    {
        for (size_t i = 0; i < val.size(); ++i)
        {
            json& elem = val[i];
            std::cout << "Is element " << i << " an object? " << elem.is<json::object>() << std::endl;
            if (elem.is<json::object>())
            {
                for (auto it = elem.object_range().begin(); it != elem.object_range().end(); ++it){
                    std::cout << "Is member " << it->key() << " a string? " << it->value().is<std::string>() << ", or a double? " << it->value().is<double>() << ", or perhaps an int? " << it->value().is<int>() << std::endl;

                }
            }
        }
    }
}

void json_accessor_examples()
{
    is_as_examples();

    introspection_example();

    byte_string_from_initializer_list();

    byte_string_from_char_array();
}


