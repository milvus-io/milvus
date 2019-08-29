// Copyright 2017 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

using namespace jsoncons;

void jsonpointer_select_RFC6901()
{
    // Example from RFC 6901
    auto j = json::parse(R"(
       {
          "foo": ["bar", "baz"],
          "": 0,
          "a/b": 1,
          "c%d": 2,
          "e^f": 3,
          "g|h": 4,
          "i\\j": 5,
          "k\"l": 6,
          " ": 7,
          "m~n": 8
       }
    )");

    try
    {
        const json& result1 = jsonpointer::get(j, "");
        std::cout << "(1) " << result1 << std::endl;
        const json& result2 = jsonpointer::get(j, "/foo");
        std::cout << "(2) " << result2 << std::endl;
        const json& result3 = jsonpointer::get(j, "/foo/0");
        std::cout << "(3) " << result3 << std::endl;
        const json& result4 = jsonpointer::get(j, "/");
        std::cout << "(4) " << result4 << std::endl;
        const json& result5 = jsonpointer::get(j, "/a~1b");
        std::cout << "(5) " << result5 << std::endl;
        const json& result6 = jsonpointer::get(j, "/c%d");
        std::cout << "(6) " << result6 << std::endl;
        const json& result7 = jsonpointer::get(j, "/e^f");
        std::cout << "(7) " << result7 << std::endl;
        const json& result8 = jsonpointer::get(j, "/g|h");
        std::cout << "(8) " << result8 << std::endl;
        const json& result9 = jsonpointer::get(j, "/i\\j");
        std::cout << "(9) " << result9 << std::endl;
        const json& result10 = jsonpointer::get(j, "/k\"l");
        std::cout << "(10) " << result10 << std::endl;
        const json& result11 = jsonpointer::get(j, "/ ");
        std::cout << "(11) " << result11 << std::endl;
        const json& result12 = jsonpointer::get(j, "/m~0n");
        std::cout << "(12) " << result12 << std::endl;
    }
    catch (const jsonpointer::jsonpointer_error& e)
    {
        std::cerr << e.what() << std::endl;
    }
}

void jsonpointer_contains()
{
    // Example from RFC 6901
    auto j = json::parse(R"(
       {
          "foo": ["bar", "baz"],
          "": 0,
          "a/b": 1,
          "c%d": 2,
          "e^f": 3,
          "g|h": 4,
          "i\\j": 5,
          "k\"l": 6,
          " ": 7,
          "m~n": 8
       }
    )");

    std::cout << "(1) " << jsonpointer::contains(j, "/foo/0") << std::endl;
    std::cout << "(2) " << jsonpointer::contains(j, "e^g") << std::endl;
}

void jsonpointer_select_author()
{
    auto j = json::parse(R"(
    [
      { "category": "reference",
        "author": "Nigel Rees",
        "title": "Sayings of the Century",
        "price": 8.95
      },
      { "category": "fiction",
        "author": "Evelyn Waugh",
        "title": "Sword of Honour",
        "price": 12.99
      }
    ]
    )");

    // Using exceptions to report errors
    try
    {
        json result = jsonpointer::get(j, "/1/author");
        std::cout << "(1) " << result << std::endl;
    }
    catch (const jsonpointer::jsonpointer_error& e)
    {
        std::cout << e.what() << std::endl;
    }

    // Using error codes to report errors
    std::error_code ec;
    const json& result = jsonpointer::get(j, "/0/title", ec);

    if (ec)
    {
        std::cout << ec.message() << std::endl;
    }
    else
    {
        std::cout << "(2) " << result << std::endl;
    }
}

void jsonpointer_add_member_to_object()
{
    auto target = json::parse(R"(
        { "foo": "bar"}
    )");

    std::error_code ec;
    jsonpointer::insert(target, "/baz", json("qux"), ec);
    if (ec)
    {
        std::cout << ec.message() << std::endl;
    }
    else
    {
        std::cout << target << std::endl;
    }
}

void jsonpointer_add_element_to_array()
{
    auto target = json::parse(R"(
        { "foo": [ "bar", "baz" ] }
    )");

    std::error_code ec;
    jsonpointer::insert(target, "/foo/1", json("qux"), ec);
    if (ec)
    {
        std::cout << ec.message() << std::endl;
    }
    else
    {
        std::cout << target << std::endl;
    }
}

void jsonpointer_add_element_to_end_array()
{
    auto target = json::parse(R"(
        { "foo": [ "bar", "baz" ] }
    )");

    std::error_code ec;
    jsonpointer::insert(target, "/foo/-", json("qux"), ec);
    if (ec)
    {
        std::cout << ec.message() << std::endl;
    }
    else
    {
        std::cout << target << std::endl;
    }
}

void jsonpointer_insert_name_exists()
{
    auto target = json::parse(R"(
        { "foo": "bar", "baz" : "abc"}
    )");

    std::error_code ec;
    jsonpointer::insert(target, "/baz", json("qux"), ec);
    if (ec)
    {
        std::cout << ec.message() << std::endl;
    }
    else
    {
        std::cout << target << std::endl;
    }
}

void jsonpointer_add_element_outside_range()
{
    auto target = json::parse(R"(
    { "foo": [ "bar", "baz" ] }
    )");

    std::error_code ec;
    jsonpointer::insert_or_assign(target, "/foo/3", json("qux"), ec);
    if (ec)
    {
        std::cout << ec.message() << std::endl;
    }
    else
    {
        std::cout << target << std::endl;
    }
}

void jsonpointer_insert_or_assign_name_exists()
{
    auto target = json::parse(R"(
        { "foo": "bar", "baz" : "abc"}
    )");

    std::error_code ec;
    jsonpointer::insert_or_assign(target, "/baz", json("qux"), ec);
    if (ec)
    {
        std::cout << ec.message() << std::endl;
    }
    else
    {
        std::cout << target << std::endl;
    }
}

void jsonpointer_remove_object_member()
{
    auto target = json::parse(R"(
        { "foo": "bar", "baz" : "qux"}
    )");

    std::error_code ec;
    jsonpointer::remove(target, "/baz", ec);
    if (ec)
    {
        std::cout << ec.message() << std::endl;
    }
    else
    {
        std::cout << target << std::endl;
    }
}

void jsonpointer_remove_array_element()
{
    auto target = json::parse(R"(
        { "foo": [ "bar", "qux", "baz" ] }
    )");

    std::error_code ec;
    jsonpointer::remove(target, "/foo/1", ec);
    if (ec)
    {
        std::cout << ec.message() << std::endl;
    }
    else
    {
        std::cout << target << std::endl;
    }
}

void jsonpointer_replace_object_value()
{
    auto target = json::parse(R"(
        {
          "baz": "qux",
          "foo": "bar"
        }
    )");

    std::error_code ec;
    jsonpointer::replace(target, "/baz", json("boo"), ec);
    if (ec)
    {
        std::cout << ec.message() << std::endl;
    }
    else
    {
        std::cout << target << std::endl;
    }
}

void jsonpointer_replace_array_value()
{
    auto target = json::parse(R"(
        { "foo": [ "bar", "baz" ] }
    )");

    std::error_code ec;
    jsonpointer::replace(target, "/foo/1", json("qux"), ec);
    if (ec)
    {
        std::cout << ec.message() << std::endl;
    }
    else
    {
        std::cout << pretty_print(target) << std::endl;
    }
}

void jsonpointer_error_example()
{
    auto j = json::parse(R"(
    [
      { "category": "reference",
        "author": "Nigel Rees",
        "title": "Sayings of the Century",
        "price": 8.95
      },
      { "category": "fiction",
        "author": "Evelyn Waugh",
        "title": "Sword of Honour",
        "price": 12.99
      }
    ]
    )");

    try
    {
        json result = jsonpointer::get(j, "/1/isbn");
        std::cout << "succeeded?" << std::endl;
        std::cout << result << std::endl;
    }
    catch (const jsonpointer::jsonpointer_error& e)
    {
        std::cout << "Caught jsonpointer_error with category " << e.code().category().name() 
                  << ", code " << e.code().value() 
                  << " and message \"" << e.what() << "\"" << std::endl;
    }
}

void jsonpointer_get_examples()
{
    {
        json j = json::array{"baz","foo"};
        json& item = jsonpointer::get(j,"/0");
        std::cout << "(1) " << item << std::endl;

        //std::vector<uint8_t> u;
        //cbor::encode_cbor(j,u);
        //for (auto c : u)
        //{
        //    std::cout << "0x" << std::hex << (int)c << ",";
        //}
        //std::cout << std::endl;
    }
    {
        const json j = json::array{"baz","foo"};
        const json& item = jsonpointer::get(j,"/1");
        std::cout << "(2) " << item << std::endl;
    }
    {
        json j = json::array{"baz","foo"};

        std::error_code ec;
        json& item = jsonpointer::get(j,"/1",ec);
        std::cout << "(4) " << item << std::endl;
    }
    {
        const json j = json::array{"baz","foo"};

        std::error_code ec;
        const json& item = jsonpointer::get(j,"/0",ec);
        std::cout << "(5) " << item << std::endl;
    }
}

void jsonpointer_address_example()
{
    auto j = json::parse(R"(
       {
          "a/b": ["bar", "baz"],
          "m~n": ["foo", "qux"]
       }
    )");

    jsonpointer::address addr;
    addr /= "m~n";
    addr /= "1";

    std::cout << "(1) " << addr << "\n\n";

    std::cout << "(2)\n";
    for (const auto& item : addr)
    {
        std::cout << item << "\n";
    }
    std::cout << "\n";

    json item = jsonpointer::get(j, addr);
    std::cout << "(3) " << item << "\n";
}

void jsonpointer_address_iterator_example()
{
    jsonpointer::address addr("/store/book/1/author");

    std::cout << "(1) " << addr << "\n\n";

    std::cout << "(2)\n";
    for (const auto& token : addr)
    {
        std::cout << token << "\n";
    }

    std::cout << "\n";
}

void jsonpointer_address_append_tokens()
{
    jsonpointer::address addr;

    addr /= "a/b";
    addr /= "";
    addr /= "m~n";

    std::cout << "(1) " << addr << "\n\n";

    std::cout << "(2)\n";
    for (const auto& token : addr)
    {
        std::cout << token << "\n";
    }

    std::cout << "\n";
}

void jsonpointer_address_concatentae()
{
    jsonpointer::address addr("/a~1b");

    addr += jsonpointer::address("//m~0n");

    std::cout << "(1) " << addr << "\n\n";

    std::cout << "(2)\n";
    for (const auto& token : addr)
    {
        std::cout << token << "\n";
    }

    std::cout << "\n";
}

void jsonpointer_examples()
{
    std::cout << "\njsonpointer examples\n\n";
    jsonpointer_select_author();
    jsonpointer_address_example();
    jsonpointer_select_RFC6901();
    jsonpointer_add_member_to_object();
    jsonpointer_add_element_to_array();
    jsonpointer_add_element_to_end_array();
    jsonpointer_add_element_outside_range();
    jsonpointer_remove_object_member();
    jsonpointer_remove_array_element();
    jsonpointer_replace_object_value();
    jsonpointer_replace_array_value();
    jsonpointer_contains();
    jsonpointer_error_example();
    jsonpointer_insert_name_exists();
    jsonpointer_insert_or_assign_name_exists();
    jsonpointer_get_examples();
    jsonpointer_address_iterator_example();
    jsonpointer_address_append_tokens();
    jsonpointer_address_concatentae();
}

