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
#include <fstream>

using namespace jsoncons;

TEST_CASE("test_1")
{
    basic_json<char32_t> j; 

    std::basic_ostringstream<char32_t> os;

    std::cout << sizeof(json) << std::endl;

    //os << j << U"\n";
}

TEST_CASE("test_shrink_to_fit")
{
    json val = json::make_array(3);
    val.reserve(100);
    val[0].reserve(100);
    val[0]["key"] = "value";
    val.shrink_to_fit();
    CHECK(3 == val.size());
    CHECK(1 == val[0].size());
}

TEST_CASE("test_for_each_value")
{
    std::string input = "{\"A\":\"Jane\", \"B\":\"Roe\",\"C\":10}";
    json val = json::parse(input);

    json::object_iterator it = val.object_range().begin();

    CHECK(it->value().is_string());
    ++it;
    CHECK(it->value().is_string());
    ++it;
    CHECK(it->value().get_storage_type() == jsoncons::storage_type::uint64_val);
    ++it;
    CHECK((it == val.object_range().end()));
}

TEST_CASE("test_assignment")
{
    json root;

    root["double_1"] = 10.0;

    json double_1 = root["double_1"];

    CHECK(double_1.as<double>() == Approx(10.0).epsilon(0.000001));

    root["myobject"] = json();
    root["myobject"]["double_2"] = 7.0;
    root["myobject"]["bool_2"] = true;
    root["myobject"]["int_2"] = 0LL;
    root["myobject"]["string_2"] = "my string";
    root["myarray"] = json::array();

    json double_2 = root["myobject"]["double_2"];

    CHECK(double_2.as<double>() == Approx(7.0).epsilon(0.000001));
    CHECK(double_2.as<int>() == 7);
    CHECK(root["myobject"]["bool_2"].as<bool>());
    CHECK(root["myobject"]["int_2"].as<int64_t>() == 0);
    CHECK(root["myobject"]["string_2"].as<std::string>() == std::string("my string"));

    CHECK(root["myobject"]["bool_2"].as<bool>());
    CHECK(root["myobject"]["int_2"].as<long long>() == 0);
    CHECK(root["myobject"]["string_2"].as<std::string>() == std::string("my string"));

}

TEST_CASE("test_array")
{
    json root;

    root["addresses"];

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

    CHECK(root["addresses"].size() == 2);

}

TEST_CASE("test_null")
{
    json nullval = json::null();
    CHECK(nullval.is_null());
    CHECK(nullval.is<jsoncons::null_type>());

    json obj;
    obj["field"] = json::null();
    CHECK(obj["field"] == json::null());
}

TEST_CASE("test_to_string")
{
    std::ostringstream os;
    os << "{"
       << "\"string\":\"value\""
       << ",\"null\":null"
       << ",\"bool1\":false"
       << ",\"bool2\":true"
       << ",\"integer\":12345678"
       << ",\"neg-integer\":-87654321"
       << ",\"double\":123456.01"
       << ",\"neg-double\":-654321.01"
       << ",\"exp\":2.00600e+03"
       << ",\"minus-exp\":1.00600e-010"
       << ",\"escaped-string\":\"\\\\\\n\""
       << "}";


    json root = json::parse(os.str());

    CHECK(root["null"].is_null());
    CHECK(root["null"].is<jsoncons::null_type>());
    CHECK_FALSE(root["bool1"].as<bool>());
    CHECK(root["bool2"].as<bool>());
    CHECK(root["integer"].as<int>() == 12345678);
    CHECK(root["integer"].as<unsigned int>() == 12345678);
    CHECK(root["neg-integer"].as<int>() == -87654321);
    CHECK(root["double"].as<double>() == Approx(123456.01).epsilon(0.0000001));
    CHECK(root["escaped-string"].as<std::string>() == std::string("\\\n"));

    CHECK_FALSE(root["bool1"].as<bool>());
    CHECK(root["bool2"].as<bool>());
    CHECK(root["integer"].as<int>() == 12345678);
    CHECK(root["integer"].as<unsigned int>() == 12345678);
    CHECK(root["neg-integer"].as<int>() == -87654321);
    CHECK(root["double"].as<double>() == Approx(123456.01).epsilon(0.0000001));
    CHECK(root["escaped-string"].as<std::string>() == std::string("\\\n"));
}

TEST_CASE("test_u0000")
{
    std::string inputStr("[\"\\u0040\\u0040\\u0000\\u0011\"]");
    //std::cout << "Input:    " << inputStr << std::endl;
    json arr = json::parse(inputStr);

    std::string s = arr[0].as<std::string>();
    REQUIRE(4 == s.length());
    CHECK(static_cast<uint8_t>(s[0]) == 0x40);
    CHECK(static_cast<uint8_t>(s[1]) == 0x40);
    CHECK(static_cast<uint8_t>(s[2]) == 0x00);
    CHECK(static_cast<uint8_t>(s[3]) == 0x11);

    std::ostringstream os;
    os << arr;
    std::string s2 = os.str();

    //std::cout << std::hex << "Output:   " << os.str() << std::endl;

}

TEST_CASE("test_uHHHH")
{
    std::string inputStr("[\"\\u007F\\u07FF\\u0800\"]");
    json arr = json::parse(inputStr);

    std::string s = arr[0].as<std::string>();
    REQUIRE(s.length() == 6);
    CHECK(static_cast<uint8_t>(s[0]) == 0x7f);
    CHECK(static_cast<uint8_t>(s[1]) == 0xdf);
    CHECK(static_cast<uint8_t>(s[2]) == 0xbf);
    CHECK(static_cast<uint8_t>(s[3]) == 0xe0);
    CHECK(static_cast<uint8_t>(s[4]) == 0xa0);
    CHECK(static_cast<uint8_t>(s[5]) == 0x80);

    std::ostringstream os;
    json_options options;
    options.escape_all_non_ascii(true);
    arr.dump(os, options);
    std::string outputStr = os.str();

    json arr2 = json::parse(outputStr);
    std::string s2 = arr2[0].as<std::string>();
    REQUIRE(s2.length() == 6);
    CHECK(static_cast<uint8_t>(s2[0]) == 0x7f);
    CHECK(static_cast<uint8_t>(s2[1]) == 0xdf);
    CHECK(static_cast<uint8_t>(s2[2]) == 0xbf);
    CHECK(static_cast<uint8_t>(s2[3]) == 0xe0);
    CHECK(static_cast<uint8_t>(s2[4]) == 0xa0);
    CHECK(static_cast<uint8_t>(s2[5]) == 0x80);

}

TEST_CASE("test_multiline_comments")
{
    std::string path = "./input/json-multiline-comment.json";
    std::fstream is(path);
    if (!is)
    {
        std::cout << "Cannot open " << path << std::endl;
        return;
    }
    json j = json::parse(is);

    CHECK(j.is_array());
    CHECK(j.is<json::array>());
    CHECK(j.size() == 0);
}

