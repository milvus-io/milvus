// Copyright 2013-2018 Daniel Parker
// Distributed under Boost license

#include <catch/catch.hpp>
#include <iostream>
#include <sstream>
#include <vector>
#include <map>
#include <utility>
#include <ctime>
#include <new>
#include <sstream>
#include <jsoncons/json.hpp>
#include <fstream>
#include <iostream>
#include <locale>

#if defined(_MSC_VER) && _MSC_VER >= 1900
#include <filesystem>
namespace fs = std::experimental::filesystem;
#else
//#include <filesystem>
//namespace fs = std::filesystem;
#endif

using namespace jsoncons;

#if defined(_MSC_VER) && _MSC_VER >= 1900
TEST_CASE("JSON Parsing Test Suite")
{
    SECTION("Expected success")
    {
        std::string path = "./input/JSONTestSuite";
        for (auto& p : fs::directory_iterator(path))
        {
            if (fs::exists(p) && fs::is_regular_file(p) && p.path().extension() == ".json" && p.path().filename().c_str()[0] == 'y')
            {
                std::ifstream is(p.path().c_str());
                strict_parse_error_handler err_handler;
                json_reader reader(is, err_handler);
                std::error_code ec;
                reader.read(ec);
                if (ec)
                {
                    std::cout << p.path().filename().string() << " failed, expected success\n";
                }
                CHECK_FALSE(ec);                        
            }
        }
    }
    SECTION("Expected failure")
    {
        std::string path = "./input/JSONTestSuite";
        for (auto& p : fs::directory_iterator(path))
        {
            if (fs::exists(p) && fs::is_regular_file(p) && p.path().extension() == ".json" && p.path().filename().c_str()[0] == 'n')
            {
                std::ifstream is(p.path().c_str());
                strict_parse_error_handler err_handler;
                json_reader reader(is, err_handler);
                std::error_code ec;
                reader.read(ec);
                if (!ec)
                {
                    std::cout << p.path().filename().string() << " succeeded, expected failure\n";
                }
                CHECK(ec);                        
            }
        }
    }
}
#endif




