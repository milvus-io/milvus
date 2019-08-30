// Copyright 2013-2018 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/json_query.hpp>
#include <iostream>
#include <sstream>
#include <vector>
#include <map>
#include <utility>
#include <ctime>
#include <new>
#include <fstream>
#include <catch/catch.hpp>

#if defined(_MSC_VER) && _MSC_VER >= 1900
#include <filesystem>
namespace fs = std::experimental::filesystem;
#else
//#include <filesystem>
//namespace fs = std::filesystem;
#endif

using namespace jsoncons;
using namespace jsoncons::jsonpath;

#if defined(_MSC_VER) && _MSC_VER >= 1900
TEST_CASE("JSONPath Test Suite")
{
    ojson document;
    std::map<fs::path,std::string> query_dictionary;
    std::map<fs::path,ojson> expected_dictionary;

    std::string path = "./input/JSONPathTestSuite";
    for (auto& p : fs::directory_iterator(path))
    {
        if (fs::exists(p) && fs::is_regular_file(p))
        {
            if (p.path().filename() == "document.json")
            {
                try 
                {
                    std::ifstream is(p.path().c_str());
                    document = ojson::parse(is);
                }
                catch(const std::exception& e)
                {
                    std::cerr << e.what() << std::endl;
                    return; 
                }
            }
            else if (p.path().extension() == ".jsonpath")
            {
                std::string s;
                char buffer[4096];
                std::ifstream is(p.path().c_str());
                while (is.read(buffer, sizeof(buffer)))
                {
                    s.append(buffer, sizeof(buffer));
                }
                s.append(buffer, (size_t)is.gcount());
                query_dictionary[p.path().stem()] = s;
            }
            else if (p.path().extension() == ".json")
            {
                try
                {
                    ojson j;
                    std::ifstream is(p.path().c_str());
                    j = ojson::parse(is);
                    expected_dictionary[p.path().stem()] = j;
                }
                catch (const jsoncons::ser_error& e)
                {
                    std::cerr << e.what() << std::endl;
                    return; 
                }
            }
        }
    }

    for (auto pair : query_dictionary)
    {
        auto it = expected_dictionary.find(pair.first);
        if (it != expected_dictionary.end())
        {
            try
            {
                ojson result = json_query(document, pair.second);
                CHECK(it->second == result);
            }
            catch (const jsoncons::jsonpath::jsonpath_error& e)
            {
                ojson result = json_query(document, pair.second);
                std::cerr << pair.first << " " << pair.second << " " << e.what() << std::endl;
            }
            catch (const std::exception& e)
            {
                std::cerr << e.what() << std::endl;
            }
        }
        else
        {
            std::cout << "Expected value for " << pair.first << "not found \n";
            std::cout << pair.second << '\n';
            ojson result = json_query(document,pair.second);
            std::cout << pretty_print(result) << std::endl;
        }
    }
}
#endif


