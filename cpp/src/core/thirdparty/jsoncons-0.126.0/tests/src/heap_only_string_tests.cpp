// Copyright 2013 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons/detail/heap_only_string.hpp>
#include <catch/catch.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>

using namespace jsoncons;

TEST_CASE("test_heap_only_string")
{
    std::string input = "Hello World";
    auto s = jsoncons::detail::heap_only_string_factory<char, std::allocator<char>>::create(input.data(), input.size());

    //std::cout << s->c_str() << std::endl;
    CHECK(input == std::string(s->c_str()));

    jsoncons::detail::heap_only_string_factory<char,std::allocator<char>>::destroy(s);
}

TEST_CASE("test_heap_only_string_wchar_t")
{
    std::wstring input = L"Hello World";
    auto s = jsoncons::detail::heap_only_string_factory<wchar_t, std::allocator<wchar_t>>::create(input.data(), input.size());

    //std::wcout << s->c_str() << std::endl;

    CHECK(input == std::wstring(s->c_str()));

    jsoncons::detail::heap_only_string_factory<wchar_t,std::allocator<wchar_t>>::destroy(s);
}

