// Copyright 2013 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>
#include <jsoncons/detail/type_traits.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <catch/catch.hpp>

using namespace jsoncons;

template<class CharT>
std::basic_string<CharT> float_to_string(double val, int precision)
{
    jsoncons::detail::print_double print(floating_point_options(chars_format::general,precision,0));

    std::basic_string<CharT> s;
    jsoncons::string_result<std::basic_string<CharT>> writer(s);
    print(val, writer);
    writer.flush();
    return s;
}

TEST_CASE("test_double_to_string")
{
    double x = 1.0e100;
    std::string s = float_to_string<char>(x, std::numeric_limits<double>::digits10);
    //std::cout << x << ": " << s << std::endl;
    CHECK(s == std::string("1e+100"));

    x = 1.0e-100;
    s = float_to_string<char>(x, std::numeric_limits<double>::digits10);
    CHECK(s == std::string("1e-100"));

    x = 0.123456789e-100;
    s = float_to_string<char>(x, std::numeric_limits<double>::digits10);
    CHECK(s == std::string("1.23456789e-101"));

    x = 0.123456789e100;
    s = float_to_string<char>(x, std::numeric_limits<double>::digits10);
    CHECK(s == std::string("1.23456789e+99"));

    x = 1234563;
    s = float_to_string<char>(x, 6);
    CHECK(s == std::string("1.23456e+06"));

    x = 0.0000001234563;
    s = float_to_string<char>(x, 6);
    CHECK(s == std::string("1.23456e-07"));

    x = -1.0e+100;
    s = float_to_string<char>(x, std::numeric_limits<double>::digits10);
    std::cout << s << "\n";
    CHECK(s == std::string("-1e+100"));

    x = -1.0e-100;
    s = float_to_string<char>(x, std::numeric_limits<double>::digits10);
    CHECK(s == std::string("-1e-100"));

    x = 0;
    s = float_to_string<char>(x, std::numeric_limits<double>::digits10);
    CHECK(s == std::string("0.0"));

    x = -0;
    s = float_to_string<char>(x, std::numeric_limits<double>::digits10);
    CHECK(s == std::string("0.0"));

    x = 1;
    s = float_to_string<char>(x, std::numeric_limits<double>::digits10);
    CHECK(s == std::string("1.0"));

    x = 0.1;
    s = float_to_string<char>(x, std::numeric_limits<double>::digits10);
    CHECK(s == std::string("0.1"));

    x = 1.1;
    s = float_to_string<char>(x, 17);
    CHECK(s == std::string("1.1000000000000001"));

    x = -1;
    s = float_to_string<char>(x, std::numeric_limits<double>::digits10);
    CHECK(s == std::string("-1.0"));

    x = 10;
    s = float_to_string<char>(x, std::numeric_limits<double>::digits10);
    CHECK(s == std::string("10.0"));

    x = -10;
    s = float_to_string<char>(x, std::numeric_limits<double>::digits10);
    CHECK(s == std::string("-10.0"));

    x = 11;
    s = float_to_string<char>(x, std::numeric_limits<double>::digits10);
    CHECK(s == std::string("11.0"));

    x = -11;
    s = float_to_string<char>(x, std::numeric_limits<double>::digits10);
    CHECK(s == std::string("-11.0"));
}

#if defined(_MSC_VER)
TEST_CASE("test_locale")
{
    wchar_t * loc = _wsetlocale(LC_ALL, L"de-DE");
    CHECK_FALSE(loc == nullptr);

    double x = 123456789.0123;
    std::wstring s = float_to_string<wchar_t>(x, 13);
    //std::wcout << std::setprecision(13) << x << L": " << s << std::endl;
    CHECK(std::wstring(L"123456789.0123") == s);
    _wsetlocale(LC_ALL, L"C");
}
#endif

TEST_CASE("test_double_to_wstring")
{
    double x = 1.0e100;
    std::wstring s = float_to_string<wchar_t>(x, std::numeric_limits<double>::digits10);
    //std::wcout << x << L":" << s << std::endl;
    CHECK(s == std::wstring(L"1e+100"));

    x = 1.0e-100;
    s = float_to_string<wchar_t>(x, std::numeric_limits<double>::digits10);
    CHECK(s == std::wstring(L"1e-100"));

    x = -1.0e+100;
    s = float_to_string<wchar_t>(x, std::numeric_limits<double>::digits10);
    CHECK(s == std::wstring(L"-1e+100"));

    x = -1.0e-100;
    s = float_to_string<wchar_t>(x, std::numeric_limits<double>::digits10);
    CHECK(s == std::wstring(L"-1e-100"));

    x = 0;
    s = float_to_string<wchar_t>(x, std::numeric_limits<double>::digits10);
    CHECK(s == std::wstring(L"0.0"));

    x = -0;
    s = float_to_string<wchar_t>(x, std::numeric_limits<double>::digits10);
    CHECK(s == std::wstring(L"0.0"));

    x = 1;
    s = float_to_string<wchar_t>(x, std::numeric_limits<double>::digits10);
    CHECK(s == std::wstring(L"1.0"));

    x = -1;
    s = float_to_string<wchar_t>(x, std::numeric_limits<double>::digits10);
    CHECK(s == std::wstring(L"-1.0"));

    x = 10;
    s = float_to_string<wchar_t>(x, std::numeric_limits<double>::digits10);
    CHECK(s == std::wstring(L"10.0"));

    x = -10;
    s = float_to_string<wchar_t>(x, std::numeric_limits<double>::digits10);
    CHECK(s == std::wstring(L"-10.0"));

    x = 11;
    s = float_to_string<wchar_t>(x, std::numeric_limits<double>::digits10);
    CHECK(s == std::wstring(L"11.0"));

    x = -11;
    s = float_to_string<wchar_t>(x, std::numeric_limits<double>::digits10);
    CHECK(s == std::wstring(L"-11.0"));
}


