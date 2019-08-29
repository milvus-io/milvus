// Copyright 2013 Daniel Parker
// Distributed under Boost license

#include <string>
#include <cwchar>
#ifdef _MSC_VER
#include <codecvt>
#endif
#include <jsoncons/json.hpp>
#include <fstream>
#include <iomanip>

using namespace jsoncons;

void wjson_object()
{
    wjson root;
    root[L"field1"] = L"test";
    root[L"field2"] = 3.9;
    root[L"field3"] = true;
    std::wcout << root << L"\n";
}

void wjson_escape_u2()
{
#ifdef _MSC_VER
    std::wstring input = L"[\"\\u007F\\u07FF\\u0800\"]";
    std::wistringstream is(input);

    wjson val = wjson::parse(is);

    std::wstring s = val[0].as<std::wstring>();
    std::cout << "length=" << s.length() << std::endl;
    std::cout << "Hex dump: [";
    for (size_t i = 0; i < s.size(); ++i)
    {
        if (i != 0)
            std::cout << " ";
        uint32_t u(s[i] >= 0 ? s[i] : 256 + s[i] );
        std::cout << "0x"  << std::hex<< std::setfill('0') << std::setw(2) << u;
    }
    std::cout << "]" << std::endl;

    std::wofstream os("./output/xxx.txt");
    os.imbue(std::locale(os.getloc(), new std::codecvt_utf8_utf16<wchar_t>));
    
    wjson_options options;
    options.escape_all_non_ascii(true);

    os << pretty_print(val,options) << L"\n";
#endif
}

void wjson_surrogate_pair()
{
#ifdef _MSC_VER
    std::wstring input = L"[\"\\uD950\\uDF21\"]";
    std::wistringstream is(input);

    wjson val = wjson::parse(is);

    std::wstring s = val[0].as<std::wstring>();
    std::cout << "length=" << s.length() << std::endl;
    std::cout << "Hex dump: [";
    for (size_t i = 0; i < s.size(); ++i)
    {
        if (i != 0)
            std::cout << " ";
        uint32_t u(s[i] >= 0 ? s[i] : 256 + s[i] );
        std::cout << "0x"  << std::hex<< std::setfill('0') << std::setw(2) << u;
    }
    std::cout << "]" << std::endl;
#endif
}

void wjson_examples()
{
    std::cout << "\nwjson examples\n\n";
    wjson_object();
    wjson_escape_u2();
    wjson_surrogate_pair();
    std::cout << std::endl;
}


