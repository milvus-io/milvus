// Copyright 2016 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif
#include <jsoncons/json.hpp>
#include <jsoncons_ext/ubjson/ubjson.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <limits>
#include <iomanip>
#include <catch/catch.hpp>

using namespace jsoncons;

void check_decode_ubjson(const std::vector<uint8_t>& v, const json& expected)
{
    json j1 = ubjson::decode_ubjson<json>(v);
    REQUIRE(j1 == expected);

    std::string s;
    for (auto c : v)
    {
        s.push_back(c);
    }
    std::istringstream is(s);
    json j2 = ubjson::decode_ubjson<json>(is);
    REQUIRE(j2 == expected);
}

void check_decode_ubjson(const std::vector<uint8_t>& expected, const std::vector<uint8_t>& result)
{
    if (result.size() != expected.size())
    {
        std::cout << std::hex << (int)expected[0] << " " << std::hex << (int)result[0] << std::endl;
    }
    REQUIRE(result.size() == expected.size());
    for (size_t i = 0; i < expected.size(); ++i)
    {
        if (expected[i] != result[i])
        {
            std::cout << "Different " << i << "\n"; 
            for (size_t k = 0; k < expected.size(); ++k)
            {
                std::cout << std::hex << std::setprecision(2) << std::setw(2) 
                          << std::noshowbase << std::setfill('0') << static_cast<int>(result[k]);
            }
        }
        REQUIRE(result[i] == expected[i]);
    }
}

TEST_CASE("decode_number_ubjson_test")
{
    SECTION("null, true, false")
    {
        check_decode_ubjson({'Z'},json::null()); 
        check_decode_ubjson({'T'},json(true)); 
        check_decode_ubjson({'F'},json(false)); 
    }
    SECTION("uint8")
    {
        check_decode_ubjson({'U',0x00},json(0U));
        check_decode_ubjson({'U',0x01},json(1U));
        check_decode_ubjson({'U',0x0a},json(10U));
        check_decode_ubjson({'U',0x17},json(23U));
        check_decode_ubjson({'U',0x18},json(24U));
        check_decode_ubjson({'U',0x7f},json(127U)); 
        check_decode_ubjson({'U',0xff},json(255U));
    }
    SECTION("int8,int16,int32,int64")
    {
        check_decode_ubjson({'i',0xff},json(-1));
        check_decode_ubjson({'I',0x01,0x00},json(256));
        check_decode_ubjson({'l',0,0,0xff,0xff},json(65535));
        check_decode_ubjson({'l',0,1,0x00,0x00},json(65536));
        check_decode_ubjson({'L',0,0,0,0,0xff,0xff,0xff,0xff},json(4294967295));
        check_decode_ubjson({'L',0,0,0,1,0,0,0,0},json(4294967296));
        check_decode_ubjson({'L',0x7f,0xff,0xff,0xff,0xff,0xff,0xff,0xff},json((std::numeric_limits<int64_t>::max)()));
        // negative integers
        check_decode_ubjson({'I',0xff,0},json(-256));
        check_decode_ubjson({'I',0xfe,0xff},json(-257));
        check_decode_ubjson({'l',0xff,0xff,0,0},json(-65536));
        check_decode_ubjson({'l',0xff,0xfe,0xff,0xff},json(-65537));
        check_decode_ubjson({'L',0xff,0xff,0xff,0xff,0,0,0,0},json(-4294967296));
        check_decode_ubjson({'L',0xff,0xff,0xff,0xfe,0xff,0xff,0xff,0xff},json(-4294967297));
    }

    SECTION("float32,float64")
    {
        check_decode_ubjson({'D',0,0,0,0,0,0,0,0},json(0.0));
        check_decode_ubjson({'D',0xbf,0xf0,0,0,0,0,0,0},json(-1.0));
        check_decode_ubjson({'D',0xc1,0x6f,0xff,0xff,0xe0,0,0,0},json(-16777215.0));
    }

    SECTION("array")
    {
        check_decode_ubjson({'[',']'},json::parse("[]"));
        check_decode_ubjson({'[', 'Z', 'T', 'F', ']'},json::parse("[null,true,false]"));
        check_decode_ubjson({'[','#','i',0},json::parse("[]"));
        check_decode_ubjson({'[','#','i',1,'I',0xff,0},json::parse("[-256]"));
    }
    SECTION("ubjson array optimized with type and count")
    {
        check_decode_ubjson({'[','$','I','#','i',2,
                             0x01,0x00, // 256
                             0xff,0}, // -256
                             json::parse("[256,-256]"));
    }
    SECTION("ubjson object optimized with type and count")
    {
        check_decode_ubjson({'{','$','I','#','i',2,
                             'i',5,'f','i','r','s','t',
                             0x01,0x00, // 256
                             'i',6,'s','e','c','o','n','d',
                             0xff,0}, // -256
                             json::parse("{\"first\":256,\"second\":-256}"));
    }
}

TEST_CASE("decode_ubjson_arrays_and_maps")
{
    check_decode_ubjson({'[','#','U',0x00},json::array());
    check_decode_ubjson({ '{','#','U',0x00 }, json::object());
    
    check_decode_ubjson({'[','#','U',0x01,'U',0x00},json::parse("[0]"));
    check_decode_ubjson({'[','#','U',0x02,'U',0x00,'U',0x00},json::parse("[0,0]"));
    check_decode_ubjson({'[','#','U',0x02,
                         '[','#','U',0x01,'U',0x00,
                         'U',0x00},json::parse("[[0],0]"));
    check_decode_ubjson({'[','#','U',0x01,'S','U',0x05,'H','e','l','l','o'},json::parse("[\"Hello\"]"));
    check_decode_ubjson({'{','#','U',0x01,'U',0x02,'o','c','[','#','U',0x01,'U',0x00}, json::parse("{\"oc\": [0]}"));
    check_decode_ubjson({'{','#','U',0x01,'U',0x02,'o','c','[','#','U',0x04,'U',0x00,'U',0x01,'U',0x02,'U',0x03}, json::parse("{\"oc\": [0,1,2,3]}"));
}

TEST_CASE("decode indefinite length ubjson arrays and maps")
{
    std::vector<uint8_t> v;
    ubjson::ubjson_bytes_encoder encoder(v);

    SECTION("[\"Hello\"]")
    {
        encoder.begin_array();
        encoder.string_value("Hello");
        encoder.end_array();

        check_decode_ubjson({'[','S','U',0x05,'H','e','l','l','o',']'}, v);
    }

    SECTION("{\"oc\": [0]}")
    {
        encoder.begin_object();
        encoder.name("oc");
        encoder.begin_array();
        encoder.uint64_value(0);
        encoder.end_array();
        encoder.end_object();

        check_decode_ubjson({'{','U',0x02,'o','c','[','U',0x00,']','}'}, v);
    }

    SECTION("{\"oc\": [0,1,2,3]}")
    {
        encoder.begin_object();
        encoder.name("oc");
        encoder.begin_array();
        encoder.uint64_value(0);
        encoder.uint64_value(1);
        encoder.uint64_value(2);
        encoder.uint64_value(3);
        encoder.end_array();
        encoder.end_object();

        check_decode_ubjson({'{','U',0x02,'o','c','[','U',0x00,'U',0x01,'U',0x02,'U',0x03,']','}'}, v);
    }
}


