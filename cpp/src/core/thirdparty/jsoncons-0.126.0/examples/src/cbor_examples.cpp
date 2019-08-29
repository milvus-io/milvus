// Copyright 2017 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>
#include <jsoncons_ext/jsonpath/json_query.hpp>
#include <string>
#include <iomanip>
#include <cassert>

using namespace jsoncons;

void serialize_to_cbor_buffer()
{
    std::vector<uint8_t> buffer;
    cbor::cbor_bytes_encoder encoder(buffer);

    encoder.begin_array(); // Indefinite length array
    encoder.string_value("cat");
    encoder.byte_string_value(byte_string({'p','u','r','r'}));
    encoder.byte_string_value(byte_string({'h','i','s','s'}),
                             semantic_tag::base64); // suggested conversion to base64
    encoder.int64_value(1431027667, semantic_tag::timestamp);
    encoder.end_array();
    encoder.flush();

    for (auto c : buffer)
    {
        std::cout << std::hex << std::setprecision(2) << std::setw(2) 
                  << std::noshowbase << std::setfill('0') << static_cast<int>(c);
    }
    std::cout << "\n\n";

/* 
    9f -- Start indefinte length array
      63 -- String value of length 3
        636174 -- "cat"
      44 -- Byte string value of length 4
        70757272 -- 'p''u''r''r'
      d6 - Expected conversion to base64
      44
        68697373 -- 'h''i''s''s'
      c1 -- Tag value 1 (seconds relative to 1970-01-01T00:00Z in UTC time)
        1a -- 32 bit unsigned integer
          554bbfd3 -- 1431027667
      ff -- "break" 
*/ 
}

void serialize_to_cbor_stream()
{
    std::ostringstream os;
    cbor::cbor_encoder encoder(os);

    encoder.begin_array(3); // array of length 3
    encoder.string_value("-18446744073709551617", semantic_tag::bigint);
    encoder.string_value("184467440737095516.16", semantic_tag::bigdec);
    encoder.int64_value(1431027667, semantic_tag::timestamp);
    encoder.end_array();
    encoder.flush();

    for (auto c : os.str())
    {
        std::cout << std::hex << std::setprecision(2) << std::setw(2) 
                  << std::noshowbase << std::setfill('0') << (int)unsigned char(c);
    }
    std::cout << "\n\n";

/*
    83 -- array of length 3
      c3 -- Tag 3 (negative bignum)
      49 -- Byte string value of length 9
        010000000000000000 -- Bytes content
      c4 -- Tag 4 (decimal fraction)
        82 -- Array of length 2
          21 -- -2 (exponent)
          c2 Tag 2 (positive bignum)
          49 -- Byte string value of length 9
            010000000000000000
      c1 -- Tag 1 (seconds relative to 1970-01-01T00:00Z in UTC time)
        1a -- 32 bit unsigned integer
          554bbfd3 -- 1431027667
*/
}

void cbor_reputon_example()
{
    ojson j1 = ojson::parse(R"(
    {
       "application": "hiking",
       "reputons": [
       {
           "rater": "HikingAsylum.example.com",
           "assertion": "strong-hiker",
           "rated": "Marilyn C",
           "rating": 0.90
         }
       ]
    }
    )");

    // Encode a basic_json value to a CBOR value
    std::vector<uint8_t> data;
    cbor::encode_cbor(j1, data);

    // Decode a CBOR value to a basic_json value
    ojson j2 = cbor::decode_cbor<ojson>(data);
    std::cout << "(1)\n" << pretty_print(j2) << "\n\n";

    // Accessing the data items 

    const ojson& reputons = j2["reputons"];

    std::cout << "(2)\n";
    for (auto element : reputons.array_range())
    {
        std::cout << element.at("rated").as<std::string>() << ", ";
        std::cout << element.at("rating").as<double>() << "\n";
    }
    std::cout << std::endl;

    // Get a CBOR value for a nested data item with jsonpointer
    std::error_code ec;
    auto const& rated = jsonpointer::get(j2, "/reputons/0/rated", ec);
    if (!ec)
    {
        std::cout << "(3) " << rated.as_string() << "\n";
    }

    std::cout << std::endl;
}

void decode_cbor_byte_string()
{
    // byte string of length 5
    std::vector<uint8_t> buf = {0x45,'H','e','l','l','o'};
    json j = cbor::decode_cbor<json>(buf);

    auto bs = j.as<byte_string>();

    // byte_string to ostream displays as hex
    std::cout << "(1) "<< bs << "\n\n";

    // byte string value to JSON text becomes base64url
    std::cout << "(2) " << j << std::endl;
}

void decode_byte_string_with_encoding_hint()
{
    // semantic tag indicating expected conversion to base64
    // followed by byte string of length 5
    std::vector<uint8_t> buf = {0xd6,0x45,'H','e','l','l','o'};
    json j = cbor::decode_cbor<json>(buf);

    auto bs = j.as<byte_string>();

    // byte_string to ostream displays as hex
    std::cout << "(1) "<< bs << "\n\n";

    // byte string value to JSON text becomes base64
    std::cout << "(2) " << j << std::endl;
}

void encode_cbor_byte_string()
{
    // construct byte string value
    json j(byte_string("Hello"));

    std::vector<uint8_t> buf;
    cbor::encode_cbor(j, buf);

    std::cout << std::hex << std::showbase << "(1) ";
    for (auto c : buf)
    {
        std::cout << (int)c;
    }
    std::cout << std::dec << "\n\n";

    json j2 = cbor::decode_cbor<json>(buf);
    std::cout << "(2) " << j2 << std::endl;
}

void encode_byte_string_with_encoding_hint()
{
    // construct byte string value
     json j1(byte_string("Hello"), semantic_tag::base64);

    std::vector<uint8_t> buf;
    cbor::encode_cbor(j1, buf);

    std::cout << std::hex << std::showbase << "(1) ";
    for (auto c : buf)
    {
        std::cout << (int)c;
    }
    std::cout << std::dec << "\n\n";

    json j2 = cbor::decode_cbor<json>(buf);
    std::cout << "(2) " << j2 << std::endl;
}

void query_cbor()
{
    // Construct a json array of numbers
    json j = json::array();

    j.emplace_back(5.0);

    j.emplace_back(0.000071);

    j.emplace_back("-18446744073709551617",semantic_tag::bigint);

    j.emplace_back("1.23456789012345678901234567890", semantic_tag::bigdec);

    j.emplace_back("0x3p-1", semantic_tag::bigfloat);

    // Encode to JSON
    std::cout << "(1)\n";
    std::cout << pretty_print(j);
    std::cout << "\n\n";

    // as<std::string>() and as<double>()
    std::cout << "(2)\n";
    std::cout << std::dec << std::setprecision(15);
    for (const auto& item : j.array_range())
    {
        std::cout << item.as<std::string>() << ", " << item.as<double>() << "\n";
    }
    std::cout << "\n";

    // Encode to CBOR
    std::vector<uint8_t> v;
    cbor::encode_cbor(j,v);

    std::cout << "(3)\n";
    for (auto c : v)
    {
        std::cout << std::hex << std::setprecision(2) << std::setw(2)
                  << std::setfill('0') << static_cast<int>(c);
    }
    std::cout << "\n\n";
/*
    85 -- Array of length 5     
      fa -- float 
        40a00000 -- 5.0
      fb -- double 
        3f129cbab649d389 -- 0.000071
      c3 -- Tag 3 (negative bignum)
        49 -- Byte string value of length 9
          010000000000000000
      c4 -- Tag 4 (decimal fraction)
        82 -- Array of length 2
          38 -- Negative integer of length 1
            1c -- -29
          c2 -- Tag 2 (positive bignum)
            4d -- Byte string value of length 13
              018ee90ff6c373e0ee4e3f0ad2
      c5 -- Tag 5 (bigfloat)
        82 -- Array of length 2
          20 -- -1
          03 -- 3   
*/

    // Decode back to json
    json other = cbor::decode_cbor<json>(v);
    assert(other == j);

    // Query with JSONPath
    std::cout << "(4)\n";
    json result = jsonpath::json_query(other,"$[?(@ < 1.5)]");
    std::cout << pretty_print(result) << "\n\n";
}

void encode_cbor_with_packed_strings()
{
    ojson j = ojson::parse(R"(
[
     {
       "name" : "Cocktail",
       "count" : 417,
       "rank" : 4
     },
     {
       "rank" : 4,
       "count" : 312,
       "name" : "Bath"
     },
     {
       "count" : 691,
       "name" : "Food",
       "rank" : 4
     }
  ]
)");

    cbor::cbor_options options;
    options.pack_strings(true);
    std::vector<uint8_t> buf;

    cbor::encode_cbor(j, buf, options);

    for (auto c : buf)
    {
        std::cout << std::hex << std::setprecision(2) << std::setw(2) 
                  << std::noshowbase << std::setfill('0') << static_cast<int>(c);
    }
    std::cout << "\n";

/*
    d90100 -- tag (256)
      83 -- array(3)
        a3 -- map(3)
          64 -- text string (4)
            6e616d65 -- "name"
          68 -- text string (8)
            436f636b7461696c -- "Cocktail"
          65 -- text string (5)
            636f756e74 -- "count"
            1901a1 -- unsigned(417)
          64 -- text string (4)
            72616e6b -- "rank"
            04 -- unsigned(4)
        a3 -- map(3)
          d819 -- tag(25)
            03 -- unsigned(3)
          04 -- unsigned(4)
          d819 -- tag(25)
            02 -- unsigned(2)
            190138 -- unsigned(312)
          d819 -- tag(25)
            00 -- unsigned(0)
          64 -- text string(4)
            42617468 -- "Bath"
        a3 -- map(3)
          d819 -- tag(25)
            02 -- unsigned(2)
          1902b3 -- unsigned(691)
          d819 -- tag(25)
            00 -- unsigned(0)
          64 - text string(4)
            466f6f64 -- "Food"
          d819 -- tag(25)
            03 -- unsigned(3)
            04 -- unsigned(4)
*/

    ojson j2 = cbor::decode_cbor<ojson>(buf);
    assert(j2 == j);
}

void decode_cbor_with_packed_strings()
{
    std::vector<uint8_t> v = {0xd9,0x01,0x00, // tag(256)
      0x85,                 // array(5)
         0x63,              // text(3)
            0x61,0x61,0x61, // "aaa"
         0xd8, 0x19,        // tag(25)
            0x00,           // unsigned(0)
         0xd9, 0x01,0x00,   // tag(256)
            0x83,           // array(3)
               0x63,        // text(3)
                  0x62,0x62,0x62, // "bbb"
               0x63,        // text(3)
                  0x61,0x61,0x61, // "aaa"
               0xd8, 0x19,  // tag(25)
                  0x01,     // unsigned(1)
         0xd9, 0x01,0x00,   // tag(256)
            0x82,           // array(2)
               0x63,        // text(3)
                  0x63,0x63,0x63, // "ccc"
               0xd8, 0x19,  // tag(25)
                  0x00,     // unsigned(0)
         0xd8, 0x19,        // tag(25)
            0x00           // unsigned(0)
    };

    ojson j = cbor::decode_cbor<ojson>(v);

    std::cout << pretty_print(j) << "\n";
}

void cbor_examples()
{
    std::cout << "\ncbor examples\n\n";
    decode_byte_string_with_encoding_hint();
    encode_byte_string_with_encoding_hint();
    decode_cbor_byte_string();
    encode_cbor_byte_string();
    serialize_to_cbor_buffer();
    serialize_to_cbor_stream();
    cbor_reputon_example();
    query_cbor();
    encode_cbor_with_packed_strings();

    decode_cbor_with_packed_strings();

    std::cout << std::endl;
}

