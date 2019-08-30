// Copyright 2018 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>

using namespace jsoncons; // For brevity

void construct_json_byte_string()
{
    byte_string bs = {'H','e','l','l','o'};

    // default suggested encoding (base64url)
    json j1(bs);
    std::cout << "(1) "<< j1 << "\n\n";

    // base64 suggested encoding
    json j2(bs, semantic_tag::base64);
    std::cout << "(2) "<< j2 << "\n\n";

    // base16 suggested encoding
    json j3(bs, semantic_tag::base16);
    std::cout << "(3) "<< j3 << "\n\n";
}

void retrieve_json_value_as_byte_string()
{
    json j;
    j["ByteString"] = byte_string({'H','e','l','l','o'});
    j["EncodedByteString"] = json("SGVsbG8=", semantic_tag::base64);

    std::cout << "(1)\n";
    std::cout << pretty_print(j) << "\n\n";

    // Retrieve a byte string as a jsoncons::byte_string
    byte_string bs1 = j["ByteString"].as<byte_string>();
    std::cout << "(2) " << bs1 << "\n\n";

    // or alternatively as a std::vector<uint8_t>
    std::vector<uint8_t> v = j["ByteString"].as<std::vector<uint8_t>>();

    // Retrieve a byte string from a text string containing base64 character values
    byte_string bs2 = j["EncodedByteString"].as<byte_string>();
    std::cout << "(3) " << bs2 << "\n\n";

    // Retrieve a byte string view  to access the memory that's holding the byte string
    byte_string_view bsv3 = j["ByteString"].as<byte_string_view>();
    std::cout << "(4) " << bsv3 << "\n\n";

    // Can't retrieve a byte string view of a text string 
    try
    {
        byte_string_view bsv4 = j["EncodedByteString"].as<byte_string_view>();
    }
    catch (const std::exception& e)
    {
        std::cout << "(5) "<< e.what() << "\n\n";
    }
}

void serialize_json_byte_string()
{
    byte_string bs = {'H','e','l','l','o'};

    json j(bs);

    // default
    std::cout << "(1) "<< j << "\n\n";

    // base16
    json_options options2;
    options2.byte_string_format(byte_string_chars_format::base16);
    std::cout << "(2) "<< print(j, options2) << "\n\n";

    // base64
    json_options options3;
    options3.byte_string_format(byte_string_chars_format::base64);
    std::cout << "(3) "<< print(j, options3) << "\n\n";

    // base64url
    json_options options4;
    options4.byte_string_format(byte_string_chars_format::base64url);
    std::cout << "(4) "<< print(j, options4) << "\n\n";
}

void byte_string_examples()
{
    std::cout << "byte_string examples" << "\n\n";
    construct_json_byte_string();
    serialize_json_byte_string();
    retrieve_json_value_as_byte_string();
}


