// Copyright 2018 Daniel Parker
// Distributed under Boost license

#include <string>
#include <sstream>
#include <jsoncons/json.hpp>
#include <jsoncons/json_parser.hpp>

using namespace jsoncons;

void incremental_parsing_example1()
{
    jsoncons::json_decoder<json> decoder;
    json_parser parser;
    try
    {
        parser.update("[fal");
        parser.parse_some(decoder);
        std::cout << "(1) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        parser.update("se]");
        parser.parse_some(decoder);
        std::cout << "(2) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        parser.finish_parse(decoder);
        std::cout << "(3) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        parser.check_done();
        std::cout << "(4) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        json j = decoder.get_result();
        std::cout << "(5) " << j << "\n\n";
    }
    catch (const ser_error& e)
    {
        std::cout << e.what() << std::endl;
    }
}

void incremental_parsing_example2()
{
    jsoncons::json_decoder<json> decoder;
    json_parser parser;
    try
    {
        parser.update("10");
        parser.parse_some(decoder);
        std::cout << "(1) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        parser.update(".5");
        parser.parse_some(decoder); // This is the end, but the parser can't tell
        std::cout << "(2) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        parser.finish_parse(decoder); // Indicates that this is the end
        std::cout << "(3) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        parser.check_done(); // Checks if there are any unconsumed 
                             // non-whitespace characters in the input
        std::cout << "(4) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        json j = decoder.get_result();
        std::cout << "(5) " << j << "\n";
    }
    catch (const ser_error& e)
    {
        std::cout << e.what() << std::endl;
    }
}

void incremental_parsing_example3()
{
    jsoncons::json_decoder<json> decoder;
    json_parser parser;
    try
    {
        parser.update("[10");
        parser.parse_some(decoder);
        std::cout << "(1) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        parser.update(".5]{}");
        parser.parse_some(decoder); // The parser reaches the end at ']'
        std::cout << "(2) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        parser.finish_parse(decoder); // Indicates that this is the end
        std::cout << "(3) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        parser.check_done(); // Checks if there are any unconsumed 
                             // non-whitespace characters in the input
                             // (there are)
    }
    catch (const ser_error& e)
    {
        std::cout << "(4) " << e.what() << std::endl;
    }
}

void parse_nan_replacement_example()
{
    std::string s = R"(
        {
           "A" : "NaN",
           "B" : "Infinity",
           "C" : "-Infinity"
        }
    )";

    json_options options;
    options.nan_to_str("NaN")
           .inf_to_str("Infinity");

    jsoncons::json_decoder<json> decoder;
    json_parser parser(options);
    try
    {
        parser.update(s);
        parser.parse_some(decoder);
        parser.finish_parse(decoder);
        parser.check_done();
    }
    catch (const ser_error& e)
    {
        std::cout << e.what() << std::endl;
    }

    json j = decoder.get_result(); // performs move
    if (j["A"].is<double>())
    {
        std::cout << "A: " << j["A"].as<double>() << std::endl;
    }
    if (j["B"].is<double>())
    {
        std::cout << "B: " << j["B"].as<double>() << std::endl;
    }
    if (j["C"].is<double>())
    {
        std::cout << "C: " << j["C"].as<double>() << std::endl;
    }
}

void json_parser_examples()
{
    std::cout << "\njson_parser examples\n\n";
    incremental_parsing_example1();
    incremental_parsing_example2();
    incremental_parsing_example3();
    parse_nan_replacement_example();

    std::cout << std::endl;
}



