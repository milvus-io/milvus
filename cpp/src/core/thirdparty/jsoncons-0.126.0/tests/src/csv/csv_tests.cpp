// Copyright 2013 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif
//#include <jsoncons_ext/csv/csv_options.hpp>
#include <jsoncons_ext/csv/csv_reader.hpp>
#include <jsoncons_ext/csv/csv_encoder.hpp>
#include <jsoncons/json_reader.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <iostream>
#include <fstream>
#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("n_objects_test")
{
    const std::string bond_yields = R"(Date,1Y,2Y,3Y,5Y
2017-01-09,0.0062,0.0075,0.0083,0.011
2017-01-08,0.0063,0.0076,0.0084,0.0112
2017-01-08,0.0063,0.0076,0.0084,0.0112
)";

    json_decoder<ojson> decoder;
    csv::csv_options options;
    options.assume_header(true)
          .subfield_delimiter(0);
          //.column_types("string,float,float,float,float");

    options.mapping(csv::mapping_type::n_rows);
    csv::csv_reader reader1(bond_yields,decoder,options);
    reader1.read();
    ojson val1 = decoder.get_result();
    //std::cout << "\n(1)\n"<< pretty_print(val1) << "\n";
    CHECK(val1.size() == 4);

    options.mapping(csv::mapping_type::n_objects);
    csv::csv_reader reader2(bond_yields,decoder,options);
    reader2.read();
    ojson val2 = decoder.get_result();
    //std::cout << "\n(2)\n"<< pretty_print(val2) << "\n";
    REQUIRE(val2.size() == 3);
    CHECK("2017-01-09" == val2[0]["Date"].as<std::string>());
}

TEST_CASE("m_columns_test")
{
    const std::string bond_yields = R"(Date,ProductType,1Y,2Y,3Y,5Y
2017-01-09,"Bond",0.0062,0.0075,0.0083,0.011
2017-01-08,"Bond",0.0063,0.0076,0.0084,0.0112
2017-01-08,"Bond",0.0063,0.0076,0.0084,0.0112
)";

    json_decoder<ojson> decoder;
    csv::csv_options options;
    options.assume_header(true)
           .mapping(csv::mapping_type::m_columns);

    std::istringstream is(bond_yields);
    csv::csv_reader reader(is, decoder, options);
    reader.read();
    ojson j = decoder.get_result();
    CHECK(6 == j.size());
    CHECK(3 == j["Date"].size());
    CHECK(3 == j["1Y"].size());
    CHECK(3 == j["2Y"].size());
    CHECK(3 == j["3Y"].size());
    CHECK(3 == j["5Y"].size());
}

TEST_CASE("m_columns with ignore_empty_value")
{
    const std::string bond_yields = R"(Date,ProductType,1Y,2Y,3Y,5Y
2017-01-09,"Bond",0.0062,0.0075,0.0083,0.011
2017-01-08,"Bond",0.0063,0.0076,,0.0112
2017-01-08,"Bond",0.0063,0.0076,0.0084,
)";
    //std::cout << bond_yields << std::endl <<std::endl;

    json_decoder<ojson> decoder;
    csv::csv_options options;
    options.assume_header(true)
           .ignore_empty_values(true)
           .mapping(csv::mapping_type::m_columns);

    std::istringstream is(bond_yields);
    csv::csv_reader reader(is, decoder, options);
    reader.read();
    ojson j = decoder.get_result();
    //std::cout << "\n(1)\n"<< pretty_print(j) << "\n";
    CHECK(6 == j.size());
    CHECK(3 == j["Date"].size());
    CHECK(3 == j["1Y"].size());
    CHECK(3 == j["2Y"].size());
    CHECK(2 == j["3Y"].size());
    CHECK(2 == j["5Y"].size());
}

TEST_CASE("csv_test_empty_values")
{
    std::string input = "bool-f,int-f,float-f,string-f"
"\n,,,,"
"\ntrue,12,24.7,\"test string\","
"\n,,,,";

    std::istringstream is(input);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.assume_header(true)
           .column_types("boolean,integer,float,string");

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val[0]["bool-f"].is_null());
    CHECK(val[0]["bool-f"].is<null_type>());
    CHECK(val[0]["int-f"].is_null());
    CHECK(val[0]["int-f"].is<null_type>());
    CHECK(val[0]["float-f"].is_null());
    CHECK(val[0]["float-f"].is<null_type>());
    CHECK(val[0]["string-f"].as<std::string>() == "");
    CHECK(val[0]["string-f"].is<std::string>());

    CHECK(val[1]["bool-f"] .as<bool>()== true);
    CHECK(val[1]["bool-f"].is<bool>());
    CHECK(val[1]["int-f"] .as<int>()== 12);
    CHECK(val[1]["int-f"].is<int>());
    CHECK(val[1]["float-f"] .as<double>()== 24.7);
    CHECK(val[1]["float-f"].is<double>());
    CHECK(val[1]["string-f"].as<std::string>() == "test string");
    CHECK(val[1]["string-f"].is<std::string>());

    CHECK(val[0]["bool-f"].is_null());
    CHECK(val[0]["bool-f"].is<null_type>());
    CHECK(val[0]["int-f"].is_null());
    CHECK(val[0]["int-f"].is<null_type>());
    CHECK(val[0]["float-f"].is_null());
    CHECK(val[0]["float-f"].is<null_type>());
    CHECK(val[0]["string-f"] .as<std::string>() == "");
    CHECK(val[0]["string-f"].is<std::string>());
}

TEST_CASE("csv_test_empty_values_with_defaults")
{
    std::string input = "bool-f,int-f,float-f,string-f"
"\n,,,,"
"\ntrue,12,24.7,\"test string\","
"\n,,,,";

    std::istringstream is(input);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.assume_header(true) 
           .column_types("boolean,integer,float,string")
           .column_defaults("false,0,0.0,\"\"");

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    std::cout << pretty_print(val) << std::endl;

    CHECK(val[0]["bool-f"].as<bool>() == false);
    CHECK(val[0]["bool-f"].is<bool>());
    CHECK(val[0]["int-f"] .as<int>()== 0);
    CHECK(val[0]["int-f"].is<int>());
    CHECK(val[0]["float-f"].as<double>() == 0.0);
    CHECK(val[0]["float-f"].is<double>());
    CHECK(val[0]["string-f"] .as<std::string>() == "");
    CHECK(val[0]["string-f"].is<std::string>());

    CHECK(val[1]["bool-f"] .as<bool>()== true);
    CHECK(val[1]["bool-f"].is<bool>());
    CHECK(val[1]["int-f"] .as<int>()== 12);
    CHECK(val[1]["int-f"].is<int>());
    CHECK(val[1]["float-f"] .as<double>()== 24.7);
    CHECK(val[1]["float-f"].is<double>());
    CHECK(val[1]["string-f"].as<std::string>() == "test string");
    CHECK(val[1]["string-f"].is<std::string>());

    CHECK(val[2]["bool-f"].as<bool>() == false);
    CHECK(val[2]["bool-f"].is<bool>());
    CHECK(val[2]["int-f"] .as<int>()== 0);
    CHECK(val[2]["int-f"].is<int>());
    CHECK(val[2]["float-f"].as<double>() == 0.0);
    CHECK(val[2]["float-f"].is<double>());
    CHECK(val[2]["string-f"].as<std::string>() == "");
    CHECK(val[2]["string-f"].is<std::string>());
}

TEST_CASE("csv_test_empty_values_with_empty_defaults")
{
    std::string input = "bool-f,int-f,float-f,string-f"
"\n,,,,"
"\ntrue,12,24.7,\"test string\","
"\n,,,,";

    std::istringstream is(input);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.assume_header(true)
           .column_types("boolean,integer,float,string")
           .column_defaults(",,,");

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val[0]["bool-f"].is_null());
    CHECK(val[0]["bool-f"].is<null_type>());
    CHECK(val[0]["int-f"].is_null());
    CHECK(val[0]["int-f"].is<null_type>());
    CHECK(val[0]["float-f"].is_null());
    CHECK(val[0]["float-f"].is<null_type>());
    CHECK(val[0]["string-f"] .as<std::string>() == "");
    CHECK(val[0]["string-f"].is<std::string>());

    CHECK(val[1]["bool-f"] .as<bool>() == true);
    CHECK(val[1]["bool-f"].is<bool>());
    CHECK(val[1]["int-f"] .as<int>()== 12);
    CHECK(val[1]["int-f"].is<int>());
    CHECK(val[1]["float-f"] .as<double>()== 24.7);
    CHECK(val[1]["float-f"].is<double>());
    CHECK(val[1]["string-f"].as<std::string>() == "test string");
    CHECK(val[1]["string-f"].is<std::string>());

    CHECK(val[0]["bool-f"].is_null());
    CHECK(val[0]["bool-f"].is<null_type>());
    CHECK(val[0]["int-f"].is_null());
    CHECK(val[0]["int-f"].is<null_type>());
    CHECK(val[0]["float-f"].is_null());
    CHECK(val[0]["float-f"].is<null_type>());
    CHECK(val[0]["string-f"] .as<std::string>() == "");
    CHECK(val[0]["string-f"].is<std::string>());
}

TEST_CASE("csv_test1_array_1col_skip1_a")
{
    std::string text = "a\n1\n4";
    std::istringstream is(text);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.header_lines(1);

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==2);
    CHECK(val[0].size()==1);
    CHECK(val[1].size()==1);
    CHECK(val[0][0]==json(1));
    CHECK(val[1][0]==json(4));
}

TEST_CASE("csv_test1_array_1col_skip1_b")
{
    std::string text = "a\n1\n4";
    std::istringstream is(text);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.header_lines(1);
    options.infer_types(false);

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==2);
    CHECK(val[0].size()==1);
    CHECK(val[1].size()==1);
    CHECK(val[0][0]==json("1"));
    CHECK(val[1][0]==json("4"));
}

TEST_CASE("csv_test1_array_1col_a")
{
    std::string text = "1\n4";
    std::istringstream is(text);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.assume_header(false);

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==2);
    CHECK(val[0].size()==1);
    CHECK(val[1].size()==1);
    CHECK(val[0][0]==json(1));
    CHECK(val[1][0]==json(4));
}

TEST_CASE("csv_test1_array_1col_b")
{
    std::string text = "1\n4";
    std::istringstream is(text);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.assume_header(false)
           .infer_types(false);

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==2);
    CHECK(val[0].size()==1);
    CHECK(val[1].size()==1);
    CHECK(val[0][0]==json("1"));
    CHECK(val[1][0]==json("4"));
}

TEST_CASE("csv_test1_array_3cols")
{
    std::string text = "a,b,c\n1,2,3\n4,5,6";
    std::istringstream is(text);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.assume_header(false);

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==3);
    CHECK(val[0].size()==3);
    CHECK(val[1].size()==3);
    CHECK(val[2].size()==3);
    CHECK(val[0][0]==json("a"));
    CHECK(val[0][1]==json("b"));
    CHECK(val[0][2]==json("c"));
    CHECK(val[1][0]==json(1));
    CHECK(val[1][1]==json(2));
    CHECK(val[1][2]==json(3));
    CHECK(val[2][0]==json(4));
    CHECK(val[2][1]==json(5));
    CHECK(val[2][2]==json(6));
}
TEST_CASE("csv_test1_array_3cols_trim_leading")
{
    std::string text = "a ,b ,c \n 1, 2, 3\n 4 , 5 , 6 ";
    std::istringstream is(text);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.assume_header(false)
           .trim_leading(true);

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==3);
    CHECK(val[0].size()==3);
    CHECK(val[1].size()==3);
    CHECK(val[2].size()==3);
    CHECK(val[0][0]==json("a "));
    CHECK(val[0][1]==json("b "));
    CHECK(val[0][2]==json("c "));
    CHECK(val[1][0]==json(1));
    CHECK(val[1][1]==json(2));
    CHECK(val[1][2]==json(3));
    CHECK(val[2][0]==json("4 "));
    CHECK(val[2][1]==json("5 "));
    CHECK(val[2][2]==json("6 "));
}

TEST_CASE("csv_test1_array_3cols_trim_trailing")
{
    std::string text = "a ,b ,c \n 1, 2, 3\n 4 , 5 , 6 ";
    std::istringstream is(text);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.assume_header(false)
           .trim_trailing(true);

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==3);
    CHECK(val[0].size()==3);
    CHECK(val[1].size()==3);
    CHECK(val[2].size()==3);
    CHECK(val[0][0]==json("a"));
    CHECK(val[0][1]==json("b"));
    CHECK(val[0][2]==json("c"));
    CHECK(json(" 1") == val[1][0]);
    CHECK(val[1][1]==json(" 2"));
    CHECK(val[1][2]==json(" 3"));
    CHECK(val[2][0]==json(" 4"));
    CHECK(val[2][1]==json(" 5"));
    CHECK(val[2][2]==json(" 6"));
}

TEST_CASE("csv_test1_array_3cols_trim")
{
    std::string text = "a ,, \n 1, 2, 3\n 4 , 5 , 6 ";
    std::istringstream is(text);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.assume_header(false)
           .trim(true)
           .unquoted_empty_value_is_null(true);

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==3);
    CHECK(val[0].size()==3);
    CHECK(val[1].size()==3);
    CHECK(val[2].size()==3);
    CHECK(val[0][0]==json("a"));
    CHECK(val[0][1]==json::null());
    CHECK(val[0][2]==json::null());
    CHECK(val[1][0]==json(1));
    CHECK(val[1][1]==json(2));
    CHECK(val[1][2]==json(3));
    CHECK(val[2][0]==json(4));
    CHECK(val[2][1]==json(5));
    CHECK(val[2][2]==json(6));
}

TEST_CASE("csv_test1_array_3cols_comment")
{
    std::string text = "a,b,c\n#1,2,3\n4,5,6";
    std::istringstream is(text);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.comment_starter('#');

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==2);
    CHECK(val[0].size()==3);
    CHECK(val[1].size()==3);
    CHECK(val[0][0]==json("a"));
    CHECK(val[0][1]==json("b"));
    CHECK(val[0][2]==json("c"));
    CHECK(val[1][0]==json(4));
    CHECK(val[1][1]==json(5));
    CHECK(val[1][2]==json(6));
}

TEST_CASE("csv_test1_object_1col")
{
    std::string text = "a\n1\n4";
    std::istringstream is(text);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.assume_header(true);

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==2);
    CHECK(val[0].size()==1);
    CHECK(val[1].size()==1);
    CHECK(val[0]["a"]==json(1));
    CHECK(val[1]["a"]==json(4));
}

TEST_CASE("csv_test1_object_3cols")
{
    std::string text = "a,b,c\n1,2,3\n4,5,6";
    std::istringstream is(text);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.assume_header(true);

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==2);
    CHECK(val[0].size()==3);
    CHECK(val[1].size()==3);
    CHECK(val[0]["a"]==json(1));
    CHECK(val[0]["b"]==json(2));
    CHECK(val[0]["c"]==json(3));
    CHECK(val[1]["a"]==json(4));
    CHECK(val[1]["b"]==json(5));
    CHECK(val[1]["c"]==json(6));
}

TEST_CASE("csv_test1_object_3cols_header")
{
    std::string text = "a,b,c\n1,2,3\n4,5,6";
    std::istringstream is(text);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.column_names("x,y,z")
           .header_lines(1);

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==2);
    CHECK(val[0].size()==3);
    CHECK(val[1].size()==3);
    CHECK(val[0]["x"]==json(1));
    CHECK(val[0]["y"]==json(2));
    CHECK(val[0]["z"]==json(3));
    CHECK(val[1]["x"]==json(4));
    CHECK(val[1]["y"]==json(5));
    CHECK(val[1]["z"]==json(6));
}

TEST_CASE("csv_test1_object_3cols_bool")
{
    std::string text = "a,b,c\n1,0,1\ntrue,FalSe,TrUe";
    std::istringstream is(text);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.column_names("x,y,z")
           .column_types("boolean,boolean,boolean")
           .header_lines(1);

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==2);
    CHECK(val[0].size()==3);
    CHECK(val[1].size()==3);
    CHECK(val[0]["x"]==json(true));
    CHECK(val[0]["y"]==json(false));
    CHECK(val[0]["z"]==json(true));
    CHECK(val[1]["x"]==json(true));
    CHECK(val[1]["y"]==json(false));
    CHECK(val[1]["z"]==json(true));
}

TEST_CASE("csv_test1_object_1col_quoted")
{
    std::string text = "a\n\"1\"\n\"4\"";
    std::istringstream is(text);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.assume_header(true);

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==2);
    CHECK(val[0].size()==1);
    CHECK(val[1].size()==1);
    CHECK(val[0]["a"]==json("1"));
    CHECK(val[1]["a"]==json("4"));
}

TEST_CASE("csv_test1_object_3cols_quoted")
{
    std::string text = "a,b,c\n\"1\",\"2\",\"3\"\n4,5,\"6\"";
    std::istringstream is(text);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.assume_header(true);

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==2);
    CHECK(val[0].size()==3);
    CHECK(val[1].size()==3);
    CHECK(val[0]["a"]==json("1"));
    CHECK(val[0]["b"]==json("2"));
    CHECK(val[0]["c"]==json("3"));
    CHECK(val[1]["a"]==json(4));
    CHECK(val[1]["b"]==json(5));
    CHECK(val[1]["c"]==json("6"));
}

TEST_CASE("csv_test1_array_1col_crlf")
{
    std::string text = "1\r\n4";
    std::istringstream is(text);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.assume_header(false);

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==2);
    CHECK(val[0].size()==1);
    CHECK(val[1].size()==1);
    CHECK(val[0][0]==json(1));
    CHECK(val[1][0]==json(4));
}

TEST_CASE("csv_test1_array_3cols_crlf")
{
    std::string text = "a,b,c\r\n1,2,3\r\n4,5,6";
    std::istringstream is(text);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.assume_header(false);

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==3);
    CHECK(val[0].size()==3);
    CHECK(val[1].size()==3);
    CHECK(val[2].size()==3);
    CHECK(val[0][0]==json("a"));
    CHECK(val[0][1]==json("b"));
    CHECK(val[0][2]==json("c"));
    CHECK(val[1][0]==json(1));
    CHECK(val[1][1]==json(2));
    CHECK(val[1][2]==json(3));
    CHECK(val[2][0]==json(4));
    CHECK(val[2][1]==json(5));
    CHECK(val[2][2]==json(6));
}

TEST_CASE("csv_test1_object_1col_crlf")
{
    std::string text = "a\r\n1\r\n4";
    std::istringstream is(text);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.assume_header(true);

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==2);
    CHECK(val[0].size()==1);
    CHECK(val[1].size()==1);
    CHECK(val[0]["a"]==json(1));
    CHECK(val[1]["a"]==json(4));
}

TEST_CASE("csv_test1_object_3cols_crlf")
{
    std::string text = "a,b,c\r\n1,2,3\r\n4,5,6";
    std::istringstream is(text);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.assume_header(true);

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==2);
    CHECK(val[0].size()==3);
    CHECK(val[1].size()==3);
    CHECK(val[0]["a"]==json(1));
    CHECK(val[0]["b"]==json(2));
    CHECK(val[0]["c"]==json(3));
    CHECK(val[1]["a"]==json(4));
    CHECK(val[1]["b"]==json(5));
    CHECK(val[1]["c"]==json(6));
}

TEST_CASE("read_comma_delimited_file")
{
    std::string in_file = "./input/countries.csv";
    std::ifstream is(in_file);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.assume_header(true);

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json countries = decoder.get_result();

    CHECK(4 == countries.size());
    CHECK(json("ABW") == countries[0]["country_code"]);
    CHECK(json("ARUBA") ==countries[0]["name"]);
    CHECK(json("ATF") == countries[1]["country_code"]);
    CHECK(json("FRENCH SOUTHERN TERRITORIES, D.R. OF") == countries[1]["name"]);
    CHECK(json("VUT") == countries[2]["country_code"]);
    CHECK(json("VANUATU") ==countries[2]["name"]);
    CHECK(json("WLF") == countries[3]["country_code"]);
    CHECK(json("WALLIS & FUTUNA ISLANDS") == countries[3]["name"]);
}

TEST_CASE("read_comma_delimited_file_header")
{
    std::string in_file = "./input/countries.csv";
    std::ifstream is(in_file);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.column_names("Country Code,Name")
           .header_lines(1);

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json countries = decoder.get_result();
    CHECK(4 == countries.size());
    CHECK(json("ABW") == countries[0]["Country Code"]);
    CHECK(json("ARUBA") ==countries[0]["Name"]);
    CHECK(json("ATF") == countries[1]["Country Code"]);
    CHECK(json("FRENCH SOUTHERN TERRITORIES, D.R. OF") == countries[1]["Name"]);
    CHECK(json("VUT") == countries[2]["Country Code"]);
    CHECK(json("VANUATU") == countries[2]["Name"]);
    CHECK(json("WLF") == countries[3]["Country Code"]);
    CHECK(json("WALLIS & FUTUNA ISLANDS") == countries[3]["Name"]);
}
 
TEST_CASE("serialize_comma_delimited_file")
{
    std::string in_file = "./input/countries.json";
    std::ifstream is(in_file);

    csv::csv_options options;
    options.assume_header(false);

    json_decoder<ojson> encoder1;
    json_reader reader1(is,encoder1);
    reader1.read();
    ojson countries1 = encoder1.get_result();

    std::stringstream ss;
    csv::csv_encoder encoder(ss,options);
    countries1.dump(encoder);

    json_decoder<ojson> encoder2;
    csv::csv_reader reader2(ss,encoder2,options);
    reader2.read();
    ojson countries2 = encoder2.get_result();

    CHECK(countries1 == countries2);
}

TEST_CASE("test_tab_delimited_file")
{
    std::string in_file = "./input/employees.txt";
    std::ifstream is(in_file);

    json_decoder<json> decoder;
    csv::csv_options options;
    options.field_delimiter('\t')
           .assume_header(true);

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json employees = decoder.get_result();
    CHECK(4 == employees.size());
    CHECK(std::string("00000001") ==employees[0]["employee-no"].as<std::string>());
    CHECK(std::string("00000002") ==employees[1]["employee-no"].as<std::string>());
    CHECK(std::string("00000003") ==employees[2]["employee-no"].as<std::string>());
    CHECK(std::string("00000004") ==employees[3]["employee-no"].as<std::string>());
}

TEST_CASE("serialize_tab_delimited_file")
{
    std::string in_file = "./input/employees.json";
    std::ifstream is(in_file);

    json_decoder<ojson> decoder;
    csv::csv_options options;
    options.assume_header(false)
           .header_lines(1)
           .column_names("dept,employee-name,employee-no,note,comment,salary")
           .field_delimiter('\t');

    json_reader reader(is,decoder);
    reader.read_next();
    ojson employees1 = decoder.get_result();

    std::stringstream ss;
    csv::csv_encoder encoder(ss,options);
    //std::cout << pretty_print(employees1) << std::endl;
    employees1.dump(encoder);
    //std::cout << ss.str() << std::endl;

    json_decoder<ojson> encoder2;
    csv::csv_reader reader2(ss,encoder2,options);
    reader2.read();
    ojson employees2 = encoder2.get_result();
    //std::cout << pretty_print(employees2) << std::endl;

    CHECK(employees1.size() == employees2.size());

    for (size_t i = 0; i < employees1.size(); ++i)
    {
        CHECK(employees1[i]["dept"] == employees2[i]["dept"]);
        CHECK(employees1[i]["employee-name"] ==employees2[i]["employee-name"]);
        CHECK(employees1[i]["employee-no"] ==employees2[i]["employee-no"]);
        CHECK(employees1[i]["salary"] == employees2[i]["salary"]);
        CHECK(employees1[i].get_with_default("note","") == employees2[i].get_with_default("note",""));
    }
}

TEST_CASE("csv_test1_array_3cols_grouped1")
{
    std::string text = "1,2,3\n4,5,6\n7,8,9";
    std::istringstream is(text);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.assume_header(false)
           .column_types("integer,[integer]*");

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    //std::cout << val << std::endl;

    /*CHECK(val.size()==3);
    CHECK(val[0].size()==3);
    CHECK(val[1].size()==3);
    CHECK(val[2].size()==3);
    CHECK(val[0][0]==json(1));
    CHECK(val[0][1]==json(2));
    CHECK(val[0][2]==json(3));
    CHECK(val[1][0]==json(4));
    CHECK(val[1][1]==json(5));
    CHECK(val[1][2]==json(6));
    CHECK(val[2][0]==json(7));
    CHECK(val[2][1]==json(8));
    CHECK(val[2][2]==json(9));*/
}

TEST_CASE("csv_test1_array_3cols_grouped2")
{
    std::string text = "1,2,3,4,5\n4,5,6,7,8\n7,8,9,10,11";
    std::istringstream is(text);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.assume_header(false)
           .column_types("integer,[integer,integer]*");

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    //std::cout << val << std::endl;
/*
    REQUIRE(options.column_types().size() == 4);
    CHECK(options.column_types()[0].first == csv::csv_column_type::integer_t);
    CHECK(options.column_types()[0].second == 0);
    CHECK(options.column_types()[1].first == csv::csv_column_type::integer_t);
    CHECK(options.column_types()[1].second == 1);
    CHECK(options.column_types()[2].first == csv::csv_column_type::integer_t);
    CHECK(options.column_types()[2].second == 1);
    CHECK(options.column_types()[3].first == csv::csv_column_type::repeat_t);
    CHECK(options.column_types()[3].second == 2);
*/
}

TEST_CASE("csv_test1_repeat")
{
    const std::string text = R"(Date,1Y,2Y,3Y,5Y
    2017-01-09,0.0062,0.0075,0.0083,0.011,0.012
    2017-01-08,0.0063,0.0076,0.0084,0.0112,0.013
    2017-01-08,0.0063,0.0076,0.0084,0.0112,0.014
    )";    

    auto result = csv::csv_options::parse_column_types("string,float*");
    REQUIRE(result.size() == 3);
    CHECK(result[0].col_type == csv::csv_column_type::string_t);
    CHECK(result[0].level == 0);
    CHECK(0 == result[0].rep_count);
    CHECK(result[1].col_type == csv::csv_column_type::float_t);
    CHECK(result[1].level == 0);
    CHECK(0 == result[1].rep_count);
    CHECK(result[2].col_type == csv::csv_column_type::repeat_t);
    CHECK(result[2].level == 0);
    CHECK(1 == result[2].rep_count);

    auto result2 = csv::csv_options::parse_column_types("string,[float*]");
    REQUIRE(result2.size() == 3);
    CHECK(result2[0].col_type == csv::csv_column_type::string_t);
    CHECK(result2[0].level == 0);
    CHECK(0 == result2[0].rep_count);
    CHECK(result2[1].col_type == csv::csv_column_type::float_t);
    CHECK(result2[1].level == 1);
    CHECK(0 == result2[1].rep_count);
    CHECK(result2[2].col_type == csv::csv_column_type::repeat_t);
    CHECK(result2[2].level == 1);
    CHECK(1 == result2[2].rep_count);

    auto result3 = csv::csv_options::parse_column_types("string,[float]*");
    REQUIRE(result3.size() == 3);
    CHECK(result3[0].col_type == csv::csv_column_type::string_t);
    CHECK(result3[0].level == 0);
    CHECK(0 == result3[0].rep_count);
    CHECK(result3[1].col_type == csv::csv_column_type::float_t);
    CHECK(result3[1].level == 1);
    CHECK(0 == result3[1].rep_count);
    CHECK(result3[2].col_type == csv::csv_column_type::repeat_t);
    CHECK(result3[2].level == 0);
    CHECK(1 == result3[2].rep_count);
}

TEST_CASE("csv_test1_repeat2")
{
    csv::csv_options options1;
    options1.column_types("[integer,string]*");
    for (auto x : options1.column_types())
    {
        std::cout << (int)x.col_type << " " << x.level << " " << x.rep_count << std::endl;
    }
}

TEST_CASE("empty_line_test_1")
{
    std::string input = R"(country_code,name
ABW,ARUBA

ATF,"FRENCH SOUTHERN TERRITORIES, D.R. OF"
VUT,VANUATU
WLF,WALLIS & FUTUNA ISLANDS
)";

    std::istringstream is(input);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.assume_header(true);

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json j = decoder.get_result();
    CHECK(j.size() == 4);

    std::cout << pretty_print(j) << std::endl;
}

TEST_CASE("empty_line_test_2")
{
    std::string input = R"(country_code,name
ABW,ARUBA

ATF,"FRENCH SOUTHERN TERRITORIES, D.R. OF"
VUT,VANUATU
WLF,WALLIS & FUTUNA ISLANDS
)";

    std::istringstream is(input);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.assume_header(true)
           .ignore_empty_lines(false);

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json j = decoder.get_result();
    CHECK(j.size() == 5);

    std::cout << pretty_print(j) << std::endl;
}

TEST_CASE("line_with_one_space")
{
    std::string input = R"(country_code,name
ABW,ARUBA
\t
ATF,"FRENCH SOUTHERN TERRITORIES, D.R. OF"
VUT,VANUATU
WLF,WALLIS & FUTUNA ISLANDS
)";

    std::istringstream is(input);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.assume_header(true);

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json j = decoder.get_result();
    CHECK(j.size() == 5);

    std::cout << pretty_print(j) << std::endl;
}

TEST_CASE("line_with_one_space_and_trim")
{
    std::string input = R"(country_code,name
ABW,ARUBA
 
ATF,"FRENCH SOUTHERN TERRITORIES, D.R. OF"
VUT,VANUATU
WLF,WALLIS & FUTUNA ISLANDS
)";

    std::istringstream is(input);

    json_decoder<json> decoder;

    csv::csv_options options;
    options.assume_header(true)
           .trim(true);

    csv::csv_reader reader(is,decoder,options);
    reader.read();
    json j = decoder.get_result();
    CHECK(j.size() == 4);

    std::cout << pretty_print(j) << std::endl;
}

TEST_CASE("test_decode_csv_from_string")
{
    std::string s = "some label\nsome value";
    csv::csv_options options;
    options.assume_header(true);
    std::cout << csv::decode_csv<json>(s,options) << std::endl;
}

TEST_CASE("test_decode_csv_from_stream")
{
    std::string s = "some label\nsome value";
    std::stringstream is(s);
    csv::csv_options options;
    options.assume_header(true);
    std::cout << csv::decode_csv<json>(is,options) << std::endl;
}

TEST_CASE("test_encode_csv_to_stream")
{
    json j = json::array();
    j.push_back(json::object({ {"a",1},{"b",2} }));
    std::cout << j << std::endl;
    csv::csv_options options;
    options.assume_header(true);
    std::ostringstream os;
    csv::encode_csv(j, os, options);
    std::cout << os.str() << std::endl;
}

TEST_CASE("test_type_inference")
{
    const std::string input = R"(customer_name,has_coupon,phone_number,zip_code,sales_tax_rate,total_amount
"John Roe",true,0272561313,01001,0.05,431.65
"Jane Doe",false,416-272-2561,55416,0.15,480.70
"Joe Bloggs",false,"4162722561","55416",0.15,300.70
"John Smith",FALSE,NULL,22313-1450,0.15,300.70
)";

    std::cout << input << std::endl;

    json_decoder<ojson> decoder;

    csv::csv_options options;
    options.assume_header(true)
           .mapping(csv::mapping_type::n_rows);

    ojson j1 = csv::decode_csv<ojson>(input,options);
    std::cout << "\n(1)\n"<< pretty_print(j1) << "\n";
    //CHECK(j1.size() == 4);

    options.mapping(csv::mapping_type::n_objects);
    ojson j2 = csv::decode_csv<ojson>(input,options);
    std::cout << "\n(2)\n"<< pretty_print(j2) << "\n";

    options.mapping(csv::mapping_type::m_columns);
    ojson j3 = csv::decode_csv<ojson>(input,options);
    std::cout << "\n(3)\n"<< pretty_print(j3) << "\n";
    //REQUIRE(j2.size() == 3);
    //CHECK("2017-01-09" == j2[0]["Date"].as<std::string>());
}

TEST_CASE("csv_options lossless_number")
{
    const std::string input = R"(index_id,observation_date,rate
EUR_LIBOR_06M,2015-10-23,0.0000214
EUR_LIBOR_06M,2015-10-26,0.0000143
EUR_LIBOR_06M,2015-10-27,0.0000001
)";

    std::cout << input << std::endl;

    json_decoder<ojson> decoder;

    csv::csv_options options;
    options.assume_header(true)
           .mapping(csv::mapping_type::n_objects)
           .trim(true)
           .lossless_number(true);

    ojson j1 = csv::decode_csv<ojson>(input,options);
    std::cout << pretty_print(j1) << "\n";
    REQUIRE(j1.size() == 3);
    CHECK((j1[0]["rate"].as<std::string>() == "0.0000214"));
}

// Test case contributed by karimhm
TEST_CASE("csv detect bom")
{
    const std::string input = "\xEF\xBB\xBFstop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n"
"\"8220B007612\",\"Hotel Merrion Street\",\"53\",\"-6\",\"\",\"\"";

    csv::csv_options options;
    options.assume_header(true);
    
    std::istringstream is(input);
    ojson j = csv::decode_csv<ojson>(is, options);
    REQUIRE(j.size() == 1);
    ojson station = j[0];
    
    try {
        auto it = station.find("stop_id");
        REQUIRE((it != station.object_range().end()));
        std::string stop_id = it->value().as<std::string>();
        CHECK((stop_id == "8220B007612"));
    } catch (const std::exception& e) {
        std::cout << e.what() << std::endl;
    }
}

TEST_CASE("test_encode_decode csv string")
{
    typedef std::vector<std::tuple<std::string,int>> cpp_type;
    std::string s1 = "\"a\",1\n\"b\",2";
    csv::csv_options options;
    options.mapping(csv::mapping_type::n_rows)
           .assume_header(false);

    SECTION("string")
    {
        cpp_type v = csv::decode_csv<cpp_type>(s1, options);
        REQUIRE(v.size() == 2);
        CHECK(std::get<0>(v[0]) == "a");
        CHECK(std::get<1>(v[0]) == 1);
        CHECK(std::get<0>(v[1]) == "b");
        CHECK(std::get<1>(v[1]) == 2);

        std::string s2;
        csv::encode_csv(v, s2, options);

        json j1 = csv::decode_csv<json>(s1);
        json j2 = csv::decode_csv<json>(s2);

        CHECK(j1 == j2);
    }

    SECTION("stream")
    {
        std::stringstream ss1(s1);
        cpp_type v = csv::decode_csv<cpp_type>(ss1, options);
        REQUIRE(v.size() == 2);
        CHECK(std::get<0>(v[0]) == "a");
        CHECK(std::get<1>(v[0]) == 1);
        CHECK(std::get<0>(v[1]) == "b");
        CHECK(std::get<1>(v[1]) == 2);

        std::stringstream ss2;
        csv::encode_csv(v, ss2, options);

        json j1 = csv::decode_csv<json>(s1);
        json j2 = csv::decode_csv<json>(ss2);

        CHECK(j1 == j2);
    }
}

