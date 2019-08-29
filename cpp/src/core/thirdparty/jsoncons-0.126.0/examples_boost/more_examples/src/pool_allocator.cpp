// Copyright 2018 Daniel Parker
// Distributed under Boost license

#include <boost/pool/pool_alloc.hpp>
#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <cstddef>

using namespace jsoncons;

typedef basic_json<char,sorted_policy,boost::pool_allocator<char>> my_json;

void pool_allocator_examples()
{
    std::cout << "pool_allocator examples\n\n";

    jsoncons::json_decoder<my_json,boost::pool_allocator<char>> decoder;

    static std::string s("[1,2,3,4,5,6]");
    std::istringstream is(s);

    basic_json_reader<char,stream_source<char>,boost::pool_allocator<char>> reader(is,decoder); 
    reader.read();

    my_json j = decoder.get_result();

    std::cout << j << std::endl;
}

