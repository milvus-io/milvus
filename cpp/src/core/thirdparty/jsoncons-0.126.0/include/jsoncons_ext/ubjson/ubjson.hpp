// Copyright 2013 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_UBJSON_UBJSON_HPP
#define JSONCONS_UBJSON_UBJSON_HPP

#include <string>
#include <vector>
#include <memory>
#include <type_traits> // std::enable_if
#include <istream> // std::basic_istream
#include <jsoncons/json.hpp>
#include <jsoncons/config/binary_detail.hpp>
#include <jsoncons_ext/ubjson/ubjson_encoder.hpp>
#include <jsoncons_ext/ubjson/ubjson_reader.hpp>

namespace jsoncons { namespace ubjson {

// encode_ubjson

template<class T>
typename std::enable_if<is_basic_json_class<T>::value,void>::type 
encode_ubjson(const T& j, std::vector<uint8_t>& v)
{
    typedef typename T::char_type char_type;
    ubjson_bytes_encoder encoder(v);
    auto adaptor = make_json_content_handler_adaptor<basic_json_content_handler<char_type>>(encoder);
    j.dump(adaptor);
}

template<class T>
typename std::enable_if<!is_basic_json_class<T>::value,void>::type 
encode_ubjson(const T& val, std::vector<uint8_t>& v)
{
    ubjson_bytes_encoder encoder(v);
    write_to(json(), val, encoder);
}

template<class T>
typename std::enable_if<is_basic_json_class<T>::value,void>::type 
encode_ubjson(const T& j, std::ostream& os)
{
    typedef typename T::char_type char_type;
    ubjson_encoder encoder(os);
    auto adaptor = make_json_content_handler_adaptor<basic_json_content_handler<char_type>>(encoder);
    j.dump(adaptor);
}

template<class T>
typename std::enable_if<!is_basic_json_class<T>::value,void>::type 
encode_ubjson(const T& val, std::ostream& os)
{
    ubjson_encoder encoder(os);
    write_to(json(), val, encoder);
}

// decode_ubjson

template<class T>
typename std::enable_if<is_basic_json_class<T>::value,T>::type 
decode_ubjson(const std::vector<uint8_t>& v)
{
    jsoncons::json_decoder<T> decoder;
    auto adaptor = make_json_content_handler_adaptor<json_content_handler>(decoder);
    basic_ubjson_reader<jsoncons::bytes_source> reader(v, adaptor);
    reader.read();
    return decoder.get_result();
}

template<class T>
typename std::enable_if<!is_basic_json_class<T>::value,T>::type 
decode_ubjson(const std::vector<uint8_t>& v)
{
    jsoncons::json_decoder<json> decoder;
    basic_ubjson_reader<jsoncons::bytes_source> reader(v, decoder);
    reader.read();
    return decoder.get_result().template as<T>();
}

template<class T>
typename std::enable_if<is_basic_json_class<T>::value,T>::type 
decode_ubjson(std::istream& is)
{
    jsoncons::json_decoder<T> decoder;
    auto adaptor = make_json_content_handler_adaptor<json_content_handler>(decoder);
    ubjson_reader reader(is, adaptor);
    reader.read();
    return decoder.get_result();
}

template<class T>
typename std::enable_if<!is_basic_json_class<T>::value,T>::type 
decode_ubjson(std::istream& is)
{
    jsoncons::json_decoder<json> decoder;
    ubjson_reader reader(is, decoder);
    reader.read();
    return decoder.get_result();
}

}}

#endif
