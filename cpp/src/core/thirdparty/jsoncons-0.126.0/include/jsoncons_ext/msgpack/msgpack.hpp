// Copyright 2013 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_MSGPACK_MSGPACK_HPP
#define JSONCONS_MSGPACK_MSGPACK_HPP

#include <string>
#include <vector>
#include <memory>
#include <type_traits> // std::enable_if
#include <istream> // std::basic_istream
#include <jsoncons/json.hpp>
#include <jsoncons/config/binary_detail.hpp>
#include <jsoncons_ext/msgpack/msgpack_encoder.hpp>
#include <jsoncons_ext/msgpack/msgpack_reader.hpp>

namespace jsoncons { namespace msgpack {

// encode_msgpack

template<class T>
typename std::enable_if<is_basic_json_class<T>::value,void>::type 
encode_msgpack(const T& j, std::vector<uint8_t>& v)
{
    typedef typename T::char_type char_type;
    msgpack_bytes_encoder encoder(v);
    auto adaptor = make_json_content_handler_adaptor<basic_json_content_handler<char_type>>(encoder);
    j.dump(adaptor);
}

template<class T>
typename std::enable_if<!is_basic_json_class<T>::value,void>::type 
encode_msgpack(const T& val, std::vector<uint8_t>& v)
{
    msgpack_bytes_encoder encoder(v);
    write_to(json(), val, encoder);
}

template<class T>
typename std::enable_if<is_basic_json_class<T>::value,void>::type 
encode_msgpack(const T& j, std::ostream& os)
{
    typedef typename T::char_type char_type;
    msgpack_encoder encoder(os);
    auto adaptor = make_json_content_handler_adaptor<basic_json_content_handler<char_type>>(encoder);
    j.dump(adaptor);
}

template<class T>
typename std::enable_if<!is_basic_json_class<T>::value,void>::type 
encode_msgpack(const T& val, std::ostream& os)
{
    msgpack_encoder encoder(os);
    write_to(json(), val, encoder);
}

// decode_msgpack

template<class T>
typename std::enable_if<is_basic_json_class<T>::value,T>::type 
decode_msgpack(const std::vector<uint8_t>& v)
{
    jsoncons::json_decoder<T> decoder;
    auto adaptor = make_json_content_handler_adaptor<json_content_handler>(decoder);
    basic_msgpack_reader<jsoncons::bytes_source> reader(v, adaptor);
    std::error_code ec;
    reader.read(ec);
    if (ec)
    {
        throw ser_error(ec,reader.line(),reader.column());
    }
    return decoder.get_result();
}

template<class T>
typename std::enable_if<!is_basic_json_class<T>::value,T>::type 
decode_msgpack(const std::vector<uint8_t>& v)
{
    jsoncons::json_decoder<json> decoder;
    basic_msgpack_reader<jsoncons::bytes_source> reader(v, decoder);
    reader.read();
    return decoder.get_result().template as<T>();
}

template<class T>
typename std::enable_if<is_basic_json_class<T>::value,T>::type 
decode_msgpack(std::istream& is)
{
    jsoncons::json_decoder<T> decoder;
    auto adaptor = make_json_content_handler_adaptor<json_content_handler>(decoder);
    msgpack_reader reader(is, adaptor);
    std::error_code ec;
    reader.read(ec);
    if (ec)
    {
        throw ser_error(ec,reader.line(),reader.column());
    }
    return decoder.get_result();
}

template<class T>
typename std::enable_if<!is_basic_json_class<T>::value,T>::type 
decode_msgpack(std::istream& is)
{
    jsoncons::json_decoder<json> decoder;
    msgpack_reader reader(is, decoder);
    reader.read();
    return decoder.get_result();
}
  
#if !defined(JSONCONS_NO_DEPRECATED)
template<class Json>
std::vector<uint8_t> encode_msgpack(const Json& j)
{
    std::vector<uint8_t> v;
    encode_msgpack(j, v);
    return v;
}
#endif

}}

#endif
