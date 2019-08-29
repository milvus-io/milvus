// Copyright 2017 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_ENCODE_DECODE_JSON_HPP
#define JSONCONS_ENCODE_DECODE_JSON_HPP

#include <iostream>
#include <string>
#include <tuple>
#include <memory>
#include <istream> // std::basic_istream
#include <jsoncons/json_conversion_traits.hpp>
#include <jsoncons/json_cursor.hpp>

namespace jsoncons {

// decode_json 

template <class T, class CharT>
typename std::enable_if<is_basic_json_class<T>::value,T>::type
decode_json(const std::basic_string<CharT>& s,
            const basic_json_decode_options<CharT>& options = basic_json_options<CharT>::default_options())
{
    jsoncons::json_decoder<T> decoder;
    basic_json_reader<CharT, string_source<CharT>> reader(s, decoder, options);
    reader.read();
    return decoder.get_result();
}

template <class T, class CharT>
typename std::enable_if<!is_basic_json_class<T>::value,T>::type
decode_json(const std::basic_string<CharT>& s,
            const basic_json_decode_options<CharT>& options = basic_json_options<CharT>::default_options())
{
    basic_json_cursor<CharT> reader(s, options);
    T val;
    read_from(basic_json<CharT>(), reader, val);
    return val;
}

template <class T, class CharT>
typename std::enable_if<is_basic_json_class<T>::value,T>::type
decode_json(std::basic_istream<CharT>& is,
            const basic_json_decode_options<CharT>& options = basic_json_options<CharT>::default_options())
{
    jsoncons::json_decoder<T> decoder;
    basic_json_reader<CharT, stream_source<CharT>> reader(is, decoder, options);
    reader.read();
    return decoder.get_result();
}

template <class T, class CharT>
typename std::enable_if<!is_basic_json_class<T>::value,T>::type
decode_json(std::basic_istream<CharT>& is,
            const basic_json_decode_options<CharT>& options = basic_json_options<CharT>::default_options())
{
    basic_json_cursor<CharT> reader(is, options);
    T val;
    read_from(basic_json<CharT>(), reader, val);
    return val;
}

template <class T, class CharT, class ImplementationPolicy, class Allocator>
T decode_json(const basic_json<CharT,ImplementationPolicy,Allocator>& j,
              const std::basic_string<CharT>& s,
              const basic_json_decode_options<CharT>& options = basic_json_options<CharT>::default_options())
{
    basic_json_cursor<CharT> reader(s, options);
    T val;
    read_from(j, reader, val);
    return val;
}

template <class T, class CharT, class ImplementationPolicy, class Allocator>
T decode_json(const basic_json<CharT,ImplementationPolicy,Allocator>& j,
              std::basic_istream<CharT>& is,
              const basic_json_decode_options<CharT>& options = basic_json_options<CharT>::default_options())
{
    basic_json_cursor<CharT> reader(is, options);
    T val;
    read_from(j, reader, val);
    return val;
}

// encode_json

template <class T, class CharT>
void encode_json(const T& val, 
                 basic_json_content_handler<CharT>& receiver)
{
    write_to(basic_json<CharT>(), val, receiver);
    receiver.flush();
}

// to stream

template <class T, class CharT>
typename std::enable_if<is_basic_json_class<T>::value>::type
encode_json(const T& val,
            std::basic_ostream<CharT>& os, 
            const basic_json_encode_options<CharT>& options = basic_json_options<CharT>::default_options(), 
            indenting line_indent = indenting::no_indent)
{
    if (line_indent == indenting::indent)
    {
        basic_json_encoder<CharT> encoder(os, options);
        val.dump(encoder);
    }
    else
    {
        basic_json_compressed_encoder<CharT> encoder(os, options);
        val.dump(encoder);
    }
}

template <class T, class CharT>
typename std::enable_if<!is_basic_json_class<T>::value>::type
encode_json(const T& val,
            std::basic_ostream<CharT>& os, 
            const basic_json_encode_options<CharT>& options = basic_json_options<CharT>::default_options(), 
            indenting line_indent = indenting::no_indent)
{
    if (line_indent == indenting::indent)
    {
        basic_json_encoder<CharT> encoder(os, options);
        encode_json(val, encoder);
    }
    else
    {
        basic_json_compressed_encoder<CharT> encoder(os, options);
        encode_json(val, encoder);
    }
}

template <class T, class CharT>
void encode_json(const T& val, 
                 std::basic_ostream<CharT>& os, 
                 indenting line_indent)
{
    encode_json(val, os, basic_json_options<CharT>::default_options(), line_indent);
}

// to string

template <class T, class CharT>
typename std::enable_if<is_basic_json_class<T>::value>::type
encode_json(const T& val,
            std::basic_string<CharT>& s, 
            const basic_json_encode_options<CharT>& options = basic_json_options<CharT>::default_options(), 
            indenting line_indent = indenting::no_indent)
{
    if (line_indent == indenting::indent)
    {
        basic_json_encoder<CharT,jsoncons::string_result<std::basic_string<CharT>>> encoder(s, options);
        val.dump(encoder);
    }
    else
    {
        basic_json_compressed_encoder<CharT, jsoncons::string_result<std::basic_string<CharT>>> encoder(s, options);
        val.dump(encoder);
    }
}

template <class T, class CharT>
typename std::enable_if<!is_basic_json_class<T>::value>::type
encode_json(const T& val,
            std::basic_string<CharT>& s, 
            const basic_json_encode_options<CharT>& options = basic_json_options<CharT>::default_options(), 
            indenting line_indent = indenting::no_indent)
{
    if (line_indent == indenting::indent)
    {
        basic_json_encoder<CharT,jsoncons::string_result<std::basic_string<CharT>>> encoder(s, options);
        encode_json(val, encoder);
    }
    else
    {
        basic_json_compressed_encoder<CharT, jsoncons::string_result<std::basic_string<CharT>>> encoder(s, options);
        encode_json(val, encoder);
    }
}

template <class T, class CharT>
void encode_json(const T& val, 
                 std::basic_string<CharT>& s, 
                 indenting line_indent)
{
    encode_json(val, s, basic_json_options<CharT>::default_options(), line_indent);
}

template <class T, class CharT, class ImplementationPolicy, class Allocator>
void encode_json(const basic_json<CharT, ImplementationPolicy, Allocator>& j, 
                 const T& val,
                 basic_json_content_handler<CharT>& receiver)
{
    write_to(j, val, receiver);
    receiver.flush();
}

template <class T, class CharT, class ImplementationPolicy, class Allocator>
void encode_json(const basic_json<CharT,ImplementationPolicy,Allocator>& j,
                 const T& val,
                 std::basic_ostream<CharT>& os, 
                 const basic_json_encode_options<CharT>& options = basic_json_options<CharT>::default_options(), 
                 indenting line_indent = indenting::no_indent)
{
    if (line_indent == indenting::indent)
    {
        basic_json_encoder<CharT> encoder(os, options);
        encode_json(j, val, encoder);
    }
    else
    {
        basic_json_compressed_encoder<CharT> encoder(os, options);
        encode_json(j, val, encoder);
    }
}

template <class T, class CharT, class ImplementationPolicy, class Allocator>
void encode_json(const basic_json<CharT,ImplementationPolicy,Allocator>& j,
                 const T& val,
                 std::basic_ostream<CharT>& os, 
                 indenting line_indent)
{
    if (line_indent == indenting::indent)
    {
        basic_json_encoder<CharT> encoder(os, basic_json_options<CharT>::default_options());
        encode_json(j, val, encoder);
    }
    else
    {
        basic_json_compressed_encoder<CharT> encoder(os, basic_json_options<CharT>::default_options());
        encode_json(j, val, encoder);
    }
}

template <class T, class CharT, class ImplementationPolicy, class Allocator>
void encode_json(const basic_json<CharT,ImplementationPolicy,Allocator>& j,
                 const T& val,
                 std::basic_string<CharT>& s, 
                 const basic_json_encode_options<CharT>& options = basic_json_options<CharT>::default_options(), 
                 indenting line_indent = indenting::no_indent)
{ 
    if (line_indent == indenting::indent)
    {
        basic_json_encoder<CharT,jsoncons::string_result<std::basic_string<CharT>>> encoder(s, options);
        encode_json(j, val, encoder);
    }
    else
    {
        basic_json_compressed_encoder<CharT,jsoncons::string_result<std::basic_string<CharT>>> encoder(s, options);
        encode_json(j, val, encoder);
    }
}

template <class T, class CharT, class ImplementationPolicy, class Allocator>
void encode_json(const basic_json<CharT,ImplementationPolicy,Allocator>& j,
                 const T& val,
                 std::basic_string<CharT>& s, 
                 indenting line_indent)
{ 
    if (line_indent == indenting::indent)
    {
        basic_json_encoder<CharT,jsoncons::string_result<std::basic_string<CharT>>> encoder(s, basic_json_options<CharT>::default_options());
        encode_json(j, val, encoder);
    }
    else
    {
        basic_json_compressed_encoder<CharT,jsoncons::string_result<std::basic_string<CharT>>> encoder(s, basic_json_options<CharT>::default_options());
        encode_json(j, val, encoder);
    }
}

}

#endif

