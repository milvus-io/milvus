// Copyright 2017 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_JSON_CONVERSION_TRAITS_HPP
#define JSONCONS_JSON_CONVERSION_TRAITS_HPP

#include <string>
#include <tuple>
#include <array>
#include <memory>
#include <type_traits> // std::enable_if
#include <jsoncons/json_content_handler.hpp>
#include <jsoncons/json_decoder.hpp>
#include <jsoncons/basic_json.hpp>
#include <jsoncons/json_options.hpp>
#include <jsoncons/json_encoder.hpp>
#include <jsoncons/json_type_traits.hpp>
#include <jsoncons/staj_reader.hpp>

namespace jsoncons {

template <class T, class CharT, class Json>
void read_from(const Json& j, basic_staj_reader<CharT>& reader, T& val, std::error_code& ec);

template <class T, class CharT, class Json>
void read_from(const Json& j, basic_staj_reader<CharT>& reader, T& val)
{
    std::error_code ec;
    read_from(j, reader, val, ec);
    if (ec)
    {
        throw ser_error(ec, reader.context().line(), reader.context().column());
    }
}

template <class T, class CharT, class Json>
void write_to(const T&val, basic_json_content_handler<CharT>& receiver);

} // namespace jsoncons

#include <jsoncons/staj_iterator.hpp>

namespace jsoncons {

template <class T, class Enable = void>
struct json_conversion_traits
{
    template <class CharT, class Json>
    static T decode(basic_staj_reader<CharT>& reader, std::error_code& ec)
    {
        json_decoder<Json> decoder;
        reader.accept(decoder, ec);
        return decoder.get_result().template as<T>();
    }

    template <class CharT, class Json>
    static void encode(const T& val, basic_json_content_handler<CharT>& receiver)
    {
        auto j = json_type_traits<Json, T>::to_json(val);
        j.dump(receiver);
    }
};

// specializations

// vector like

template <class T>
struct json_conversion_traits<T,
    typename std::enable_if<!is_json_type_traits_declared<T>::value && jsoncons::detail::is_vector_like<T>::value
>::type>
{
    typedef typename T::value_type value_type;

    template <class CharT, class Json>
    static T decode(basic_staj_reader<CharT>& reader, std::error_code& ec)
    {
        T v;
        basic_staj_array_iterator<value_type,CharT,Json> end;
        basic_staj_array_iterator<value_type,CharT,Json> it(reader, ec);

        while (it != end && !ec)
        {
            v.push_back(*it);
            it.increment(ec);
        }
        return v;
    }

    template <class CharT, class Json>
    static void encode(const T& val, basic_json_content_handler<CharT>& receiver)
    {
        receiver.begin_array();
        for (auto it = std::begin(val); it != std::end(val); ++it)
        {
            json_conversion_traits<value_type>::template encode<CharT,Json>(*it,receiver);
        }
        receiver.end_array();
        receiver.flush();
    }
};
// std::array

template <class T, size_t N>
struct json_conversion_traits<std::array<T,N>>
{
    typedef typename std::array<T,N>::value_type value_type;

    template <class CharT,class Json>
    static std::array<T, N> decode(basic_staj_reader<CharT>& reader, std::error_code& ec)
    {
        std::array<T,N> v;
        v.fill(T{});
        basic_staj_array_iterator<value_type,CharT,Json> end;
        basic_staj_array_iterator<value_type,CharT,Json> it(reader, ec);

        for (size_t i = 0; it != end && i < N && !ec; ++i)
        {
            v[i] = *it;
            it.increment(ec);
        }
        return v;
    }

    template <class CharT, class Json>
    static void encode(const std::array<T, N>& val, basic_json_content_handler<CharT>& receiver)
    {
        receiver.begin_array();
        for (auto it = std::begin(val); it != std::end(val); ++it)
        {
            json_conversion_traits<value_type>::template encode<CharT,Json>(*it,receiver);
        }
        receiver.end_array();
        receiver.flush();
    }
};

// map like

template <class T>
struct json_conversion_traits<T,
    typename std::enable_if<!is_json_type_traits_declared<T>::value && jsoncons::detail::is_map_like<T>::value
>::type>
{
    typedef typename T::mapped_type mapped_type;
    typedef typename T::value_type value_type;
    typedef typename T::key_type key_type;

    template <class CharT, class Json>
    static T decode(basic_staj_reader<CharT>& reader, std::error_code& ec)
    {
        T m;
        basic_staj_object_iterator<mapped_type,CharT,Json> end;
        basic_staj_object_iterator<mapped_type,CharT,Json> it(reader, ec);

        while (it != end && !ec)
        {
            m.emplace(it->first,it->second);
            it.increment(ec);
        }
        return m;
    }

    template <class CharT, class Json>
    static void encode(const T& val, basic_json_content_handler<CharT>& receiver)
    {
        receiver.begin_object();
        for (auto it = std::begin(val); it != std::end(val); ++it)
        {
            receiver.name(it->first);
            json_conversion_traits<mapped_type>::template encode<CharT,Json>(it->second,receiver);
        }
        receiver.end_object();
        receiver.flush();
    }
};

template <class T, class CharT, class Json>
void read_from(const Json&, basic_staj_reader<CharT>& reader, T& val, std::error_code& ec)
{
    val = json_conversion_traits<T>::template decode<CharT,Json>(reader,ec);
}

template <class T, class CharT, class Json>
void write_to(const Json&, const T&val, basic_json_content_handler<CharT>& receiver)
{
    json_conversion_traits<T>::template encode<CharT,Json>(val, receiver);
}

}

#endif

