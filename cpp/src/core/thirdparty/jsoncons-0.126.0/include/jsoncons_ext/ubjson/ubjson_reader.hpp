// Copyright 2017 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_UBJSON_UBJSON_READER_HPP
#define JSONCONS_UBJSON_UBJSON_READER_HPP

#include <string>
#include <memory>
#include <utility> // std::move
#include <jsoncons/json.hpp>
#include <jsoncons/source.hpp>
#include <jsoncons/json_content_handler.hpp>
#include <jsoncons/config/binary_detail.hpp>
#include <jsoncons_ext/ubjson/ubjson_detail.hpp>
#include <jsoncons_ext/ubjson/ubjson_error.hpp>

namespace jsoncons { namespace ubjson {

template <class Src>
class basic_ubjson_reader : public ser_context
{
    Src source_;
    json_content_handler& handler_;
    size_t nesting_depth_;
    std::string buffer_;
public:
    template <class Source>
    basic_ubjson_reader(Source&& source, json_content_handler& handler)
       : source_(std::forward<Source>(source)),
         handler_(handler), 
         nesting_depth_(0)
    {
    }

    void read()
    {
        std::error_code ec;
        read(ec);
        if (ec)
        {
            throw ser_error(ec,line(),column());
        }
    }

    void read(std::error_code& ec)
    {
        try
        {
            read_internal(ec);
        }
        catch (const ser_error& e)
        {
            ec = e.code();
        }
    }

    size_t line() const override
    {
        return 0;
    }

    size_t column() const override
    {
        return source_.position();
    }
private:

    void read_internal(std::error_code& ec)
    {
        if (source_.is_error())
        {
            ec = ubjson_errc::source_error;
            return;
        }   
        //const uint8_t* pos = input_ptr_++;

        uint8_t type{};
        if (source_.get(type) == 0)
        {
            ec = ubjson_errc::unexpected_eof;
            return;
        }
        read_value(type, ec);
    }

    void read_value(uint8_t type, std::error_code& ec)
    {
        switch (type)
        {
            case jsoncons::ubjson::detail::ubjson_format::null_type: 
            {
                handler_.null_value(semantic_tag::none, *this);
                break;
            }
            case jsoncons::ubjson::detail::ubjson_format::no_op_type: 
            {
                break;
            }
            case jsoncons::ubjson::detail::ubjson_format::true_type:
            {
                handler_.bool_value(true, semantic_tag::none, *this);
                break;
            }
            case jsoncons::ubjson::detail::ubjson_format::false_type:
            {
                handler_.bool_value(false, semantic_tag::none, *this);
                break;
            }
            case jsoncons::ubjson::detail::ubjson_format::int8_type: 
            {
                uint8_t buf[sizeof(int8_t)];
                source_.read(buf, sizeof(int8_t));
                if (source_.eof())
                {
                    ec = ubjson_errc::unexpected_eof;
                    return;
                }
                const uint8_t* endp;
                int8_t val = jsoncons::detail::from_big_endian<int8_t>(buf,buf+sizeof(buf),&endp);
                handler_.int64_value(val, semantic_tag::none, *this);
                break;
            }
            case jsoncons::ubjson::detail::ubjson_format::uint8_type: 
            {
                uint8_t val{};
                if (source_.get(val) == 0)
                {
                    ec = ubjson_errc::unexpected_eof;
                    return;
                }
                handler_.uint64_value(val, semantic_tag::none, *this);
                break;
            }
            case jsoncons::ubjson::detail::ubjson_format::int16_type: 
            {
                uint8_t buf[sizeof(int16_t)];
                source_.read(buf, sizeof(int16_t));
                if (source_.eof())
                {
                    ec = ubjson_errc::unexpected_eof;
                    return;
                }
                const uint8_t* endp;
                int16_t val = jsoncons::detail::from_big_endian<int16_t>(buf,buf+sizeof(buf),&endp);
                handler_.int64_value(val, semantic_tag::none, *this);
                break;
            }
            case jsoncons::ubjson::detail::ubjson_format::int32_type: 
            {
                uint8_t buf[sizeof(int32_t)];
                source_.read(buf, sizeof(int32_t));
                if (source_.eof())
                {
                    ec = ubjson_errc::unexpected_eof;
                    return;
                }
                const uint8_t* endp;
                int32_t val = jsoncons::detail::from_big_endian<int32_t>(buf,buf+sizeof(buf),&endp);
                handler_.int64_value(val, semantic_tag::none, *this);
                break;
            }
            case jsoncons::ubjson::detail::ubjson_format::int64_type: 
            {
                uint8_t buf[sizeof(int64_t)];
                source_.read(buf, sizeof(int64_t));
                if (source_.eof())
                {
                    ec = ubjson_errc::unexpected_eof;
                    return;
                }
                const uint8_t* endp;
                int64_t val = jsoncons::detail::from_big_endian<int64_t>(buf,buf+sizeof(buf),&endp);
                handler_.int64_value(val, semantic_tag::none, *this);
                break;
            }
            case jsoncons::ubjson::detail::ubjson_format::float32_type: 
            {
                uint8_t buf[sizeof(float)];
                source_.read(buf, sizeof(float));
                if (source_.eof())
                {
                    ec = ubjson_errc::unexpected_eof;
                    return;
                }
                const uint8_t* endp;
                float val = jsoncons::detail::from_big_endian<float>(buf,buf+sizeof(buf),&endp);
                handler_.double_value(val, semantic_tag::none, *this);
                break;
            }
            case jsoncons::ubjson::detail::ubjson_format::float64_type: 
            {
                uint8_t buf[sizeof(double)];
                source_.read(buf, sizeof(double));
                if (source_.eof())
                {
                    ec = ubjson_errc::unexpected_eof;
                    return;
                }
                const uint8_t* endp;
                double val = jsoncons::detail::from_big_endian<double>(buf,buf+sizeof(buf),&endp);
                handler_.double_value(val, semantic_tag::none, *this);
                break;
            }
            case jsoncons::ubjson::detail::ubjson_format::char_type: 
            {
                uint8_t buf[sizeof(char)];
                source_.read(buf, sizeof(char));
                if (source_.eof())
                {
                    ec = ubjson_errc::unexpected_eof;
                    return;
                }
                const uint8_t* endp;
                char c = jsoncons::detail::from_big_endian<char>(buf,buf+sizeof(buf),&endp);
                auto result = unicons::validate(&c,&c+1);
                if (result.ec != unicons::conv_errc())
                {
                    ec = ubjson_errc::invalid_utf8_text_string;
                    return;
                }
                handler_.string_value(basic_string_view<char>(&c,1), semantic_tag::none, *this);
                break;
            }
            case jsoncons::ubjson::detail::ubjson_format::string_type: 
            {
                size_t length = get_length(ec);
                if (ec)
                {
                    return;
                }
                std::string s;
                source_.read(std::back_inserter(s), length);
                if (source_.eof())
                {
                    ec = ubjson_errc::unexpected_eof;
                    return;
                }
                auto result = unicons::validate(s.begin(),s.end());
                if (result.ec != unicons::conv_errc())
                {
                    ec = ubjson_errc::invalid_utf8_text_string;
                    return;
                }
                handler_.string_value(basic_string_view<char>(s.data(),s.length()), semantic_tag::none, *this);
                break;
            }
            case jsoncons::ubjson::detail::ubjson_format::high_precision_number_type: 
            {
                size_t length = get_length(ec);
                if (ec)
                {
                    return;
                }
                std::string s;
                source_.read(std::back_inserter(s), length);
                if (source_.eof())
                {
                    ec = ubjson_errc::unexpected_eof;
                    return;
                }
                if (jsoncons::detail::is_integer(s.data(),s.length()))
                {
                    handler_.string_value(basic_string_view<char>(s.data(),s.length()), semantic_tag::bigint, *this);
                }
                else
                {
                    handler_.string_value(basic_string_view<char>(s.data(),s.length()), semantic_tag::bigdec, *this);
                }
                break;
            }
            case jsoncons::ubjson::detail::ubjson_format::start_array_marker: 
            {
                if (source_.peek() == jsoncons::ubjson::detail::ubjson_format::type_marker)
                {
                    source_.ignore(1);
                    uint8_t item_type{};
                    if (source_.get(item_type) == 0)
                    {
                        ec = ubjson_errc::unexpected_eof;
                        return;
                    }
                    if (source_.peek() == jsoncons::ubjson::detail::ubjson_format::count_marker)
                    {
                        source_.ignore(1);
                        size_t length = get_length(ec);
                        handler_.begin_array(length, semantic_tag::none, *this);
                        for (size_t i = 0; i < length; ++i)
                        {
                            read_value(item_type, ec);
                            if (ec)
                            {
                                return;
                            }
                        }
                        handler_.end_array(*this);
                    }
                    else
                    {
                        ec = ubjson_errc::count_required_after_type;
                        return;
                    }
                }
                else if (source_.peek() == jsoncons::ubjson::detail::ubjson_format::count_marker)
                {
                    source_.ignore(1);
                    size_t length = get_length(ec);
                    handler_.begin_array(length, semantic_tag::none, *this);
                    for (size_t i = 0; i < length; ++i)
                    {
                        read(ec);
                        if (ec)
                        {
                            return;
                        }
                    }
                    handler_.end_array(*this);
                }
                else
                {
                    handler_.begin_array(semantic_tag::none, *this);
                    while (source_.peek() != jsoncons::ubjson::detail::ubjson_format::end_array_marker)
                    {
                        read(ec);
                        if (ec)
                        {
                            return;
                        }
                    }
                    handler_.end_array(*this);
                    source_.ignore(1);
                }
                break;
            }
            case jsoncons::ubjson::detail::ubjson_format::start_object_marker: 
            {
                if (source_.peek() == jsoncons::ubjson::detail::ubjson_format::type_marker)
                {
                    source_.ignore(1);
                    uint8_t item_type{};
                    if (source_.get(item_type) == 0)
                    {
                        ec = ubjson_errc::unexpected_eof;
                        return;
                    }
                    if (source_.peek() == jsoncons::ubjson::detail::ubjson_format::count_marker)
                    {
                        source_.ignore(1);
                        size_t length = get_length(ec);
                        handler_.begin_object(length, semantic_tag::none, *this);
                        for (size_t i = 0; i < length; ++i)
                        {
                            read_name(ec);
                            if (ec)
                            {
                                return;
                            }
                            read_value(item_type, ec);
                            if (ec)
                            {
                                return;
                            }
                        }
                        handler_.end_object(*this);
                    }
                    else
                    {
                        ec = ubjson_errc::count_required_after_type;
                        return;
                    }
                }
                else
                {
                    if (source_.peek() == jsoncons::ubjson::detail::ubjson_format::count_marker)
                    {
                        source_.ignore(1);
                        size_t length = get_length(ec);
                        handler_.begin_object(length, semantic_tag::none, *this);
                        for (size_t i = 0; i < length; ++i)
                        {
                            read_name(ec);
                            if (ec)
                            {
                                return;
                            }
                            read(ec);
                            if (ec)
                            {
                                return;
                            }
                        }
                        handler_.end_object(*this);
                    }
                    else
                    {
                        handler_.begin_object(semantic_tag::none, *this);
                        while (source_.peek() != jsoncons::ubjson::detail::ubjson_format::end_object_marker)
                        {
                            read_name(ec);
                            if (ec)
                            {
                                return;
                            }
                            read(ec);
                            if (ec)
                            {
                                return;
                            }
                        }
                        handler_.end_object(*this);
                        source_.ignore(1);
                    }
                }
                break;
            }
            default:
            {
                ec = ubjson_errc::unknown_type;
                return;
            }
        }
    }

    size_t get_length(std::error_code& ec)
    {
        size_t length = 0;
        if (JSONCONS_UNLIKELY(source_.eof()))
        {
            ec = ubjson_errc::unexpected_eof;
            return length;
        }
        uint8_t type{};
        if (source_.get(type) == 0)
        {
            ec = ubjson_errc::unexpected_eof;
            return length;
        }
        switch (type)
        {
            case jsoncons::ubjson::detail::ubjson_format::int8_type: 
            {
                uint8_t buf[sizeof(int8_t)];
                source_.read(buf, sizeof(int8_t));
                if (source_.eof())
                {
                    ec = ubjson_errc::unexpected_eof;
                    return length;
                }
                const uint8_t* endp;
                int8_t val = jsoncons::detail::from_big_endian<int8_t>(buf,buf+sizeof(buf),&endp);
                if (val >= 0)
                {
                    length = val;
                }
                else
                {
                    ec = ubjson_errc::length_cannot_be_negative;
                    return length;
                }
                break;
            }
            case jsoncons::ubjson::detail::ubjson_format::uint8_type: 
            {
                uint8_t val{};
                if (source_.get(val) == 0)
                {
                    ec = ubjson_errc::unexpected_eof;
                    return length;
                }
                length = val;
                break;
            }
            case jsoncons::ubjson::detail::ubjson_format::int16_type: 
            {
                uint8_t buf[sizeof(int16_t)];
                source_.read(buf, sizeof(int16_t));
                if (source_.eof())
                {
                    ec = ubjson_errc::unexpected_eof;
                    return length;
                }
                const uint8_t* endp;
                int16_t val = jsoncons::detail::from_big_endian<int16_t>(buf,buf+sizeof(buf),&endp);
                if (val >= 0)
                {
                    length = val;
                }
                else
                {
                    ec = ubjson_errc::length_cannot_be_negative;
                    return length;
                }
                break;
            }
            case jsoncons::ubjson::detail::ubjson_format::int32_type: 
            {
                uint8_t buf[sizeof(int32_t)];
                source_.read(buf, sizeof(int32_t));
                if (source_.eof())
                {
                    ec = ubjson_errc::unexpected_eof;
                    return length;
                }
                const uint8_t* endp;
                int32_t val = jsoncons::detail::from_big_endian<int32_t>(buf,buf+sizeof(buf),&endp);
                if (val >= 0)
                {
                    length = val;
                }
                else
                {
                    ec = ubjson_errc::length_cannot_be_negative;
                    return length;
                }
                break;
            }
            case jsoncons::ubjson::detail::ubjson_format::int64_type: 
            {
                uint8_t buf[sizeof(int64_t)];
                source_.read(buf, sizeof(int64_t));
                if (source_.eof())
                {
                    ec = ubjson_errc::unexpected_eof;
                    return length;
                }
                const uint8_t* endp;
                int64_t val = jsoncons::detail::from_big_endian<int64_t>(buf,buf+sizeof(buf),&endp);
                if (val >= 0)
                {
                    length = (size_t)val;
                    if (length != (uint64_t)val)
                    {
                        ec = ubjson_errc::number_too_large;
                        return length;
                    }
                }
                else
                {
                    ec = ubjson_errc::length_cannot_be_negative;
                    return length;
                }
                break;
            }
            default:
            {
                ec = ubjson_errc::length_must_be_integer;
                return length;
            }
        }
        return length;
    }

    void read_name(std::error_code& ec)
    {
        size_t length = get_length(ec);
        if (ec)
        {
            return;
        }
        std::string s;
        source_.read(std::back_inserter(s), length);
        if (source_.eof())
        {
            ec = ubjson_errc::unexpected_eof;
            return;
        }
        auto result = unicons::validate(s.begin(),s.end());
        if (result.ec != unicons::conv_errc())
        {
            ec = ubjson_errc::invalid_utf8_text_string;
            return;
        }
        handler_.name(basic_string_view<char>(s.data(),s.length()), *this);
    }
};

typedef basic_ubjson_reader<jsoncons::binary_stream_source> ubjson_reader;

typedef basic_ubjson_reader<jsoncons::bytes_source> ubjson_bytes_reader;

#if !defined(JSONCONS_NO_DEPRECATED)
typedef ubjson_bytes_reader ubjson_buffer_reader;
#endif

}}

#endif
