// Copyright 2017 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_MSGPACK_MSGPACK_READER_HPP
#define JSONCONS_MSGPACK_MSGPACK_READER_HPP

#include <string>
#include <vector>
#include <memory>
#include <utility> // std::move
#include <jsoncons/json.hpp>
#include <jsoncons/source.hpp>
#include <jsoncons/json_content_handler.hpp>
#include <jsoncons/config/binary_detail.hpp>
#include <jsoncons_ext/msgpack/msgpack_detail.hpp>
#include <jsoncons_ext/msgpack/msgpack_error.hpp>

namespace jsoncons { namespace msgpack {

template <class Src>
class basic_msgpack_reader : public ser_context
{
    Src source_;
    json_content_handler& handler_;
    size_t nesting_depth_;
    std::string buffer_;
public:
    template <class Source>
    basic_msgpack_reader(Source&& source, json_content_handler& handler)
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
            ec = msgpack_errc::source_error;
            return;
        }   
        //const uint8_t* pos = input_ptr_++;

        uint8_t type{};
        source_.get(type);

        if (type <= 0xbf)
        {
            if (type <= 0x7f) 
            {
                // positive fixint
                handler_.uint64_value(type, semantic_tag::none, *this);
            }
            else if (type <= 0x8f) 
            {
                // fixmap
                const size_t len = type & 0x0f;
                handler_.begin_object(len, semantic_tag::none, *this);
                ++nesting_depth_;
                for (size_t i = 0; i < len; ++i)
                {
                    parse_name(ec);
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
                --nesting_depth_;
            }
            else if (type <= 0x9f) 
            {
                // fixarray
                const size_t len = type & 0x0f;
                handler_.begin_array(len, semantic_tag::none, *this);
                ++nesting_depth_;
                for (size_t i = 0; i < len; ++i)
                {
                    read(ec);
                    if (ec)
                    {
                        return;
                    }
                }
                handler_.end_array(*this);
                --nesting_depth_;
            }
            else 
            {
                // fixstr
                const size_t len = type & 0x1f;

                std::basic_string<char> s;
                source_.read(std::back_inserter(s), len);
                if (source_.eof())
                {
                    ec = msgpack_errc::unexpected_eof;
                    return;
                }

                auto result = unicons::validate(s.begin(),s.end());
                if (result.ec != unicons::conv_errc())
                {
                    ec = msgpack_errc::invalid_utf8_text_string;
                    return;
                }
                handler_.string_value(basic_string_view<char>(s.data(),s.length()), semantic_tag::none, *this);
            }
        }
        else if (type >= 0xe0) 
        {
            // negative fixint
            handler_.int64_value(static_cast<int8_t>(type), semantic_tag::none, *this);
        }
        else
        {
            switch (type)
            {
                case jsoncons::msgpack::detail::msgpack_format ::nil_cd: 
                {
                    handler_.null_value(semantic_tag::none, *this);
                    break;
                }
                case jsoncons::msgpack::detail::msgpack_format ::true_cd:
                {
                    handler_.bool_value(true, semantic_tag::none, *this);
                    break;
                }
                case jsoncons::msgpack::detail::msgpack_format ::false_cd:
                {
                    handler_.bool_value(false, semantic_tag::none, *this);
                    break;
                }
                case jsoncons::msgpack::detail::msgpack_format ::float32_cd: 
                {
                    uint8_t buf[sizeof(float)];
                    source_.read(buf, sizeof(float));
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }
                    const uint8_t* endp;
                    float val = jsoncons::detail::from_big_endian<float>(buf,buf+sizeof(buf),&endp);
                    handler_.double_value(val, semantic_tag::none, *this);
                    break;
                }

                case jsoncons::msgpack::detail::msgpack_format ::float64_cd: 
                {
                    uint8_t buf[sizeof(double)];
                    source_.read(buf, sizeof(double));
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }
                    const uint8_t* endp;
                    double val = jsoncons::detail::from_big_endian<double>(buf,buf+sizeof(buf),&endp);
                    handler_.double_value(val, semantic_tag::none, *this);
                    break;
                }

                case jsoncons::msgpack::detail::msgpack_format ::uint8_cd: 
                {
                    uint8_t val{};
                    source_.get(val);
                    handler_.uint64_value(val, semantic_tag::none, *this);
                    break;
                }

                case jsoncons::msgpack::detail::msgpack_format ::uint16_cd: 
                {
                    uint8_t buf[sizeof(uint16_t)];
                    source_.read(buf, sizeof(uint16_t));
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }
                    const uint8_t* endp;
                    uint16_t val = jsoncons::detail::from_big_endian<uint16_t>(buf,buf+sizeof(buf),&endp);
                    handler_.uint64_value(val, semantic_tag::none, *this);
                    break;
                }

                case jsoncons::msgpack::detail::msgpack_format ::uint32_cd: 
                {
                    uint8_t buf[sizeof(uint32_t)];
                    source_.read(buf, sizeof(uint32_t));
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }
                    const uint8_t* endp;
                    uint32_t val = jsoncons::detail::from_big_endian<uint32_t>(buf,buf+sizeof(buf),&endp);
                    handler_.uint64_value(val, semantic_tag::none, *this);
                    break;
                }

                case jsoncons::msgpack::detail::msgpack_format ::uint64_cd: 
                {
                    uint8_t buf[sizeof(uint64_t)];
                    source_.read(buf, sizeof(uint64_t));
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }
                    const uint8_t* endp;
                    uint64_t val = jsoncons::detail::from_big_endian<uint64_t>(buf,buf+sizeof(buf),&endp);
                    handler_.uint64_value(val, semantic_tag::none, *this);
                    break;
                }

                case jsoncons::msgpack::detail::msgpack_format ::int8_cd: 
                {
                    uint8_t buf[sizeof(int8_t)];
                    source_.read(buf, sizeof(int8_t));
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }
                    const uint8_t* endp;
                    int8_t val = jsoncons::detail::from_big_endian<int8_t>(buf,buf+sizeof(buf),&endp);
                    handler_.int64_value(val, semantic_tag::none, *this);
                    break;
                }

                case jsoncons::msgpack::detail::msgpack_format ::int16_cd: 
                {
                    uint8_t buf[sizeof(int16_t)];
                    source_.read(buf, sizeof(int16_t));
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }
                    const uint8_t* endp;
                    int16_t val = jsoncons::detail::from_big_endian<int16_t>(buf,buf+sizeof(buf),&endp);
                    handler_.int64_value(val, semantic_tag::none, *this);
                    break;
                }

                case jsoncons::msgpack::detail::msgpack_format ::int32_cd: 
                {
                    uint8_t buf[sizeof(int32_t)];
                    source_.read(buf, sizeof(int32_t));
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }
                    const uint8_t* endp;
                    int32_t val = jsoncons::detail::from_big_endian<int32_t>(buf,buf+sizeof(buf),&endp);
                    handler_.int64_value(val, semantic_tag::none, *this);
                    break;
                }

                case jsoncons::msgpack::detail::msgpack_format ::int64_cd: 
                {
                    uint8_t buf[sizeof(int64_t)];
                    source_.read(buf, sizeof(int64_t));
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }
                    const uint8_t* endp;
                    int64_t val = jsoncons::detail::from_big_endian<int64_t>(buf,buf+sizeof(buf),&endp);
                    handler_.int64_value(val, semantic_tag::none, *this);
                    break;
                }

                case jsoncons::msgpack::detail::msgpack_format ::str8_cd: 
                {
                    uint8_t buf[sizeof(int8_t)];
                    source_.read(buf, sizeof(int8_t));
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }
                    const uint8_t* endp;
                    int8_t len = jsoncons::detail::from_big_endian<int8_t>(buf,buf+sizeof(buf),&endp);

                    std::basic_string<char> s;
                    source_.read(std::back_inserter(s), len);
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }
                    auto result = unicons::validate(s.begin(),s.end());
                    if (result.ec != unicons::conv_errc())
                    {
                        ec = msgpack_errc::invalid_utf8_text_string;
                        return;
                    }
                    handler_.string_value(basic_string_view<char>(s.data(),s.length()), semantic_tag::none, *this);
                    break;
                }

                case jsoncons::msgpack::detail::msgpack_format ::str16_cd: 
                {
                    uint8_t buf[sizeof(int16_t)];
                    source_.read(buf, sizeof(int16_t));
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }
                    const uint8_t* endp;
                    int16_t len = jsoncons::detail::from_big_endian<int16_t>(buf,buf+sizeof(buf),&endp);

                    std::basic_string<char> s;
                    source_.read(std::back_inserter(s), len);
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }

                    auto result = unicons::validate(s.begin(),s.end());
                    if (result.ec != unicons::conv_errc())
                    {
                        ec = msgpack_errc::invalid_utf8_text_string;
                        return;
                    }
                    handler_.string_value(basic_string_view<char>(s.data(),s.length()), semantic_tag::none, *this);
                    break;
                }

                case jsoncons::msgpack::detail::msgpack_format ::str32_cd: 
                {
                    uint8_t buf[sizeof(int32_t)];
                    source_.read(buf, sizeof(int32_t));
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }
                    const uint8_t* endp;
                    int32_t len = jsoncons::detail::from_big_endian<int32_t>(buf,buf+sizeof(buf),&endp);

                    std::basic_string<char> s;
                    source_.read(std::back_inserter(s), len);
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }

                    auto result = unicons::validate(s.begin(),s.end());
                    if (result.ec != unicons::conv_errc())
                    {
                        ec = msgpack_errc::invalid_utf8_text_string;
                        return;
                    }
                    handler_.string_value(basic_string_view<char>(s.data(),s.length()), semantic_tag::none, *this);
                    break;
                }

                case jsoncons::msgpack::detail::msgpack_format ::bin8_cd: 
                {
                    uint8_t buf[sizeof(int8_t)];
                    source_.read(buf, sizeof(int8_t));
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }
                    const uint8_t* endp;
                    int8_t len = jsoncons::detail::from_big_endian<int8_t>(buf,buf+sizeof(buf),&endp);

                    std::vector<uint8_t> v;
                    v.reserve(len);
                    source_.read(std::back_inserter(v), len);
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }

                    handler_.byte_string_value(byte_string_view(v.data(),v.size()), 
                                               semantic_tag::none, 
                                               *this);
                    break;
                }

                case jsoncons::msgpack::detail::msgpack_format ::bin16_cd: 
                {
                    uint8_t buf[sizeof(int16_t)];
                    source_.read(buf, sizeof(int16_t));
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }
                    const uint8_t* endp;
                    int16_t len = jsoncons::detail::from_big_endian<int16_t>(buf,buf+sizeof(buf),&endp);

                    std::vector<uint8_t> v;
                    v.reserve(len);
                    source_.read(std::back_inserter(v), len);
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }

                    handler_.byte_string_value(byte_string_view(v.data(),v.size()), 
                                               semantic_tag::none, 
                                               *this);
                    break;
                }

                case jsoncons::msgpack::detail::msgpack_format ::bin32_cd: 
                {
                    uint8_t buf[sizeof(int32_t)];
                    source_.read(buf, sizeof(int32_t));
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }
                    const uint8_t* endp;
                    int32_t len = jsoncons::detail::from_big_endian<int32_t>(buf,buf+sizeof(buf),&endp);

                    std::vector<uint8_t> v;
                    v.reserve(len);
                    source_.read(std::back_inserter(v), len);
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }

                    handler_.byte_string_value(byte_string_view(v.data(),v.size()), 
                                               semantic_tag::none, 
                                               *this);
                    break;
                }

                case jsoncons::msgpack::detail::msgpack_format ::array16_cd: 
                {
                    uint8_t buf[sizeof(int16_t)];
                    source_.read(buf, sizeof(int16_t));
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }
                    const uint8_t* endp;
                    int16_t len = jsoncons::detail::from_big_endian<int16_t>(buf,buf+sizeof(buf),&endp);

                    handler_.begin_array(len, semantic_tag::none, *this);
                    ++nesting_depth_;
                    for (int16_t i = 0; i < len; ++i)
                    {
                        read(ec);
                        if (ec)
                        {
                            return;
                        }
                    }
                    handler_.end_array(*this);
                    --nesting_depth_;
                    break;
                }

                case jsoncons::msgpack::detail::msgpack_format ::array32_cd: 
                {
                    uint8_t buf[sizeof(int32_t)];
                    source_.read(buf, sizeof(int32_t));
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }
                    const uint8_t* endp;
                    int32_t len = jsoncons::detail::from_big_endian<int32_t>(buf,buf+sizeof(buf),&endp);

                    handler_.begin_array(len, semantic_tag::none, *this);
                    ++nesting_depth_;
                    for (int32_t i = 0; i < len; ++i)
                    {
                        read(ec);
                        if (ec)
                        {
                            return;
                        }
                    }
                    handler_.end_array(*this);
                    --nesting_depth_;
                    break;
                }

                case jsoncons::msgpack::detail::msgpack_format ::map16_cd : 
                {
                    uint8_t buf[sizeof(int16_t)];
                    source_.read(buf, sizeof(int16_t));
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }
                    const uint8_t* endp;
                    int16_t len = jsoncons::detail::from_big_endian<int16_t>(buf,buf+sizeof(buf),&endp);

                    handler_.begin_object(len, semantic_tag::none, *this);
                    ++nesting_depth_;
                    for (int16_t i = 0; i < len; ++i)
                    {
                        parse_name(ec);
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
                    --nesting_depth_;
                    break;
                }

                case jsoncons::msgpack::detail::msgpack_format ::map32_cd : 
                {
                    uint8_t buf[sizeof(int32_t)];
                    source_.read(buf, sizeof(int32_t));
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }
                    const uint8_t* endp;
                    int32_t len = jsoncons::detail::from_big_endian<int32_t>(buf,buf+sizeof(buf),&endp);

                    handler_.begin_object(len, semantic_tag::none, *this);
                    ++nesting_depth_;
                    for (int32_t i = 0; i < len; ++i)
                    {
                        parse_name(ec);
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
                    --nesting_depth_;
                    break;
                }

                default:
                {
                    //error
                }
            }
        }
    }

    void parse_name(std::error_code& ec)
    {
        uint8_t type{};
        source_.get(type);
        if (source_.eof())
        {
            ec = msgpack_errc::unexpected_eof;
            return;
        }

        //const uint8_t* pos = input_ptr_++;
        if (type >= 0xa0 && type <= 0xbf)
        {
                // fixstr
            const size_t len = type & 0x1f;

            std::basic_string<char> s;
            source_.read(std::back_inserter(s), len);
            if (source_.eof())
            {
                ec = msgpack_errc::unexpected_eof;
                return;
            }
            auto result = unicons::validate(s.begin(),s.end());
            if (result.ec != unicons::conv_errc())
            {
                ec = msgpack_errc::invalid_utf8_text_string;
                return;
            }
            handler_.name(basic_string_view<char>(s.data(),s.length()), *this);
        }
        else
        {
            switch (type)
            {
                case jsoncons::msgpack::detail::msgpack_format ::str8_cd: 
                {
                    uint8_t buf[sizeof(int8_t)];
                    source_.read(buf, sizeof(int8_t));
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }
                    const uint8_t* endp;
                    int8_t len = jsoncons::detail::from_big_endian<int8_t>(buf,buf+sizeof(buf),&endp);

                    std::basic_string<char> s;
                    source_.read(std::back_inserter(s), len);
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }

                    auto result = unicons::validate(s.begin(),s.end());
                    if (result.ec != unicons::conv_errc())
                    {
                        ec = msgpack_errc::invalid_utf8_text_string;
                        return;
                    }
                    handler_.name(basic_string_view<char>(s.data(),s.length()), *this);
                    break;
                }

                case jsoncons::msgpack::detail::msgpack_format ::str16_cd: 
                {
                    uint8_t buf[sizeof(int16_t)];
                    source_.read(buf, sizeof(int16_t));
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }
                    const uint8_t* endp;
                    int16_t len = jsoncons::detail::from_big_endian<int16_t>(buf,buf+sizeof(buf),&endp);

                    std::basic_string<char> s;
                    source_.read(std::back_inserter(s), len);
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }

                    //std::basic_string<char> s;
                    //auto result = unicons::convert(
                    //    first, last,std::back_inserter(s),unicons::conv_flags::strict);
                    //if (result.ec != unicons::conv_errc())
                    //{
                    //    JSONCONS_THROW(json_runtime_error<std::runtime_error>("Illegal unicode"));
                    //}
                    handler_.name(basic_string_view<char>(s.data(),s.length()), *this);
                    break;
                }

                case jsoncons::msgpack::detail::msgpack_format ::str32_cd: 
                {
                    uint8_t buf[sizeof(int32_t)];
                    source_.read(buf, sizeof(int32_t));
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }
                    const uint8_t* endp;
                    int32_t len = jsoncons::detail::from_big_endian<int32_t>(buf,buf+sizeof(buf),&endp);

                    std::basic_string<char> s;
                    source_.read(std::back_inserter(s), len);
                    if (source_.eof())
                    {
                        ec = msgpack_errc::unexpected_eof;
                        return;
                    }

                    //std::basic_string<char> s;
                    //auto result = unicons::convert(
                    //    first, last,std::back_inserter(s),unicons::conv_flags::strict);
                    //if (result.ec != unicons::conv_errc())
                    //{
                    //    JSONCONS_THROW(json_runtime_error<std::runtime_error>("Illegal unicode"));
                    //}
                    handler_.name(basic_string_view<char>(s.data(),s.length()), *this);
                    break;
                }
            }

        }
    }
};

typedef basic_msgpack_reader<jsoncons::binary_stream_source> msgpack_reader;

typedef basic_msgpack_reader<jsoncons::bytes_source> msgpack_bytes_reader;

#if !defined(JSONCONS_NO_DEPRECATED)
typedef msgpack_bytes_reader msgpack_buffer_reader;
#endif

}}

#endif
