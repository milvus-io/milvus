// Copyright 2017 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_BSON_BSON_READER_HPP
#define JSONCONS_BSON_BSON_READER_HPP

#include <string>
#include <vector>
#include <memory>
#include <utility> // std::move
#include <jsoncons/json.hpp>
#include <jsoncons/source.hpp>
#include <jsoncons/json_content_handler.hpp>
#include <jsoncons/config/binary_detail.hpp>
#include <jsoncons_ext/bson/bson_detail.hpp>
#include <jsoncons_ext/bson/bson_error.hpp>

namespace jsoncons { namespace bson {

template <class Src>
class basic_bson_reader : public ser_context
{
    Src source_;
    json_content_handler& handler_;
    size_t nesting_depth_;
public:
    template <class Source>
    basic_bson_reader(Source&& source, json_content_handler& handler)
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
            if (source_.is_error())
            {
                ec = bson_errc::source_error;
                return;
            }   
            uint8_t buf[sizeof(int32_t)]; 
            if (source_.read(buf, sizeof(int32_t)) != sizeof(int32_t))
            {
                ec = bson_errc::unexpected_eof;
                return;
            }
            const uint8_t* endp;
            /* auto len = */jsoncons::detail::from_little_endian<int32_t>(buf, buf+sizeof(int32_t),&endp);

            handler_.begin_object(semantic_tag::none, *this);
            ++nesting_depth_;
            read_e_list(jsoncons::bson::detail::bson_container_type::document, ec);
            handler_.end_object(*this);
            --nesting_depth_;
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

    void read_e_list(jsoncons::bson::detail::bson_container_type type, std::error_code& ec)
    {
        uint8_t t{};
        while (source_.get(t) > 0 && t != 0x00)
        {
            std::basic_string<char> s;
            uint8_t c{};
            while (source_.get(c) > 0 && c != 0)
            {
                s.push_back(c);
            }

            if (type == jsoncons::bson::detail::bson_container_type::document)
            {
                auto result = unicons::validate(s.begin(),s.end());
                if (result.ec != unicons::conv_errc())
                {
                    ec = bson_errc::invalid_utf8_text_string;
                    return;
                }
                handler_.name(basic_string_view<char>(s.data(),s.length()), *this);
            }
            read_internal(t, ec);
        }
    }

    void read_internal(uint8_t type, std::error_code& ec)
    {
        switch (type)
        {
            case jsoncons::bson::detail::bson_format::double_cd:
            {
                uint8_t buf[sizeof(double)]; 
                if (source_.read(buf, sizeof(double)) != sizeof(double))
                {
                    ec = bson_errc::unexpected_eof;
                    return;
                }
                const uint8_t* endp;
                double res = jsoncons::detail::from_little_endian<double>(buf,buf+sizeof(buf),&endp);
                handler_.double_value(res, semantic_tag::none, *this);
                break;
            }
            case jsoncons::bson::detail::bson_format::string_cd:
            {
                uint8_t buf[sizeof(int32_t)]; 
                if (source_.read(buf, sizeof(int32_t)) != sizeof(int32_t))
                {
                    ec = bson_errc::unexpected_eof;
                    return;
                }
                const uint8_t* endp;
                auto len = jsoncons::detail::from_little_endian<int32_t>(buf, buf+sizeof(buf),&endp);

                std::basic_string<char> s;
                s.reserve(len - 1);
                if ((int32_t)source_.read(std::back_inserter(s), len-1) != len-1)
                {
                    ec = bson_errc::unexpected_eof;
                    return;
                }
                uint8_t c{};
                source_.get(c); // discard 0
                auto result = unicons::validate(s.begin(),s.end());
                if (result.ec != unicons::conv_errc())
                {
                    ec = bson_errc::invalid_utf8_text_string;
                    return;
                }
                handler_.string_value(basic_string_view<char>(s.data(),s.length()), semantic_tag::none, *this);
                break;
            }
            case jsoncons::bson::detail::bson_format::document_cd: 
            {
                read(ec);
                if (ec)
                {
                    return;
                }
                break;
            }

            case jsoncons::bson::detail::bson_format::array_cd: 
            {
                uint8_t buf[sizeof(int32_t)]; 
                if (source_.read(buf, sizeof(int32_t)) != sizeof(int32_t))
                {
                    ec = bson_errc::unexpected_eof;
                    return;
                }
                const uint8_t* endp;
                /* auto len = */ jsoncons::detail::from_little_endian<int32_t>(buf, buf+sizeof(int32_t),&endp);

                handler_.begin_array(semantic_tag::none, *this);
                ++nesting_depth_;
                read_e_list(jsoncons::bson::detail::bson_container_type::document, ec);
                handler_.end_array(*this);
                --nesting_depth_;
                break;
            }
            case jsoncons::bson::detail::bson_format::null_cd: 
            {
                handler_.null_value(semantic_tag::none, *this);
                break;
            }
            case jsoncons::bson::detail::bson_format::bool_cd:
            {
                uint8_t val{};
                if (source_.get(val) == 0)
                {
                    ec = bson_errc::unexpected_eof;
                    return;
                }
                handler_.bool_value(val != 0, semantic_tag::none, *this);
                break;
            }
            case jsoncons::bson::detail::bson_format::int32_cd: 
            {
                uint8_t buf[sizeof(int32_t)]; 
                if (source_.read(buf, sizeof(int32_t)) != sizeof(int32_t))
                {
                    ec = bson_errc::unexpected_eof;
                    return;
                }
                const uint8_t* endp;
                auto val = jsoncons::detail::from_little_endian<int32_t>(buf, buf+sizeof(int32_t),&endp);
                handler_.int64_value(val, semantic_tag::none, *this);
                break;
            }

            case jsoncons::bson::detail::bson_format::timestamp_cd: 
            {
                uint8_t buf[sizeof(uint64_t)]; 
                if (source_.read(buf, sizeof(uint64_t)) != sizeof(uint64_t))
                {
                    ec = bson_errc::unexpected_eof;
                    return;
                }
                const uint8_t* endp;
                auto val = jsoncons::detail::from_little_endian<uint64_t>(buf, buf+sizeof(uint64_t),&endp);
                handler_.uint64_value(val, semantic_tag::timestamp, *this);
                break;
            }

            case jsoncons::bson::detail::bson_format::int64_cd: 
            {
                uint8_t buf[sizeof(int64_t)]; 
                if (source_.read(buf, sizeof(int64_t)) != sizeof(int64_t))
                {
                    ec = bson_errc::unexpected_eof;
                    return;
                }
                const uint8_t* endp;
                auto val = jsoncons::detail::from_little_endian<int64_t>(buf, buf+sizeof(int64_t),&endp);
                handler_.int64_value(val, semantic_tag::none, *this);
                break;
            }

            case jsoncons::bson::detail::bson_format::datetime_cd: 
            {
                uint8_t buf[sizeof(int64_t)]; 
                if (source_.read(buf, sizeof(int64_t)) != sizeof(int64_t))
                {
                    ec = bson_errc::unexpected_eof;
                    return;
                }
                const uint8_t* endp;
                auto val = jsoncons::detail::from_little_endian<int64_t>(buf, buf+sizeof(int64_t),&endp);
                handler_.int64_value(val, semantic_tag::timestamp, *this);
                break;
            }
            case jsoncons::bson::detail::bson_format::binary_cd: 
            {
                uint8_t buf[sizeof(int32_t)]; 
                if (source_.read(buf, sizeof(int32_t)) != sizeof(int32_t))
                {
                    ec = bson_errc::unexpected_eof;
                    return;
                }
                const uint8_t* endp;
                const auto len = jsoncons::detail::from_little_endian<int32_t>(buf, buf+sizeof(int32_t),&endp);

                std::vector<uint8_t> v(len, 0);
                if (source_.read(v.data(), v.size()) != v.size())
                {
                    ec = bson_errc::unexpected_eof;
                    return;
                }

                handler_.byte_string_value(byte_string_view(v.data(),v.size()), 
                                           semantic_tag::none, 
                                           *this);
                break;
            }
        }

    }
};

typedef basic_bson_reader<jsoncons::binary_stream_source> bson_reader;
typedef basic_bson_reader<jsoncons::bytes_source> bson_bytes_reader;

#if !defined(JSONCONS_NO_DEPRECATED)
typedef bson_bytes_reader bson_buffer_reader;
#endif

}}

#endif
