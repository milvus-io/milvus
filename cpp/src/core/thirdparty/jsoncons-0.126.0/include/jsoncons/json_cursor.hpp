// Copyright 2018 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_JSON_CURSOR_HPP
#define JSONCONS_JSON_CURSOR_HPP

#include <memory> // std::allocator
#include <string>
#include <vector>
#include <stdexcept>
#include <system_error>
#include <ios>
#include <istream> // std::basic_istream
#include <jsoncons/byte_string.hpp>
#include <jsoncons/config/jsoncons_config.hpp>
#include <jsoncons/json_content_handler.hpp>
#include <jsoncons/json_exception.hpp>
#include <jsoncons/json_parser.hpp>
#include <jsoncons/parse_error_handler.hpp>
#include <jsoncons/staj_reader.hpp>
#include <jsoncons/source.hpp>

namespace jsoncons {

template <class CharT>
class basic_staj_event_handler final : public basic_json_content_handler<CharT>
{
public:
    using typename basic_json_content_handler<CharT>::string_view_type;
private:
    basic_staj_event<CharT> event_;
public:
    basic_staj_event_handler()
        : event_(staj_event_type::null_value)
    {
    }

    basic_staj_event_handler(staj_event_type event_type)
        : event_(event_type)
    {
    }

    const basic_staj_event<CharT>& event() const
    {
        return event_;
    }
private:

    bool do_begin_object(semantic_tag, const ser_context&) override
    {
        event_ = basic_staj_event<CharT>(staj_event_type::begin_object);
        return false;
    }

    bool do_end_object(const ser_context&) override
    {
        event_ = basic_staj_event<CharT>(staj_event_type::end_object);
        return false;
    }

    bool do_begin_array(semantic_tag, const ser_context&) override
    {
        event_ = basic_staj_event<CharT>(staj_event_type::begin_array);
        return false;
    }

    bool do_end_array(const ser_context&) override
    {
        event_ = basic_staj_event<CharT>(staj_event_type::end_array);
        return false;
    }

    bool do_name(const string_view_type& name, const ser_context&) override
    {
        event_ = basic_staj_event<CharT>(name.data(), name.length(), staj_event_type::name);
        return false;
    }

    bool do_null_value(semantic_tag, const ser_context&) override
    {
        event_ = basic_staj_event<CharT>(staj_event_type::null_value);
        return false;
    }

    bool do_bool_value(bool value, semantic_tag, const ser_context&) override
    {
        event_ = basic_staj_event<CharT>(value);
        return false;
    }

    bool do_string_value(const string_view_type& s, semantic_tag tag, const ser_context&) override
    {
        event_ = basic_staj_event<CharT>(s.data(), s.length(), staj_event_type::string_value, tag);
        return false;
    }

    bool do_byte_string_value(const byte_string_view&, 
                              semantic_tag,
                              const ser_context&) override
    {
        JSONCONS_UNREACHABLE();
    }

    bool do_int64_value(int64_t value, 
                        semantic_tag tag,
                        const ser_context&) override
    {
        event_ = basic_staj_event<CharT>(value, tag);
        return false;
    }

    bool do_uint64_value(uint64_t value, 
                         semantic_tag tag, 
                         const ser_context&) override
    {
        event_ = basic_staj_event<CharT>(value, tag);
        return false;
    }

    bool do_double_value(double value, 
                         semantic_tag tag, 
                         const ser_context&) override
    {
        event_ = basic_staj_event<CharT>(value, tag);
        return false;
    }

    void do_flush() override
    {
    }
};

template<class CharT,class Src=jsoncons::stream_source<CharT>,class Allocator=std::allocator<char>>
class basic_json_cursor : public basic_staj_reader<CharT>, private virtual ser_context
{
public:
    typedef Src source_type;
    typedef CharT char_type;
    typedef Allocator allocator_type;
private:
    static const size_t default_max_buffer_length = 16384;

    basic_staj_event_handler<CharT> event_handler_;
    default_parse_error_handler default_err_handler_;

    default_basic_staj_filter<CharT> default_filter_;

    typedef typename std::allocator_traits<allocator_type>:: template rebind_alloc<CharT> char_allocator_type;

    basic_json_parser<CharT,Allocator> parser_;
    basic_staj_filter<CharT>& filter_;
    source_type source_;
    std::vector<CharT,char_allocator_type> buffer_;
    size_t buffer_length_;
    bool eof_;
    bool begin_;

    // Noncopyable and nonmoveable
    basic_json_cursor(const basic_json_cursor&) = delete;
    basic_json_cursor& operator=(const basic_json_cursor&) = delete;

public:
    typedef basic_string_view<CharT> string_view_type;

    // Constructors that throw parse exceptions
    template <class Source>
    basic_json_cursor(Source&& source)
        : basic_json_cursor(std::forward<Source>(source),
                            default_filter_,
                            basic_json_options<CharT>::default_options(),
                            default_err_handler_)
    {
    }

    template <class Source>
    basic_json_cursor(Source&& source,
                      basic_staj_filter<CharT>& filter)
        : basic_json_cursor(std::forward<Source>(source),
                            filter,basic_json_options<CharT>::default_options(),
                            default_err_handler_)
    {
    }

    template <class Source>
    basic_json_cursor(Source&& source,
                      parse_error_handler& err_handler)
        : basic_json_cursor(std::forward<Source>(source),
                            default_filter_,
                            basic_json_options<CharT>::default_options(),
                            err_handler)
    {
    }

    template <class Source>
    basic_json_cursor(Source&& source,
                      basic_staj_filter<CharT>& filter,
                      parse_error_handler& err_handler)
        : basic_json_cursor(std::forward<Source>(source),
                            filter,
                            basic_json_options<CharT>::default_options(),
                            err_handler)
    {
    }

    template <class Source>
    basic_json_cursor(Source&& source, 
                      const basic_json_decode_options<CharT>& options)
        : basic_json_cursor(std::forward<Source>(source),
                            default_filter_,
                            options,
                            default_err_handler_)
    {
    }

    template <class Source>
    basic_json_cursor(Source&& source,
                      basic_staj_filter<CharT>& filter, 
                      const basic_json_decode_options<CharT>& options)
        : basic_json_cursor(std::forward<Source>(source),
                            filter,
                            options,
                            default_err_handler_)
    {
    }

    template <class Source>
    basic_json_cursor(Source&& source, 
                      basic_staj_filter<CharT>& filter,
                      const basic_json_decode_options<CharT>& options,
                      parse_error_handler& err_handler,
                      typename std::enable_if<!std::is_constructible<basic_string_view<CharT>,Source>::value>::type* = 0)
       : parser_(options,err_handler),
         filter_(filter),
         source_(source),
         buffer_length_(default_max_buffer_length),
         eof_(false),
         begin_(true)
    {
        buffer_.reserve(buffer_length_);
        if (!done())
        {
            next();
        }
    }

    template <class Source>
    basic_json_cursor(Source&& source, 
                      basic_staj_filter<CharT>& filter,
                      const basic_json_decode_options<CharT>& options,
                      parse_error_handler& err_handler,
                      typename std::enable_if<std::is_constructible<basic_string_view<CharT>,Source>::value>::type* = 0)
       : parser_(options,err_handler),
         filter_(filter),
         buffer_length_(0),
         eof_(false),
         begin_(false)
    {
        basic_string_view<CharT> sv(std::forward<Source>(source));
        auto result = unicons::skip_bom(sv.begin(), sv.end());
        if (result.ec != unicons::encoding_errc())
        {
            throw ser_error(result.ec,parser_.line(),parser_.column());
        }
        size_t offset = result.it - sv.begin();
        parser_.update(sv.data()+offset,sv.size()-offset);
        if (!done())
        {
            next();
        }
    }

    // Constructors that set parse error codes
    template <class Source>
    basic_json_cursor(Source&& source,
                      std::error_code& ec)
        : basic_json_cursor(std::forward<Source>(source),default_filter_,basic_json_options<CharT>::default_options(),default_err_handler_,ec)
    {
    }

    template <class Source>
    basic_json_cursor(Source&& source,
                      basic_staj_filter<CharT>& filter,
                      std::error_code& ec)
        : basic_json_cursor(std::forward<Source>(source),
                            filter,
                            basic_json_options<CharT>::default_options(),
                            default_err_handler_,
                            ec)
    {
    }

    template <class Source>
    basic_json_cursor(Source&& source,
                      parse_error_handler& err_handler,
                      std::error_code& ec)
        : basic_json_cursor(std::forward<Source>(source),
                            default_filter_,
                            basic_json_options<CharT>::default_options(),
                            err_handler,
                            ec)
    {
    }

    template <class Source>
    basic_json_cursor(Source&& source,
                      basic_staj_filter<CharT>& filter,
                      parse_error_handler& err_handler,
                      std::error_code& ec)
        : basic_json_cursor(std::forward<Source>(source),
                            filter,
                            basic_json_options<CharT>::default_options(),
                            err_handler,
                            ec)
    {
    }

    template <class Source>
    basic_json_cursor(Source&& source, 
                      const basic_json_decode_options<CharT>& options,
                      std::error_code& ec)
        : basic_json_cursor(std::forward<Source>(source),
                            default_filter_,
                            options,
                            default_err_handler_,
                            ec)
    {
    }

    template <class Source>
    basic_json_cursor(Source&& source,
                      basic_staj_filter<CharT>& filter, 
                      const basic_json_decode_options<CharT>& options,
                      std::error_code& ec)
        : basic_json_cursor(std::forward<Source>(source),
                            filter,options,
                            default_err_handler_,
                            ec)
    {
    }

    template <class Source>
    basic_json_cursor(Source&& source, 
                      basic_staj_filter<CharT>& filter,
                      const basic_json_decode_options<CharT>& options,
                      parse_error_handler& err_handler,
                      std::error_code& ec,
                      typename std::enable_if<!std::is_constructible<basic_string_view<CharT>,Source>::value>::type* = 0)
       : parser_(options,err_handler),
         filter_(filter),
         source_(source),
         eof_(false),
         buffer_length_(default_max_buffer_length),
         begin_(true)
    {
        buffer_.reserve(buffer_length_);
        if (!done())
        {
            next(ec);
        }
    }

    template <class Source>
    basic_json_cursor(Source&& source, 
                      basic_staj_filter<CharT>& filter,
                      const basic_json_decode_options<CharT>& options,
                      parse_error_handler& err_handler,
                      std::error_code& ec,
                      typename std::enable_if<std::is_constructible<basic_string_view<CharT>,Source>::value>::type* = 0)
       : parser_(options,err_handler),
         filter_(filter),
         eof_(false),
         buffer_length_(0),
         begin_(false)
    {
        basic_string_view<CharT> sv(std::forward<Source>(source));
        auto result = unicons::skip_bom(sv.begin(), sv.end());
        if (result.ec != unicons::encoding_errc())
        {
            ec = result.ec;
            return;
        }
        size_t offset = result.it - sv.begin();
        parser_.update(sv.data()+offset,sv.size()-offset);
        if (!done())
        {
            next(ec);
        }
    }

    size_t buffer_length() const
    {
        return buffer_length_;
    }

    void buffer_length(size_t length)
    {
        buffer_length_ = length;
        buffer_.reserve(buffer_length_);
    }

    bool done() const override
    {
        return parser_.done();
    }

    const basic_staj_event<CharT>& current() const override
    {
        return event_handler_.event();
    }

    void accept(basic_json_content_handler<CharT>& handler) override
    {
        std::error_code ec;
        accept(handler, ec);
        if (ec)
        {
            throw ser_error(ec,parser_.line(),parser_.column());
        }
    }

    void accept(basic_json_content_handler<CharT>& handler,
                std::error_code& ec) override
    {
        switch (event_handler_.event().event_type())
        {
            case staj_event_type::begin_array:
                if (!handler.begin_array(semantic_tag::none, *this))
                {
                    return;
                }
                break;
            case staj_event_type::end_array:
                if (!handler.end_array(*this))
                {
                    return;
                }
                break;
            case staj_event_type::begin_object:
                if (!handler.begin_object(semantic_tag::none, *this))
                {
                    return;
                }
                break;
            case staj_event_type::end_object:
                if (!handler.end_object(*this))
                {
                    return;
                }
                break;
            case staj_event_type::name:
                if (!handler.name(event_handler_.event().template as<jsoncons::basic_string_view<CharT>>(), *this))
                {
                    return;
                }
                break;
            case staj_event_type::string_value:
                if (!handler.string_value(event_handler_.event().template as<jsoncons::basic_string_view<CharT>>(), semantic_tag::none, *this))
                {
                    return;
                }
                break;
            case staj_event_type::null_value:
                if (!handler.null_value(semantic_tag::none, *this))
                {
                    return;
                }
                break;
            case staj_event_type::bool_value:
                if (!handler.bool_value(event_handler_.event().template as<bool>(), semantic_tag::none, *this))
                {
                    return;
                }
                break;
            case staj_event_type::int64_value:
                if (!handler.int64_value(event_handler_.event().template as<int64_t>(), semantic_tag::none, *this))
                {
                    return;
                }
                break;
            case staj_event_type::uint64_value:
                if (!handler.uint64_value(event_handler_.event().template as<uint64_t>(), semantic_tag::none, *this))
                {
                    return;
                }
                break;
            case staj_event_type::double_value:
                if (!handler.double_value(event_handler_.event().template as<double>(), semantic_tag::none, *this))
                {
                    return;
                }
                break;
            default:
                break;
        }
        do
        {
            read_next(handler, ec);
        } 
        while (!ec && !done() && !filter_.accept(event_handler_.event(), *this));
    }

    void next() override
    {
        std::error_code ec;
        next(ec);
        if (ec)
        {
            throw ser_error(ec,parser_.line(),parser_.column());
        }
    }

    void next(std::error_code& ec) override
    {
        do
        {
            read_next(ec);
        } 
        while (!ec && !done() && !filter_.accept(event_handler_.event(), *this));
    }

    void read_buffer(std::error_code& ec)
    {
        buffer_.clear();
        buffer_.resize(buffer_length_);
        size_t count = source_.read(buffer_.data(), buffer_length_);
        buffer_.resize(static_cast<size_t>(count));
        if (buffer_.size() == 0)
        {
            eof_ = true;
        }
        else if (begin_)
        {
            auto result = unicons::skip_bom(buffer_.begin(), buffer_.end());
            if (result.ec != unicons::encoding_errc())
            {
                ec = result.ec;
                return;
            }
            size_t offset = result.it - buffer_.begin();
            parser_.update(buffer_.data()+offset,buffer_.size()-offset);
            begin_ = false;
        }
        else
        {
            parser_.update(buffer_.data(),buffer_.size());
        }
    }

    void read_next(std::error_code& ec)
    {
        read_next(event_handler_, ec);
    }

    void read_next(basic_json_content_handler<CharT>& handler, std::error_code& ec)
    {
        parser_.restart();
        while (!parser_.stopped())
        {
            if (parser_.source_exhausted())
            {
                if (!source_.eof())
                {
                    read_buffer(ec);
                    if (ec) return;
                }
                else
                {
                    eof_ = true;
                }
            }
            parser_.parse_some(handler, ec);
            if (ec) return;
        }
    }

    void check_done()
    {
        std::error_code ec;
        check_done(ec);
        if (ec)
        {
            throw ser_error(ec,parser_.line(),parser_.column());
        }
    }

    const ser_context& context() const override
    {
        return *this;
    }

    void check_done(std::error_code& ec)
    {
        try
        {
            if (source_.is_error())
            {
                ec = json_errc::source_error;
                return;
            }   
            if (eof_)
            {
                parser_.check_done(ec);
                if (ec) return;
            }
            else
            {
                while (!eof_)
                {
                    if (parser_.source_exhausted())
                    {
                        if (!source_.eof())
                        {
                            read_buffer(ec);     
                            if (ec) return;
                        }
                        else
                        {
                            eof_ = true;
                        }
                    }
                    if (!eof_)
                    {
                        parser_.check_done(ec);
                        if (ec) return;
                    }
                }
            }
        }
        catch (const ser_error& e)
        {
            ec = e.code();
        }
    }

    bool eof() const
    {
        return eof_;
    }

    size_t line() const override
    {
        return parser_.line();
    }

    size_t column() const override
    {
        return parser_.column();
    }
private:
};

typedef basic_json_cursor<char> json_cursor;
typedef basic_json_cursor<wchar_t> wjson_cursor;

#if !defined(JSONCONS_NO_DEPRECATED)
template<class CharT,class Src,class Allocator=std::allocator<CharT>>
using basic_json_pull_reader = basic_json_cursor<CharT,Src,Allocator>;
typedef basic_json_cursor<char> json_pull_reader;
typedef basic_json_cursor<wchar_t> wjson_pull_reader;

template<class CharT,class Src,class Allocator=std::allocator<CharT>>
using basic_json_stream_reader = basic_json_cursor<CharT,Src,Allocator>;

template<class CharT,class Src,class Allocator=std::allocator<CharT>>
using basic_json_staj_reader = basic_json_cursor<CharT,Src,Allocator>;

typedef basic_json_cursor<char> json_stream_reader;
typedef basic_json_cursor<wchar_t> wjson_stream_reader;

typedef basic_json_cursor<char> json_staj_reader;
typedef basic_json_cursor<wchar_t> wjson_staj_reader;
#endif

}

#endif

