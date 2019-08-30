// Copyright 2015 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_JSON_READER_HPP
#define JSONCONS_JSON_READER_HPP

#include <memory> // std::allocator
#include <string>
#include <vector>
#include <stdexcept>
#include <system_error>
#include <ios>
#include <utility> // std::move
#include <jsoncons/source.hpp>
#include <jsoncons/json_exception.hpp>
#include <jsoncons/json_content_handler.hpp>
#include <jsoncons/parse_error_handler.hpp>
#include <jsoncons/json_parser.hpp>

namespace jsoncons {

// utf8_other_json_input_adapter

template <class CharT>
class json_utf8_other_content_handler_adapter : public json_content_handler
{
public:
    using json_content_handler::string_view_type;
private:
    basic_null_json_content_handler<CharT> default_content_handler_;
    basic_json_content_handler<CharT>& other_handler_;
    //parse_error_handler& err_handler_;

    // noncopyable and nonmoveable
    json_utf8_other_content_handler_adapter<CharT>(const json_utf8_other_content_handler_adapter<CharT>&) = delete;
    json_utf8_other_content_handler_adapter<CharT>& operator=(const json_utf8_other_content_handler_adapter<CharT>&) = delete;

public:
    json_utf8_other_content_handler_adapter()
        : other_handler_(default_content_handler_)
    {
    }

    json_utf8_other_content_handler_adapter(basic_json_content_handler<CharT>& other_handler/*,
                                          parse_error_handler& err_handler*/)
        : other_handler_(other_handler)/*,
          err_handler_(err_handler)*/
    {
    }

private:

    void do_flush() override
    {
        other_handler_.flush();
    }

    bool do_begin_object(semantic_tag tag, const ser_context& context) override
    {
        return other_handler_.begin_object(tag, context);
    }

    bool do_end_object(const ser_context& context) override
    {
        return other_handler_.end_object(context);
    }

    bool do_begin_array(semantic_tag tag, const ser_context& context) override
    {
        return other_handler_.begin_array(tag, context);
    }

    bool do_end_array(const ser_context& context) override
    {
        return other_handler_.end_array(context);
    }

    bool do_name(const string_view_type& name, const ser_context& context) override
    {
        std::basic_string<CharT> target;
        auto result = unicons::convert(
            name.begin(), name.end(), std::back_inserter(target), 
            unicons::conv_flags::strict);
        if (result.ec != unicons::conv_errc())
        {
            throw ser_error(result.ec,context.line(),context.column());
        }
        return other_handler_.name(target, context);
    }

    bool do_string_value(const string_view_type& value, semantic_tag tag, const ser_context& context) override
    {
        std::basic_string<CharT> target;
        auto result = unicons::convert(
            value.begin(), value.end(), std::back_inserter(target), 
            unicons::conv_flags::strict);
        if (result.ec != unicons::conv_errc())
        {
            throw ser_error(result.ec,context.line(),context.column());
        }
        return other_handler_.string_value(target, tag, context);
    }

    bool do_int64_value(int64_t value, 
                        semantic_tag tag, 
                        const ser_context& context) override
    {
        return other_handler_.int64_value(value, tag, context);
    }

    bool do_uint64_value(uint64_t value, 
                         semantic_tag tag, 
                         const ser_context& context) override
    {
        return other_handler_.uint64_value(value, tag, context);
    }

    bool do_double_value(double value, 
                         semantic_tag tag,
                         const ser_context& context) override
    {
        return other_handler_.double_value(value, tag, context);
    }

    bool do_bool_value(bool value, semantic_tag tag, const ser_context& context) override
    {
        return other_handler_.bool_value(value, tag, context);
    }

    bool do_null_value(semantic_tag tag, const ser_context& context) override
    {
        return other_handler_.null_value(tag, context);
    }
};

template<class CharT,class Src=jsoncons::stream_source<CharT>,class Allocator=std::allocator<char>>
class basic_json_reader 
{
public:
    typedef CharT char_type;
    typedef Src source_type;
    typedef basic_string_view<CharT> string_view_type;
    typedef Allocator allocator_type;
private:
    typedef typename std::allocator_traits<allocator_type>:: template rebind_alloc<CharT> char_allocator_type;

    static const size_t default_max_buffer_length = 16384;

    basic_null_json_content_handler<CharT> default_content_handler_;
    default_parse_error_handler default_err_handler_;

    basic_json_content_handler<CharT>& handler_;

    basic_json_parser<CharT,Allocator> parser_;

    source_type source_;
    std::vector<CharT,char_allocator_type> buffer_;
    size_t buffer_length_;
    bool eof_;
    bool begin_;

    // Noncopyable and nonmoveable
    basic_json_reader(const basic_json_reader&) = delete;
    basic_json_reader& operator=(const basic_json_reader&) = delete;

public:
    template <class Source>
    explicit basic_json_reader(Source&& source)
        : basic_json_reader(std::forward<Source>(source),
                            default_content_handler_,
                            basic_json_options<CharT>::default_options(),
                            default_err_handler_)
    {
    }

    template <class Source>
    basic_json_reader(Source&& source, 
                      const basic_json_decode_options<CharT>& options)
        : basic_json_reader(std::forward<Source>(source),
                            default_content_handler_,
                            options,
                            default_err_handler_)
    {
    }

    template <class Source>
    basic_json_reader(Source&& source,
                      parse_error_handler& err_handler)
        : basic_json_reader(std::forward<Source>(source),
                            default_content_handler_,
                            basic_json_options<CharT>::default_options(),
                            err_handler)
    {
    }

    template <class Source>
    basic_json_reader(Source&& source, 
                      const basic_json_decode_options<CharT>& options,
                      parse_error_handler& err_handler)
        : basic_json_reader(std::forward<Source>(source),
                            default_content_handler_,
                            options,
                            err_handler)
    {
    }

    template <class Source>
    basic_json_reader(Source&& source, 
                      basic_json_content_handler<CharT>& handler)
        : basic_json_reader(std::forward<Source>(source),
                            handler,
                            basic_json_options<CharT>::default_options(),
                            default_err_handler_)
    {
    }

    template <class Source>
    basic_json_reader(Source&& source, 
                      basic_json_content_handler<CharT>& handler,
                      const basic_json_decode_options<CharT>& options)
        : basic_json_reader(std::forward<Source>(source),
                            handler,
                            options,
                            default_err_handler_)
    {
    }

    template <class Source>
    basic_json_reader(Source&& source,
                      basic_json_content_handler<CharT>& handler,
                      parse_error_handler& err_handler)
        : basic_json_reader(std::forward<Source>(source),
                            handler,
                            basic_json_options<CharT>::default_options(),
                            err_handler)
    {
    }

    template <class Source>
    basic_json_reader(Source&& source,
                      basic_json_content_handler<CharT>& handler, 
                      const basic_json_decode_options<CharT>& options,
                      parse_error_handler& err_handler,
                      typename std::enable_if<!std::is_constructible<basic_string_view<CharT>,Source>::value>::type* = 0)
       : handler_(handler),
         parser_(options,err_handler),
         source_(std::forward<Source>(source)),
         buffer_length_(default_max_buffer_length),
         eof_(false),
         begin_(true)
    {
        buffer_.reserve(buffer_length_);
    }

    template <class Source>
    basic_json_reader(Source&& source,
                      basic_json_content_handler<CharT>& handler, 
                      const basic_json_decode_options<CharT>& options,
                      parse_error_handler& err_handler,
                      typename std::enable_if<std::is_constructible<basic_string_view<CharT>,Source>::value>::type* = 0)
       : handler_(handler),
         parser_(options,err_handler),
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
#if !defined(JSONCONS_NO_DEPRECATED)
    size_t max_nesting_depth() const
    {
        return parser_.max_nesting_depth();
    }

    void max_nesting_depth(size_t depth)
    {
        parser_.max_nesting_depth(depth);
    }
#endif
    void read_next()
    {
        std::error_code ec;
        read_next(ec);
        if (ec)
        {
            throw ser_error(ec,parser_.line(),parser_.column());
        }
    }

    void read_next(std::error_code& ec)
    {
        try
        {
            if (source_.is_error())
            {
                ec = json_errc::source_error;
                return;
            }        
            parser_.reset();
            while (!parser_.finished())
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
                parser_.parse_some(handler_, ec);
                if (ec) return;
            }
            
            while (!eof_)
            {
                parser_.skip_whitespace();
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
                else
                {
                    break;
                }
            }
        }
        catch (const ser_error& e)
        {
            ec = e.code();
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

    size_t line() const
    {
        return parser_.line();
    }

    size_t column() const
    {
        return parser_.column();
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

    void read()
    {
        read_next();
        check_done();
    }

    void read(std::error_code& ec)
    {
        read_next(ec);
        if (!ec)
        {
            check_done(ec);
        }
    }

#if !defined(JSONCONS_NO_DEPRECATED)

    size_t buffer_capacity() const
    {
        return buffer_length_;
    }

    void buffer_capacity(size_t length)
    {
        buffer_length_ = length;
        buffer_.reserve(buffer_length_);
    }
    size_t max_depth() const
    {
        return parser_.max_nesting_depth();
    }

    void max_depth(size_t depth)
    {
        parser_.max_nesting_depth(depth);
    }
#endif

private:

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
};

typedef basic_json_reader<char> json_reader;
typedef basic_json_reader<wchar_t> wjson_reader;
#if !defined(JSONCONS_NO_DEPRECATED)
typedef basic_json_reader<char,jsoncons::string_source<char>> json_string_reader;
typedef basic_json_reader<wchar_t, jsoncons::string_source<wchar_t>> wjson_string_reader;
#endif

}

#endif

