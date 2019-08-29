// Copyright 2013 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_CSV_CSV_ENCODER_HPP
#define JSONCONS_CSV_CSV_ENCODER_HPP

#include <array> // std::array
#include <string>
#include <sstream>
#include <vector>
#include <ostream>
#include <utility> // std::move
#include <unordered_map> // std::unordered_map
#include <memory> // std::allocator
#include <limits> // std::numeric_limits
#include <jsoncons/json_exception.hpp>
#include <jsoncons/json_content_handler.hpp>
#include <jsoncons/detail/print_number.hpp>
#include <jsoncons_ext/csv/csv_options.hpp>
#include <jsoncons/result.hpp>

namespace jsoncons { namespace csv {

template<class CharT,class Result=jsoncons::stream_result<CharT>,class Allocator=std::allocator<CharT>>
class basic_csv_encoder final : public basic_json_content_handler<CharT>
{
    static const std::array<CharT, 4>& null_k()
    {
        static constexpr std::array<CharT,4> k{'n','u','l','l'};
        return k;
    }
    static const std::array<CharT, 4>& true_k()
    {
        static constexpr std::array<CharT,4> k{'t','r','u','e'};
        return k;
    }
    static const std::array<CharT, 5>& false_k()
    {
        static constexpr std::array<CharT,5> k{'f','a','l','s','e'};
        return k;
    }

public:
    typedef CharT char_type;
    using typename basic_json_content_handler<CharT>::string_view_type;
    typedef Result result_type;

    typedef Allocator allocator_type;
    typedef typename std::allocator_traits<allocator_type>:: template rebind_alloc<CharT> char_allocator_type;
    typedef std::basic_string<CharT, std::char_traits<CharT>, char_allocator_type> string_type;
    typedef typename std::allocator_traits<allocator_type>:: template rebind_alloc<string_type> string_allocator_type;

private:
    struct stack_item
    {
        stack_item(bool is_object)
           : is_object_(is_object), count_(0)
        {
        }
        bool is_object() const
        {
            return is_object_;
        }

        bool is_object_;
        size_t count_;
        string_type name_;
    };
    Result result_;
    const basic_csv_encode_options<CharT>& options_;
    std::vector<stack_item> stack_;
    jsoncons::detail::print_double fp_;
    std::vector<string_type,string_allocator_type> column_names_;

    typedef typename std::allocator_traits<allocator_type>:: template rebind_alloc<std::pair<const string_type,string_type>> string_string_allocator_type;
    std::unordered_map<string_type,string_type, std::hash<string_type>,std::equal_to<string_type>,string_string_allocator_type> buffered_line_;

    // Noncopyable and nonmoveable
    basic_csv_encoder(const basic_csv_encoder&) = delete;
    basic_csv_encoder& operator=(const basic_csv_encoder&) = delete;
public:
    basic_csv_encoder(result_type result)
       : basic_csv_encoder(std::move(result), basic_csv_options<CharT>::default_options())
    {
    }

    basic_csv_encoder(result_type result,
                      const basic_csv_encode_options<CharT>& options)
       :
       result_(std::move(result)),
       options_(options),
       stack_(),
       fp_(floating_point_options(options.floating_point_format(), 
                                  options.precision(),
                                  0)),
       column_names_(options_.column_names())
    {
    }

    ~basic_csv_encoder()
    {
        try
        {
            result_.flush();
        }
        catch (...)
        {
        }
    }

private:

    template<class AnyWriter>
    void escape_string(const CharT* s,
                       size_t length,
                       CharT quote_char, CharT quote_escape_char,
                       AnyWriter& result)
    {
        const CharT* begin = s;
        const CharT* end = s + length;
        for (const CharT* it = begin; it != end; ++it)
        {
            CharT c = *it;
            if (c == quote_char)
            {
                result.push_back(quote_escape_char); 
                result.push_back(quote_char);
            }
            else
            {
                result.push_back(c);
            }
        }
    }

    void do_flush() override
    {
        result_.flush();
    }

    bool do_begin_object(semantic_tag, const ser_context&) override
    {
        stack_.push_back(stack_item(true));
        return true;
    }

    bool do_end_object(const ser_context&) override
    {
        if (stack_.size() == 2)
        {
            if (stack_[0].count_ == 0)
            {
                for (size_t i = 0; i < column_names_.size(); ++i)
                {
                    if (i > 0)
                    {
                        result_.push_back(options_.field_delimiter());
                    }
                    result_.append(column_names_[i].data(),
                                  column_names_[i].length());
                }
                result_.append(options_.line_delimiter().data(),
                              options_.line_delimiter().length());
            }
            for (size_t i = 0; i < column_names_.size(); ++i)
            {
                if (i > 0)
                {
                    result_.push_back(options_.field_delimiter());
                }
                auto it = buffered_line_.find(column_names_[i]);
                if (it != buffered_line_.end())
                {
                    result_.append(it->second.data(),it->second.length());
                    it->second.clear();
                }
            }
            result_.append(options_.line_delimiter().data(), options_.line_delimiter().length());
        }
        stack_.pop_back();

        end_value();
        return true;
    }

    bool do_begin_array(semantic_tag, const ser_context&) override
    {
        stack_.push_back(stack_item(false));
        if (stack_.size() == 2)
        {
            if (stack_[0].count_ == 0)
            {
                for (size_t i = 0; i < column_names_.size(); ++i)
                {
                    if (i > 0)
                    {
                        result_.push_back(options_.field_delimiter());
                    }
                    result_.append(column_names_[i].data(),column_names_[i].length());
                }
                if (column_names_.size() > 0)
                {
                    result_.append(options_.line_delimiter().data(),
                                  options_.line_delimiter().length());
                }
            }
        }
        return true;
    }

    bool do_end_array(const ser_context&) override
    {
        if (stack_.size() == 2)
        {
            result_.append(options_.line_delimiter().data(),
                          options_.line_delimiter().length());
        }
        stack_.pop_back();

        end_value();
        return true;
    }

    bool do_name(const string_view_type& name, const ser_context&) override
    {
        if (stack_.size() == 2)
        {
            stack_.back().name_ = string_type(name);
            buffered_line_[string_type(name)] = std::basic_string<CharT>();
            if (stack_[0].count_ == 0 && options_.column_names().size() == 0)
            {
                column_names_.push_back(string_type(name));
            }
        }
        return true;
    }

    template <class AnyWriter>
    bool string_value(const CharT* s, size_t length, AnyWriter& result)
    {
        bool quote = false;
        if (options_.quote_style() == quote_style_type::all || options_.quote_style() == quote_style_type::nonnumeric ||
            (options_.quote_style() == quote_style_type::minimal &&
            (std::char_traits<CharT>::find(s, length, options_.field_delimiter()) != nullptr || std::char_traits<CharT>::find(s, length, options_.quote_char()) != nullptr)))
        {
            quote = true;
            result.push_back(options_.quote_char());
        }
        escape_string(s, length, options_.quote_char(), options_.quote_escape_char(), result);
        if (quote)
        {
            result.push_back(options_.quote_char());
        }

        return true;
    }

    bool do_null_value(semantic_tag, const ser_context&) override
    {
        if (stack_.size() == 2)
        {
            if (stack_.back().is_object())
            {
                auto it = buffered_line_.find(stack_.back().name_);
                if (it != buffered_line_.end())
                {
                    std::basic_string<CharT> s;
                    jsoncons::string_result<std::basic_string<CharT>> bo(s);
                    accept_null_value(bo);
                    bo.flush();
                    it->second = s;
                }
            }
            else
            {
                accept_null_value(result_);
            }
        }
        return true;
    }

    bool do_string_value(const string_view_type& sv, semantic_tag, const ser_context&) override
    {
        if (stack_.size() == 2)
        {
            if (stack_.back().is_object())
            {
                auto it = buffered_line_.find(stack_.back().name_);
                if (it != buffered_line_.end())
                {
                    std::basic_string<CharT> s;
                    jsoncons::string_result<std::basic_string<CharT>> bo(s);
                    value(sv,bo);
                    bo.flush();
                    it->second = s;
                }
            }
            else
            {
                value(sv,result_);
            }
        }
        return true;
    }

    bool do_byte_string_value(const byte_string_view& b, 
                              semantic_tag tag, 
                              const ser_context& context) override
    {
        byte_string_chars_format encoding_hint;
        switch (tag)
        {
            case semantic_tag::base16:
                encoding_hint = byte_string_chars_format::base16;
                break;
            case semantic_tag::base64:
                encoding_hint = byte_string_chars_format::base64;
                break;
            case semantic_tag::base64url:
                encoding_hint = byte_string_chars_format::base64url;
                break;
            default:
                encoding_hint = byte_string_chars_format::none;
                break;
        }
        byte_string_chars_format format = jsoncons::detail::resolve_byte_string_chars_format(encoding_hint,byte_string_chars_format::none,byte_string_chars_format::base64url);

        std::basic_string<CharT> s;
        switch (format)
        {
            case byte_string_chars_format::base16:
            {
                encode_base16(b.begin(),b.end(),s);
                do_string_value(s, semantic_tag::none, context);
                break;
            }
            case byte_string_chars_format::base64:
            {
                encode_base64(b.begin(),b.end(),s);
                do_string_value(s, semantic_tag::none, context);
                break;
            }
            case byte_string_chars_format::base64url:
            {
                encode_base64url(b.begin(),b.end(),s);
                do_string_value(s, semantic_tag::none, context);
                break;
            }
            default:
            {
                JSONCONS_UNREACHABLE();
            }
        }

        return true;
    }

    bool do_double_value(double val, 
                         semantic_tag, 
                         const ser_context&) override
    {
        if (stack_.size() == 2)
        {
            if (stack_.back().is_object())
            {
                auto it = buffered_line_.find(stack_.back().name_);
                if (it != buffered_line_.end())
                {
                    std::basic_string<CharT> s;
                    jsoncons::string_result<std::basic_string<CharT>> bo(s);
                    value(val, bo);
                    bo.flush();
                    it->second = s;
                }
            }
            else
            {
                value(val, result_);
            }
        }
        return true;
    }

    bool do_int64_value(int64_t val, 
                        semantic_tag, 
                        const ser_context&) override
    {
        if (stack_.size() == 2)
        {
            if (stack_.back().is_object())
            {
                auto it = buffered_line_.find(stack_.back().name_);
                if (it != buffered_line_.end())
                {
                    std::basic_string<CharT> s;
                    jsoncons::string_result<std::basic_string<CharT>> bo(s);
                    value(val,bo);
                    bo.flush();
                    it->second = s;
                }
            }
            else
            {
                value(val,result_);
            }
        }
        return true;
    }

    bool do_uint64_value(uint64_t val, 
                         semantic_tag, 
                         const ser_context&) override
    {
        if (stack_.size() == 2)
        {
            if (stack_.back().is_object())
            {
                auto it = buffered_line_.find(stack_.back().name_);
                if (it != buffered_line_.end())
                {
                    std::basic_string<CharT> s;
                    jsoncons::string_result<std::basic_string<CharT>> bo(s);
                    value(val,bo);
                    bo.flush();
                    it->second = s;
                }
            }
            else
            {
                value(val,result_);
            }
        }
        return true;
    }

    bool do_bool_value(bool val, semantic_tag, const ser_context&) override
    {
        if (stack_.size() == 2)
        {
            if (stack_.back().is_object())
            {
                auto it = buffered_line_.find(stack_.back().name_);
                if (it != buffered_line_.end())
                {
                    std::basic_string<CharT> s;
                    jsoncons::string_result<std::basic_string<CharT>> bo(s);
                    value(val,bo);
                    bo.flush();
                    it->second = s;
                }
            }
            else
            {
                value(val,result_);
            }
        }
        return true;
    }

    template <class AnyWriter>
    void value(const string_view_type& value, AnyWriter& result)
    {
        begin_value(result);
        string_value(value.data(),value.length(),result);
        end_value();
    }

    template <class AnyWriter>
    void value(double val, AnyWriter& result)
    {
        begin_value(result);

        if ((std::isnan)(val))
        {
            result.append(null_k().data(), null_k().size());
        }
        else if (val == std::numeric_limits<double>::infinity())
        {
            result.append(null_k().data(), null_k().size());
        }
        else if (!(std::isfinite)(val))
        {
            result.append(null_k().data(), null_k().size());
        }
        else
        {
            fp_(val, result);
        }

        end_value();

    }

    template <class AnyWriter>
    void value(int64_t val, AnyWriter& result)
    {
        begin_value(result);

        std::basic_ostringstream<CharT> ss;
        ss << val;
        result.append(ss.str().data(),ss.str().length());

        end_value();
    }

    template <class AnyWriter>
    void value(uint64_t val, AnyWriter& result)
    {
        begin_value(result);

        std::basic_ostringstream<CharT> ss;
        ss << val;
        result.append(ss.str().data(),ss.str().length());

        end_value();
    }

    template <class AnyWriter>
    void value(bool val, AnyWriter& result) 
    {
        begin_value(result);

        if (val)
        {
            result.append(true_k().data(), true_k().size());
        }
        else
        {
            result.append(false_k().data(), false_k().size());
        }

        end_value();
    }

    template <class AnyWriter>
    bool accept_null_value(AnyWriter& result) 
    {
        begin_value(result);
        result.append(null_k().data(), null_k().size());
        end_value();
        return true;
    }

    template <class AnyWriter>
    void begin_value(AnyWriter& result)
    {
        if (!stack_.empty())
        {
            if (!stack_.back().is_object_ && stack_.back().count_ > 0)
            {
                result.push_back(options_.field_delimiter());
            }
        }
    }

    void end_value()
    {
        if (!stack_.empty())
        {
            ++stack_.back().count_;
        }
    }
};

// encode_csv

template <class T,class CharT>
typename std::enable_if<is_basic_json_class<T>::value,void>::type 
encode_csv(const T& j, std::basic_string<CharT>& s, const basic_csv_options<CharT>& options = basic_csv_options<CharT>::default_options())
{
    typedef CharT char_type;
    basic_csv_encoder<char_type,jsoncons::string_result<std::basic_string<char_type>>> encoder(s,options);
    j.dump(encoder);
}

template <class T,class CharT>
typename std::enable_if<!is_basic_json_class<T>::value,void>::type 
encode_csv(const T& val, std::basic_string<CharT>& s, const basic_csv_options<CharT>& options = basic_csv_options<CharT>::default_options())
{
    typedef CharT char_type;
    basic_csv_encoder<char_type,jsoncons::string_result<std::basic_string<char_type>>> encoder(s,options);
    write_to(basic_json<CharT>(), val, encoder);
}

template <class T, class CharT>
typename std::enable_if<is_basic_json_class<T>::value,void>::type 
encode_csv(const T& j, std::basic_ostream<CharT>& os, const basic_csv_options<CharT>& options = basic_csv_options<CharT>::default_options())
{
    typedef CharT char_type;
    basic_csv_encoder<char_type,jsoncons::stream_result<char_type>> encoder(os,options);
    j.dump(encoder);
}

template <class T, class CharT>
typename std::enable_if<!is_basic_json_class<T>::value,void>::type 
encode_csv(const T& val, std::basic_ostream<CharT>& os, const basic_csv_options<CharT>& options = basic_csv_options<CharT>::default_options())
{
    typedef CharT char_type;
    basic_csv_encoder<char_type,jsoncons::stream_result<char_type>> encoder(os,options);
    write_to(basic_json<CharT>(), val, encoder);
}

typedef basic_csv_encoder<char> csv_encoder;
typedef basic_csv_encoder<char,jsoncons::string_result<std::string>> csv_string_encoder;

#if !defined(JSONCONS_NO_DEPRECATED)
template<class CharT, class Result = jsoncons::stream_result<CharT>, class Allocator = std::allocator<CharT>>
using basic_csv_serializer = basic_csv_encoder<CharT,Result,Allocator>;

typedef basic_csv_serializer<char> csv_serializer;
typedef basic_csv_serializer<char,jsoncons::string_result<std::string>> csv_string_serializer;
#endif

}}

#endif
