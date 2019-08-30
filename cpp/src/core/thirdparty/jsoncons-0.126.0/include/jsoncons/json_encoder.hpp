// Copyright 2013 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_JSON_ENCODER_HPP
#define JSONCONS_JSON_ENCODER_HPP

#include <array> // std::array
#include <string>
#include <vector>
#include <cmath> // std::isfinite, std::isnan
#include <limits> // std::numeric_limits
#include <memory>
#include <utility> // std::move
#include <jsoncons/json_exception.hpp>
#include <jsoncons/byte_string.hpp>
#include <jsoncons/bignum.hpp>
#include <jsoncons/json_options.hpp>
#include <jsoncons/json_error.hpp>
#include <jsoncons/json_content_handler.hpp>
#include <jsoncons/result.hpp>
#include <jsoncons/detail/print_number.hpp>

namespace jsoncons { namespace detail {
template <class CharT, class Result>
size_t escape_string(const CharT* s, size_t length,
                     bool escape_all_non_ascii, bool escape_solidus,
                     Result& result)
{
    size_t count = 0;
    const CharT* begin = s;
    const CharT* end = s + length;
    for (const CharT* it = begin; it != end; ++it)
    {
        CharT c = *it;
        switch (c)
        {
            case '\\':
                result.push_back('\\');
                result.push_back('\\');
                count += 2;
                break;
            case '"':
                result.push_back('\\');
                result.push_back('\"');
                count += 2;
                break;
            case '\b':
                result.push_back('\\');
                result.push_back('b');
                count += 2;
                break;
            case '\f':
                result.push_back('\\');
                result.push_back('f');
                count += 2;
                break;
            case '\n':
                result.push_back('\\');
                result.push_back('n');
                count += 2;
                break;
            case '\r':
                result.push_back('\\');
                result.push_back('r');
                count += 2;
                break;
            case '\t':
                result.push_back('\\');
                result.push_back('t');
                count += 2;
                break;
            default:
                if (escape_solidus && c == '/')
                {
                    result.push_back('\\');
                    result.push_back('/');
                    count += 2;
                }
                else if (is_control_character(c) || escape_all_non_ascii)
                {
                    // convert utf8 to codepoint
                    unicons::sequence_generator<const CharT*> g(it, end, unicons::conv_flags::strict);
                    if (g.done() || g.status() != unicons::conv_errc())
                    {
                        throw ser_error(json_errc::illegal_codepoint);
                    }
                    uint32_t cp = g.get().codepoint();
                    it += (g.get().length() - 1);
                    if (is_non_ascii_codepoint(cp) || is_control_character(c))
                    {
                        if (cp > 0xFFFF)
                        {
                            cp -= 0x10000;
                            uint32_t first = (cp >> 10) + 0xD800;
                            uint32_t second = ((cp & 0x03FF) + 0xDC00);

                            result.push_back('\\');
                            result.push_back('u');
                            result.push_back(to_hex_character(first >> 12 & 0x000F));
                            result.push_back(to_hex_character(first >> 8 & 0x000F));
                            result.push_back(to_hex_character(first >> 4 & 0x000F));
                            result.push_back(to_hex_character(first & 0x000F));
                            result.push_back('\\');
                            result.push_back('u');
                            result.push_back(to_hex_character(second >> 12 & 0x000F));
                            result.push_back(to_hex_character(second >> 8 & 0x000F));
                            result.push_back(to_hex_character(second >> 4 & 0x000F));
                            result.push_back(to_hex_character(second & 0x000F));
                            count += 12;
                        }
                        else
                        {
                            result.push_back('\\');
                            result.push_back('u');
                            result.push_back(to_hex_character(cp >> 12 & 0x000F));
                            result.push_back(to_hex_character(cp >> 8 & 0x000F));
                            result.push_back(to_hex_character(cp >> 4 & 0x000F));
                            result.push_back(to_hex_character(cp & 0x000F));
                            count += 6;
                        }
                    }
                    else
                    {
                        result.push_back(c);
                        ++count;
                    }
                }
                else
                {
                    result.push_back(c);
                    ++count;
                }
                break;
        }
    }
    return count;
}

inline
byte_string_chars_format resolve_byte_string_chars_format(byte_string_chars_format format1,
                                                          byte_string_chars_format format2,
                                                          byte_string_chars_format default_format = byte_string_chars_format::base64url)
{
    byte_string_chars_format result;
    switch (format1)
    {
        case byte_string_chars_format::base16:
        case byte_string_chars_format::base64:
        case byte_string_chars_format::base64url:
            result = format1;
            break;
        default:
            switch (format2)
            {
                case byte_string_chars_format::base64url:
                case byte_string_chars_format::base64:
                case byte_string_chars_format::base16:
                    result = format2;
                    break;
                default: // base64url
                {
                    result = default_format;
                    break;
                }
            }
            break;
    }
    return result;
}

}}

namespace jsoncons {

template<class CharT,class Result=jsoncons::stream_result<CharT>>
class basic_json_encoder final : public basic_json_content_handler<CharT>
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
    typedef typename basic_json_options<CharT>::string_type string_type;

private:
    enum class container_type {object, array};

    class encoding_context
    {
        container_type type_;
        size_t count_;
        line_split_kind line_splits_;
        bool indent_before_;
        bool new_line_after_;
        size_t begin_pos_;
        size_t data_pos_;
    public:
        encoding_context(container_type type, line_split_kind split_lines, bool indent_once,
                              size_t begin_pos, size_t data_pos)
           : type_(type), count_(0), line_splits_(split_lines), indent_before_(indent_once), new_line_after_(false),
             begin_pos_(begin_pos), data_pos_(data_pos)
        {
        }

        void set_position(size_t pos)
        {
            data_pos_ = pos;
        }

        size_t begin_pos() const
        {
            return begin_pos_;
        }

        size_t data_pos() const
        {
            return data_pos_;
        }

        size_t count() const
        {
            return count_;
        }

        void increment_count()
        {
            ++count_;
        }

        bool new_line_after() const
        {
            return new_line_after_;
        }

        void new_line_after(bool value) 
        {
            new_line_after_ = value;
        }

        bool is_object() const
        {
            return type_ == container_type::object;
        }

        bool is_array() const
        {
            return type_ == container_type::array;
        }

        bool is_same_line() const
        {
            return line_splits_ == line_split_kind::same_line;
        }

        bool is_new_line() const
        {
            return line_splits_ == line_split_kind::new_line;
        }

        bool is_multi_line() const
        {
            return line_splits_ == line_split_kind::multi_line;
        }

        bool is_indent_once() const
        {
            return count_ == 0 ? indent_before_ : false;
        }

    };

    size_t indent_size_;

    const basic_json_encode_options<CharT>& options_;

    jsoncons::detail::print_double fp_;

    Result result_;

    std::vector<encoding_context> stack_;
    int indent_amount_;
    size_t column_;
    std::basic_string<CharT> colon_str_;
    std::basic_string<CharT> comma_str_;
    std::basic_string<CharT> open_object_brace_str_;
    std::basic_string<CharT> close_object_brace_str_;
    std::basic_string<CharT> open_array_bracket_str_;
    std::basic_string<CharT> close_array_bracket_str_;

    // Noncopyable and nonmoveable
    basic_json_encoder(const basic_json_encoder&) = delete;
    basic_json_encoder& operator=(const basic_json_encoder&) = delete;
public:
    basic_json_encoder(result_type result)
        : basic_json_encoder(std::move(result), basic_json_options<CharT>::default_options())
    {
    }

    basic_json_encoder(result_type result, 
                       const basic_json_encode_options<CharT>& options)
       : options_(options),
         fp_(floating_point_options(options.floating_point_format(), 
                                    options.precision(),
                                    0)),
         result_(std::move(result)), 
         indent_amount_(0), 
         column_(0)
    {
        switch (options.spaces_around_colon())
        {
            case spaces_option::space_after:
                colon_str_ = std::basic_string<CharT>({':',' '});
                break;
            case spaces_option::space_before:
                colon_str_ = std::basic_string<CharT>({' ',':'});
                break;
            case spaces_option::space_before_and_after:
                colon_str_ = std::basic_string<CharT>({' ',':',' '});
                break;
            default:
                colon_str_.push_back(':');
                break;
        }
        switch (options.spaces_around_comma())
        {
            case spaces_option::space_after:
                comma_str_ = std::basic_string<CharT>({',',' '});
                break;
            case spaces_option::space_before:
                comma_str_ = std::basic_string<CharT>({' ',','});
                break;
            case spaces_option::space_before_and_after:
                comma_str_ = std::basic_string<CharT>({' ',',',' '});
                break;
            default:
                comma_str_.push_back(',');
                break;
        }
        if (options.pad_inside_object_braces())
        {
            open_object_brace_str_ = std::basic_string<CharT>({'{', ' '});
            close_object_brace_str_ = std::basic_string<CharT>({' ', '}'});
        }
        else
        {
            open_object_brace_str_.push_back('{');
            close_object_brace_str_.push_back('}');
        }
        if (options.pad_inside_array_brackets())
        {
            open_array_bracket_str_ = std::basic_string<CharT>({'[', ' '});
            close_array_bracket_str_ = std::basic_string<CharT>({' ', ']'});
        }
        else
        {
            open_array_bracket_str_.push_back('[');
            close_array_bracket_str_.push_back(']');
        }
    }

    ~basic_json_encoder()
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
    // Implementing methods
    void do_flush() override
    {
        result_.flush();
    }

    bool do_begin_object(semantic_tag, const ser_context&) override
    {
        if (!stack_.empty() && stack_.back().is_array() && stack_.back().count() > 0)
        {
            result_.append(comma_str_.data(),comma_str_.length());
            column_ += comma_str_.length();
        }

        if (!stack_.empty()) // object or array
        {
            if (stack_.back().is_object())
            {
                switch (options_.object_object_line_splits())
                {
                    case line_split_kind::same_line:
                        if (column_ >= options_.line_length_limit())
                        {
                            break_line();
                        }
                        break;
                    case line_split_kind::new_line:
                        if (column_ >= options_.line_length_limit())
                        {
                            break_line();
                        }
                        break;
                    default: // multi_line
                        break;
                }
                stack_.emplace_back(container_type::object,options_.object_object_line_splits(), false,
                                    column_, column_+open_object_brace_str_.length());
            }
            else // array
            {
                switch (options_.array_object_line_splits())
                {
                    case line_split_kind::same_line:
                        if (column_ >= options_.line_length_limit())
                        {
                            //stack_.back().new_line_after(true);
                            new_line();
                        }
                        break;
                    case line_split_kind::new_line:
                        stack_.back().new_line_after(true);
                        new_line();
                        break;
                    default: // multi_line
                        stack_.back().new_line_after(true);
                        new_line();
                        break;
                }
                stack_.emplace_back(container_type::object,options_.array_object_line_splits(), false,
                                    column_, column_+open_object_brace_str_.length());
            }
        }
        else 
        {
            stack_.emplace_back(container_type::object, line_split_kind::multi_line, false,
                                column_, column_+open_object_brace_str_.length());
        }
        indent();
        
        result_.append(open_object_brace_str_.data(), open_object_brace_str_.length());
        column_ += open_object_brace_str_.length();
        return true;
    }

    bool do_end_object(const ser_context&) override
    {
        JSONCONS_ASSERT(!stack_.empty());
        unindent();
        if (stack_.back().new_line_after())
        {
            new_line();
        }
        stack_.pop_back();
        result_.append(close_object_brace_str_.data(), close_object_brace_str_.length());
        column_ += close_object_brace_str_.length();

        end_value();
        return true;
    }

    bool do_begin_array(semantic_tag, const ser_context&) override
    {
        if (!stack_.empty() && stack_.back().is_array() && stack_.back().count() > 0)
        {
            result_.append(comma_str_.data(),comma_str_.length());
            column_ += comma_str_.length();
        }
        if (!stack_.empty())
        {
            if (stack_.back().is_object())
            {
                switch (options_.object_array_line_splits())
                {
                    case line_split_kind::same_line:
                        stack_.emplace_back(container_type::array,options_.object_array_line_splits(),false,
                                            column_, column_ + open_array_bracket_str_.length());
                        break;
                    case line_split_kind::new_line:
                    {
                        stack_.emplace_back(container_type::array,options_.object_array_line_splits(),true,
                                            column_, column_+open_array_bracket_str_.length());
                        break;
                    }
                    default: // multi_line
                        stack_.emplace_back(container_type::array,options_.object_array_line_splits(),true,
                                            column_, column_+open_array_bracket_str_.length());
                        break;
                }
            }
            else // array
            {
                switch (options_.array_array_line_splits())
                {
                    case line_split_kind::same_line:
                        if (stack_.back().is_multi_line())
                        {
                            stack_.back().new_line_after(true);
                            new_line();
                        }
                        stack_.emplace_back(container_type::array,options_.array_array_line_splits(), false,
                                            column_, column_+open_array_bracket_str_.length());
                        break;
                    case line_split_kind::new_line:
                        stack_.back().new_line_after(true);
                        new_line();
                        stack_.emplace_back(container_type::array,options_.array_array_line_splits(), false,
                                            column_, column_+open_array_bracket_str_.length());
                        break;
                    default: // multi_line
                        stack_.back().new_line_after(true);
                        new_line();
                        stack_.emplace_back(container_type::array,options_.array_array_line_splits(), false,
                                            column_, column_+open_array_bracket_str_.length());
                        //new_line();
                        break;
                }
            }
        }
        else 
        {
            stack_.emplace_back(container_type::array, line_split_kind::multi_line, false,
                                column_, column_+open_array_bracket_str_.length());
        }
        indent();
        result_.append(open_array_bracket_str_.data(), open_array_bracket_str_.length());
        column_ += open_array_bracket_str_.length();
        return true;
    }

    bool do_end_array(const ser_context&) override
    {
        JSONCONS_ASSERT(!stack_.empty());
        unindent();
        if (stack_.back().new_line_after())
        {
            new_line();
        }
        stack_.pop_back();
        result_.append(close_array_bracket_str_.data(), close_array_bracket_str_.length());
        column_ += close_array_bracket_str_.length();
        end_value();
        return true;
    }

    bool do_name(const string_view_type& name, const ser_context&) override
    {
        JSONCONS_ASSERT(!stack_.empty());
        if (stack_.back().count() > 0)
        {
            result_.append(comma_str_.data(),comma_str_.length());
            column_ += comma_str_.length();
        }

        if (stack_.back().is_multi_line())
        {
            stack_.back().new_line_after(true);
            new_line();
        }
        else if (stack_.back().count() > 0 && column_ >= options_.line_length_limit())
        {
            //stack_.back().new_line_after(true);
            new_line(stack_.back().data_pos());
        }

        if (stack_.back().count() == 0)
        {
            stack_.back().set_position(column_);
        }
        result_.push_back('\"');
        size_t length = jsoncons::detail::escape_string(name.data(), name.length(),options_.escape_all_non_ascii(),options_.escape_solidus(),result_);
        result_.push_back('\"');
        result_.append(colon_str_.data(),colon_str_.length());
        column_ += (length+2+colon_str_.length());
        return true;
    }

    bool do_null_value(semantic_tag, const ser_context&) override
    {
        if (!stack_.empty()) 
        {
            if (stack_.back().is_array())
            {
                begin_scalar_value();
            }
            if (!stack_.back().is_multi_line() && column_ >= options_.line_length_limit())
            {
                break_line();
            }
        }

        result_.append(null_k().data(), null_k().size());
        column_ += null_k().size();

        end_value();
        return true;
    }

    bool do_string_value(const string_view_type& sv, semantic_tag tag, const ser_context&) override
    {
        if (!stack_.empty()) 
        {
            if (stack_.back().is_array())
            {
                begin_scalar_value();
            }
            if (!stack_.back().is_multi_line() && column_ >= options_.line_length_limit())
            {
                break_line();
            }
        }

        switch (tag)
        {
            case semantic_tag::bigint:
                write_bigint_value(sv);
                break;
            default:
            {
                result_.push_back('\"');
                size_t length = jsoncons::detail::escape_string(sv.data(), sv.length(),options_.escape_all_non_ascii(),options_.escape_solidus(),result_);
                result_.push_back('\"');
                column_ += (length+2);
                break;
            }
        }

        end_value();
        return true;
    }

    bool do_byte_string_value(const byte_string_view& b, 
                              semantic_tag tag,
                              const ser_context&) override
    {
        if (!stack_.empty()) 
        {
            if (stack_.back().is_array())
            {
                begin_scalar_value();
            }
            if (!stack_.back().is_multi_line() && column_ >= options_.line_length_limit())
            {
                break_line();
            }
        }

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

        byte_string_chars_format format = jsoncons::detail::resolve_byte_string_chars_format(options_.byte_string_format(), 
                                                                                             encoding_hint, 
                                                                                             byte_string_chars_format::base64url);
        switch (format)
        {
            case byte_string_chars_format::base16:
            {
                result_.push_back('\"');
                size_t length = encode_base16(b.begin(),b.end(),result_);
                result_.push_back('\"');
                column_ += (length + 2);
                break;
            }
            case byte_string_chars_format::base64:
            {
                result_.push_back('\"');
                size_t length = encode_base64(b.begin(), b.end(), result_);
                result_.push_back('\"');
                column_ += (length + 2);
                break;
            }
            case byte_string_chars_format::base64url:
            {
                result_.push_back('\"');
                size_t length = encode_base64url(b.begin(),b.end(),result_);
                result_.push_back('\"');
                column_ += (length + 2);
                break;
            }
            default:
            {
                JSONCONS_UNREACHABLE();
            }
        }

        end_value();
        return true;
    }

    bool do_double_value(double value, 
                         semantic_tag,
                         const ser_context& context) override
    {
        if (!stack_.empty()) 
        {
            if (stack_.back().is_array())
            {
                begin_scalar_value();
            }
            if (!stack_.back().is_multi_line() && column_ >= options_.line_length_limit())
            {
                break_line();
            }
        }

        if (!std::isfinite(value))
        {
            if ((std::isnan)(value))
            {
                if (options_.is_nan_to_num())
                {
                    result_.append(options_.nan_to_num().data(), options_.nan_to_num().length());
                    column_ += options_.nan_to_num().length();
                }
                else if (options_.is_nan_to_str())
                {
                    do_string_value(options_.nan_to_str(), semantic_tag::none, context);
                }
                else
                {
                    result_.append(null_k().data(), null_k().size());
                    column_ += null_k().size();
                }
            }
            else if (value == std::numeric_limits<double>::infinity())
            {
                if (options_.is_inf_to_num())
                {
                    result_.append(options_.inf_to_num().data(), options_.inf_to_num().length());
                    column_ += options_.inf_to_num().length();
                }
                else if (options_.is_inf_to_str())
                {
                    do_string_value(options_.inf_to_str(), semantic_tag::none, context);
                }
                else
                {
                    result_.append(null_k().data(), null_k().size());
                    column_ += null_k().size();
                }
            }
            else
            {
                if (options_.is_neginf_to_num())
                {
                    result_.append(options_.neginf_to_num().data(), options_.neginf_to_num().length());
                    column_ += options_.neginf_to_num().length();
                }
                else if (options_.is_neginf_to_str())
                {
                    do_string_value(options_.neginf_to_str(), semantic_tag::none, context);
                }
                else
                {
                    result_.append(null_k().data(), null_k().size());
                    column_ += null_k().size();
                }
            }
        }
        else
        {
            size_t length = fp_(value, result_);
            column_ += length;
        }

        end_value();
        return true;
    }

    bool do_int64_value(int64_t value, 
                        semantic_tag,
                        const ser_context&) override
    {
        if (!stack_.empty()) 
        {
            if (stack_.back().is_array())
            {
                begin_scalar_value();
            }
            if (!stack_.back().is_multi_line() && column_ >= options_.line_length_limit())
            {
                break_line();
            }
        }
        size_t length = jsoncons::detail::print_integer(value, result_);
        column_ += length;
        end_value();
        return true;
    }

    bool do_uint64_value(uint64_t value, 
                         semantic_tag, 
                         const ser_context&) override
    {
        if (!stack_.empty()) 
        {
            if (stack_.back().is_array())
            {
                begin_scalar_value();
            }
            if (!stack_.back().is_multi_line() && column_ >= options_.line_length_limit())
            {
                break_line();
            }
        }
        size_t length = jsoncons::detail::print_uinteger(value, result_);
        column_ += length;
        end_value();
        return true;
    }

    bool do_bool_value(bool value, semantic_tag, const ser_context&) override
    {
        if (!stack_.empty()) 
        {
            if (stack_.back().is_array())
            {
                begin_scalar_value();
            }
            if (!stack_.back().is_multi_line() && column_ >= options_.line_length_limit())
            {
                break_line();
            }
        }

        if (value)
        {
            result_.append(true_k().data(), true_k().size());
            column_ += true_k().size();
        }
        else
        {
            result_.append(false_k().data(), false_k().size());
            column_ += false_k().size();
        }

        end_value();
        return true;
    }

    void begin_scalar_value()
    {
        if (!stack_.empty())
        {
            if (stack_.back().count() > 0)
            {
                result_.append(comma_str_.data(),comma_str_.length());
                column_ += comma_str_.length();
            }
            if (stack_.back().is_multi_line() || stack_.back().is_indent_once())
            {
                stack_.back().new_line_after(true);
                new_line();
            }
        }
    }

    void write_bigint_value(const string_view_type& sv)
    {
        switch (options_.bigint_format())
        {
            case bigint_chars_format::number:
            {
                result_.append(sv.data(),sv.size());
                column_ += sv.size();
                break;
            }
            case bigint_chars_format::base64:
            {
                bignum n(sv.data(), sv.length());
                int signum;
                std::vector<uint8_t> v;
                n.dump(signum, v);

                result_.push_back('\"');
                if (signum == -1)
                {
                    result_.push_back('~');
                    ++column_;
                }
                size_t length = encode_base64(v.begin(), v.end(), result_);
                result_.push_back('\"');
                column_ += (length+2);
                break;
            }
            case bigint_chars_format::base64url:
            {
                bignum n(sv.data(), sv.length());
                int signum;
                std::vector<uint8_t> v;
                n.dump(signum, v);

                result_.push_back('\"');
                if (signum == -1)
                {
                    result_.push_back('~');
                    ++column_;
                }
                size_t length = encode_base64url(v.begin(), v.end(), result_);
                result_.push_back('\"');
                column_ += (length+2);
                break;
            }
            default:
            {
                result_.push_back('\"');
                result_.append(sv.data(),sv.size());
                result_.push_back('\"');
                column_ += (sv.size() + 2);
                break;
            }
        }
    }

    void end_value()
    {
        if (!stack_.empty())
        {
            stack_.back().increment_count();
        }
    }

    void indent()
    {
        indent_amount_ += static_cast<int>(options_.indent_size());
    }

    void unindent()
    {
        indent_amount_ -= static_cast<int>(options_.indent_size());
    }

    void new_line()
    {
        result_.append(options_.new_line_chars().data(),options_.new_line_chars().length());
        for (int i = 0; i < indent_amount_; ++i)
        {
            result_.push_back(' ');
        }
        column_ = indent_amount_;
    }

    void new_line(size_t len)
    {
        result_.append(options_.new_line_chars().data(),options_.new_line_chars().length());
        for (size_t i = 0; i < len; ++i)
        {
            result_.push_back(' ');
        }
        column_ = len;
    }

    void break_line()
    {
        stack_.back().new_line_after(true);
        new_line();
    }
};

template<class CharT,class Result=jsoncons::stream_result<CharT>>
class basic_json_compressed_encoder final : public basic_json_content_handler<CharT>
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
    typedef typename basic_json_options<CharT>::string_type string_type;

private:
    enum class container_type {object, array};

    class encoding_context
    {
        container_type type_;
        size_t count_;
    public:
        encoding_context(container_type type)
           : type_(type), count_(0)
        {
        }

        size_t count() const
        {
            return count_;
        }

        void increment_count()
        {
            ++count_;
        }

        bool is_array() const
        {
            return type_ == container_type::array;
        }
    };

    const basic_json_encode_options<CharT>& options_;

    std::vector<encoding_context> stack_;
    jsoncons::detail::print_double fp_;
    Result result_;

    // Noncopyable and nonmoveable
    basic_json_compressed_encoder(const basic_json_compressed_encoder&) = delete;
    basic_json_compressed_encoder& operator=(const basic_json_compressed_encoder&) = delete;
public:
    basic_json_compressed_encoder(result_type result)
        : basic_json_compressed_encoder(std::move(result), basic_json_options<CharT>::default_options())
    {
    }

    basic_json_compressed_encoder(result_type result, 
                                     const basic_json_encode_options<CharT>& options)
       : options_(options),
         fp_(floating_point_options(options.floating_point_format(), 
                                    options.precision(),
                                    0)),
         result_(std::move(result))
    {
    }

    ~basic_json_compressed_encoder()
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
    // Implementing methods
    void do_flush() override
    {
        result_.flush();
    }

    bool do_begin_object(semantic_tag, const ser_context&) override
    {
        if (!stack_.empty() && stack_.back().is_array() && stack_.back().count() > 0)
        {
            result_.push_back(',');
        }

        stack_.emplace_back(container_type::object);
        result_.push_back('{');
        return true;
    }

    bool do_end_object(const ser_context&) override
    {
        JSONCONS_ASSERT(!stack_.empty());
        stack_.pop_back();
        result_.push_back('}');

        if (!stack_.empty())
        {
            stack_.back().increment_count();
        }
        return true;
    }


    bool do_begin_array(semantic_tag, const ser_context&) override
    {
        if (!stack_.empty() && stack_.back().is_array() && stack_.back().count() > 0)
        {
            result_.push_back(',');
        }
        stack_.emplace_back(container_type::array);
        result_.push_back('[');
        return true;
    }

    bool do_end_array(const ser_context&) override
    {
        JSONCONS_ASSERT(!stack_.empty());
        stack_.pop_back();
        result_.push_back(']');
        if (!stack_.empty())
        {
            stack_.back().increment_count();
        }
        return true;
    }

    bool do_name(const string_view_type& name, const ser_context&) override
    {
        if (!stack_.empty() && stack_.back().count() > 0)
        {
            result_.push_back(',');
        }

        result_.push_back('\"');
        jsoncons::detail::escape_string(name.data(), name.length(),options_.escape_all_non_ascii(),options_.escape_solidus(),result_);
        result_.push_back('\"');
        result_.push_back(':');
        return true;
    }

    bool do_null_value(semantic_tag, const ser_context&) override
    {
        if (!stack_.empty() && stack_.back().is_array() && stack_.back().count() > 0)
        {
            result_.push_back(',');
        }

        result_.append(null_k().data(), null_k().size());

        if (!stack_.empty())
        {
            stack_.back().increment_count();
        }
        return true;
    }

    void write_bigint_value(const string_view_type& sv)
    {
        switch (options_.bigint_format())
        {
            case bigint_chars_format::number:
            {
                result_.append(sv.data(),sv.size());
                break;
            }
            case bigint_chars_format::base64:
            {
                bignum n(sv.data(), sv.length());
                int signum;
                std::vector<uint8_t> v;
                n.dump(signum, v);

                result_.push_back('\"');
                if (signum == -1)
                {
                    result_.push_back('~');
                }
                encode_base64(v.begin(), v.end(), result_);
                result_.push_back('\"');
                break;
            }
            case bigint_chars_format::base64url:
            {
                bignum n(sv.data(), sv.length());
                int signum;
                std::vector<uint8_t> v;
                n.dump(signum, v);

                result_.push_back('\"');
                if (signum == -1)
                {
                    result_.push_back('~');
                }
                encode_base64url(v.begin(), v.end(), result_);
                result_.push_back('\"');
                break;
            }
            default:
            {
                result_.push_back('\"');
                result_.append(sv.data(),sv.size());
                result_.push_back('\"');
                break;
            }
        }
    }

    bool do_string_value(const string_view_type& sv, semantic_tag tag, const ser_context&) override
    {
        if (!stack_.empty() && stack_.back().is_array() && stack_.back().count() > 0)
        {
            result_.push_back(',');
        }

        switch (tag)
        {
            case semantic_tag::bigint:
                write_bigint_value(sv);
                break;
            default:
            {
                result_.push_back('\"');
                jsoncons::detail::escape_string(sv.data(), sv.length(),options_.escape_all_non_ascii(),options_.escape_solidus(),result_);
                result_.push_back('\"');
                break;
            }
        }

        if (!stack_.empty())
        {
            stack_.back().increment_count();
        }
        return true;
    }

    bool do_byte_string_value(const byte_string_view& b, 
                              semantic_tag tag,
                              const ser_context&) override
    {
        if (!stack_.empty() && stack_.back().is_array() && stack_.back().count() > 0)
        {
            result_.push_back(',');
        }

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

        byte_string_chars_format format = jsoncons::detail::resolve_byte_string_chars_format(options_.byte_string_format(), 
                                                                                   encoding_hint, 
                                                                                   byte_string_chars_format::base64url);
        switch (format)
        {
            case byte_string_chars_format::base16:
            {
                result_.push_back('\"');
                encode_base16(b.begin(),b.end(),result_);
                result_.push_back('\"');
                break;
            }
            case byte_string_chars_format::base64:
            {
                result_.push_back('\"');
                encode_base64(b.begin(), b.end(), result_);
                result_.push_back('\"');
                break;
            }
            case byte_string_chars_format::base64url:
            {
                result_.push_back('\"');
                encode_base64url(b.begin(),b.end(),result_);
                result_.push_back('\"');
                break;
            }
            default:
            {
                JSONCONS_UNREACHABLE();
            }
        }

        if (!stack_.empty())
        {
            stack_.back().increment_count();
        }
        return true;
    }

    bool do_double_value(double value, 
                         semantic_tag,
                         const ser_context& context) override
    {
        if (!stack_.empty() && stack_.back().is_array() && stack_.back().count() > 0)
        {
            result_.push_back(',');
        }

        if (JSONCONS_UNLIKELY(!std::isfinite(value)))
        {
            if ((std::isnan)(value))
            {
                if (options_.is_nan_to_num())
                {
                    result_.append(options_.nan_to_num().data(), options_.nan_to_num().length());
                }
                else if (options_.is_nan_to_str())
                {
                    do_string_value(options_.nan_to_str(), semantic_tag::none, context);
                }
                else
                {
                    result_.append(null_k().data(), null_k().size());
                }
            }
            else if (value == std::numeric_limits<double>::infinity())
            {
                if (options_.is_inf_to_num())
                {
                    result_.append(options_.inf_to_num().data(), options_.inf_to_num().length());
                }
                else if (options_.is_inf_to_str())
                {
                    do_string_value(options_.inf_to_str(), semantic_tag::none, context);
                }
                else
                {
                    result_.append(null_k().data(), null_k().size());
                }
            }
            else 
            {
                if (options_.is_neginf_to_num())
                {
                    result_.append(options_.neginf_to_num().data(), options_.neginf_to_num().length());
                }
                else if (options_.is_neginf_to_str())
                {
                    do_string_value(options_.neginf_to_str(), semantic_tag::none, context);
                }
                else
                {
                    result_.append(null_k().data(), null_k().size());
                }
            }
        }
        else
        {
            fp_(value, result_);
        }

        if (!stack_.empty())
        {
            stack_.back().increment_count();
        }
        return true;
    }

    bool do_int64_value(int64_t value, 
                        semantic_tag,
                        const ser_context&) override
    {
        if (!stack_.empty() && stack_.back().is_array() && stack_.back().count() > 0)
        {
            result_.push_back(',');
        }
        jsoncons::detail::print_integer(value, result_);
        if (!stack_.empty())
        {
            stack_.back().increment_count();
        }
        return true;
    }

    bool do_uint64_value(uint64_t value, 
                         semantic_tag, 
                         const ser_context&) override
    {
        if (!stack_.empty() && stack_.back().is_array() && stack_.back().count() > 0)
        {
            result_.push_back(',');
        }
        jsoncons::detail::print_uinteger(value, result_);
        if (!stack_.empty())
        {
            stack_.back().increment_count();
        }
        return true;
    }

    bool do_bool_value(bool value, semantic_tag, const ser_context&) override
    {
        if (!stack_.empty() && stack_.back().is_array() && stack_.back().count() > 0)
        {
            result_.push_back(',');
        }

        if (value)
        {
            result_.append(true_k().data(), true_k().size());
        }
        else
        {
            result_.append(false_k().data(), false_k().size());
        }

        if (!stack_.empty())
        {
            stack_.back().increment_count();
        }
        return true;
    }
};

typedef basic_json_encoder<char,jsoncons::stream_result<char>> json_encoder;
typedef basic_json_encoder<wchar_t,jsoncons::stream_result<wchar_t>> wjson_encoder;

typedef basic_json_compressed_encoder<char,jsoncons::stream_result<char>> json_compressed_encoder;
typedef basic_json_compressed_encoder<wchar_t,jsoncons::stream_result<wchar_t>> wjson_compressed_encoder;

typedef basic_json_encoder<char,jsoncons::string_result<std::string>> json_string_encoder;
typedef basic_json_encoder<wchar_t,jsoncons::string_result<std::wstring>> wjson_string_encoder;

typedef basic_json_compressed_encoder<char,jsoncons::string_result<std::string>> json_compressed_string_encoder;
typedef basic_json_compressed_encoder<wchar_t,jsoncons::string_result<std::wstring>> wjson_compressed_string_encoder;

#if !defined(JSONCONS_NO_DEPRECATED)
template<class CharT,class Result=jsoncons::stream_result<CharT>>
using basic_json_serializer = basic_json_encoder<CharT,Result>; 

template<class CharT,class Result=jsoncons::stream_result<CharT>>
using basic_json_compressed_serializer = basic_json_compressed_encoder<CharT,Result>; 

typedef basic_json_serializer<char,jsoncons::stream_result<char>> json_serializer;
typedef basic_json_serializer<wchar_t,jsoncons::stream_result<wchar_t>> wjson_encoder;

typedef basic_json_compressed_serializer<char,jsoncons::stream_result<char>> json_compressed_serializer;
typedef basic_json_compressed_serializer<wchar_t,jsoncons::stream_result<wchar_t>> wjson_compressed_serializer;

typedef basic_json_serializer<char,jsoncons::string_result<std::string>> json_string_serializer;
typedef basic_json_serializer<wchar_t,jsoncons::string_result<std::wstring>> wjson_string_serializer;

typedef basic_json_compressed_serializer<char,jsoncons::string_result<std::string>> json_compressed_string_serializer;
typedef basic_json_compressed_serializer<wchar_t,jsoncons::string_result<std::wstring>> wjson_compressed_string_serializer;
#endif

}
#endif
