// Copyright 2013 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_JSON_OPTIONS_HPP
#define JSONCONS_JSON_OPTIONS_HPP

#include <string>
#include <limits> // std::numeric_limits
#include <cwchar>
#include <jsoncons/json_exception.hpp>
#include <jsoncons/detail/type_traits.hpp>

namespace jsoncons {

#if !defined(JSONCONS_NO_TO_CHARS)
using chars_format = std::chars_format;
#else
enum class chars_format : uint8_t {fixed=1,scientific=2,hex=4,general=fixed|scientific};
#endif

// floating_point_options

class floating_point_options
{
    chars_format format_;
    int precision_;
    uint8_t decimal_places_;
public:
    floating_point_options()
        : format_(chars_format::general), precision_(0), decimal_places_(0)
    {
    }

    floating_point_options(chars_format format, int precision, uint8_t decimal_places = 0)
        : format_(format), precision_(precision), decimal_places_(decimal_places)
    {
    }

    explicit floating_point_options(chars_format format)
        : format_(format), precision_(0), decimal_places_(0)
    {
    }

    floating_point_options(const floating_point_options&) = default;
    floating_point_options(floating_point_options&&) = default;
    floating_point_options& operator=(const floating_point_options& e) = default;
    floating_point_options& operator=(floating_point_options&& e) = default;

    int precision() const
    {
        return precision_;
    }

    uint8_t decimal_places() const
    {
        return decimal_places_;
    }

    chars_format format() const
    {
        return format_;
    }
};

enum class indenting : uint8_t {no_indent = 0, indent = 1};

enum class line_split_kind  : uint8_t {same_line,new_line,multi_line};

enum class bigint_chars_format : uint8_t {number, base10, base64, base64url
#if !defined(JSONCONS_NO_DEPRECATED)
,integer = number
#endif
};

#if !defined(JSONCONS_NO_DEPRECATED)
typedef bigint_chars_format bignum_chars_format;
typedef bigint_chars_format big_integer_chars_format;
#endif

enum class byte_string_chars_format : uint8_t {none=0,base16,base64,base64url};

enum class spaces_option{no_spaces=0,space_after,space_before,space_before_and_after};

template <class CharT>
class basic_json_decode_options
{
public:
    typedef std::basic_string<CharT> string_type;

    virtual ~basic_json_decode_options() = default;

    virtual size_t max_nesting_depth() const = 0;

    virtual bool is_str_to_nan() const = 0;

    virtual std::basic_string<CharT> nan_to_str() const = 0;

    virtual bool is_str_to_inf() const = 0;

    virtual std::basic_string<CharT> inf_to_str() const = 0;

    virtual bool is_str_to_neginf() const = 0;

    virtual std::basic_string<CharT> neginf_to_str() const = 0;

    virtual bool lossless_number() const = 0;
};

template <class CharT>
class basic_json_encode_options
{
public:
    typedef std::basic_string<CharT> string_type;

    virtual ~basic_json_encode_options() = default;

    virtual size_t max_nesting_depth() const = 0;

    virtual byte_string_chars_format byte_string_format() const = 0; 

    virtual bigint_chars_format bigint_format() const = 0; 

    virtual line_split_kind object_object_line_splits() const = 0; 

    virtual line_split_kind array_object_line_splits() const = 0; 

    virtual line_split_kind object_array_line_splits() const = 0; 

    virtual line_split_kind array_array_line_splits() const = 0; 

    virtual size_t  indent_size() const = 0;  

    virtual size_t line_length_limit() const = 0;  

    virtual chars_format floating_point_format() const = 0; 

    virtual int precision() const = 0; 

    virtual bool escape_all_non_ascii() const = 0; 

    virtual bool escape_solidus() const = 0; 

    virtual spaces_option spaces_around_colon() const = 0;  

    virtual spaces_option spaces_around_comma() const = 0;  

    virtual bool pad_inside_object_braces() const = 0;

    virtual bool pad_inside_array_brackets() const = 0;

    virtual std::basic_string<CharT> new_line_chars() const = 0;

    virtual bool is_nan_to_num() const = 0;

    virtual std::basic_string<CharT> nan_to_num() const = 0;

    virtual bool is_inf_to_num() const = 0;

    virtual std::basic_string<CharT> inf_to_num() const = 0;

    virtual bool is_neginf_to_num() const = 0;

    virtual std::basic_string<CharT> neginf_to_num() const = 0;

    virtual bool is_nan_to_str() const = 0;

    virtual std::basic_string<CharT> nan_to_str() const = 0;

    virtual bool is_inf_to_str() const = 0;

    virtual std::basic_string<CharT> inf_to_str() const = 0;

    virtual bool is_neginf_to_str() const = 0;

    virtual std::basic_string<CharT> neginf_to_str() const = 0;
};

template <class CharT>
class basic_json_options : public virtual basic_json_decode_options<CharT>, 
                           public virtual basic_json_encode_options<CharT>
{
public:
    typedef CharT char_type;
    typedef std::basic_string<CharT> string_type;
private:
    size_t indent_size_;
    chars_format floating_point_format_;
    int precision_;
#if !defined(JSONCONS_NO_DEPRECATED)
    bool can_read_nan_replacement_;
    bool can_read_pos_inf_replacement_;
    bool can_read_neg_inf_replacement_;
    string_type nan_replacement_;
    string_type pos_inf_replacement_;
    string_type neg_inf_replacement_;
#endif
    bool escape_all_non_ascii_;
    bool escape_solidus_;
    byte_string_chars_format byte_string_format_;
    bigint_chars_format bigint_format_;
    line_split_kind object_object_line_splits_;
    line_split_kind object_array_line_splits_;
    line_split_kind array_array_line_splits_;
    line_split_kind array_object_line_splits_;
    size_t line_length_limit_;

    size_t max_nesting_depth_;
    spaces_option spaces_around_colon_;
    spaces_option spaces_around_comma_;
    bool pad_inside_object_braces_;
    bool pad_inside_array_brackets_;
    std::basic_string<CharT> new_line_chars_;

    bool is_nan_to_num_;
    bool is_inf_to_num_;
    bool is_neginf_to_num_;
    bool is_nan_to_str_;
    bool is_inf_to_str_;
    bool is_neginf_to_str_;
    bool is_str_to_nan_;
    bool is_str_to_inf_;
    bool is_str_to_neginf_;

    std::basic_string<CharT> nan_to_num_;
    std::basic_string<CharT> inf_to_num_;
    std::basic_string<CharT> neginf_to_num_;
    std::basic_string<CharT> nan_to_str_;
    std::basic_string<CharT> inf_to_str_;
    std::basic_string<CharT> neginf_to_str_;

    bool lossless_number_;
public:
    static const size_t indent_size_default = 4;
    static const size_t line_length_limit_default = 120;

    static const basic_json_options<CharT>& default_options()
    {
        static basic_json_options<CharT> options{};
        return options;
    }

//  Constructors

    basic_json_options()
        : indent_size_(indent_size_default),
          floating_point_format_(chars_format()),
          precision_(0),
#if !defined(JSONCONS_NO_DEPRECATED)
          can_read_nan_replacement_(false),
          can_read_pos_inf_replacement_(false),
          can_read_neg_inf_replacement_(false),
#endif
          escape_all_non_ascii_(false),
          escape_solidus_(false),
          byte_string_format_(byte_string_chars_format::none),
          bigint_format_(bigint_chars_format::base10),
          object_object_line_splits_(line_split_kind::multi_line),
          object_array_line_splits_(line_split_kind::same_line),
          array_array_line_splits_(line_split_kind::new_line),
          array_object_line_splits_(line_split_kind::multi_line),
          line_length_limit_(line_length_limit_default),
          max_nesting_depth_((std::numeric_limits<size_t>::max)()),
          spaces_around_colon_(spaces_option::space_after),
          spaces_around_comma_(spaces_option::space_after),
          pad_inside_object_braces_(false),
          pad_inside_array_brackets_(false),
          is_nan_to_num_(false),
          is_inf_to_num_(false),
          is_neginf_to_num_(false),
          is_nan_to_str_(false),
          is_inf_to_str_(false),
          is_neginf_to_str_(false),
          is_str_to_nan_(false),
          is_str_to_inf_(false),
          is_str_to_neginf_(false),
          lossless_number_(false)
    {
        new_line_chars_.push_back('\n');
    }

//  Properties
    byte_string_chars_format byte_string_format() const override {return byte_string_format_;}
    basic_json_options<CharT>&  byte_string_format(byte_string_chars_format value) {byte_string_format_ = value; return *this;}

    bigint_chars_format bigint_format() const override {return bigint_format_;}
    basic_json_options<CharT>&  bigint_format(bigint_chars_format value) {bigint_format_ = value; return *this;}

#if !defined(JSONCONS_NO_DEPRECATED)
    basic_json_options<CharT>&  big_integer_format(bigint_chars_format value) {bigint_format_ = value; return *this;}
    bignum_chars_format bignum_format() const {return bigint_format_;}
    basic_json_options<CharT>&  bignum_format(bignum_chars_format value) {bigint_format_ = value; return *this;}
#endif
    line_split_kind object_object_line_splits() const override {return object_object_line_splits_;}
    basic_json_options<CharT>& object_object_line_splits(line_split_kind value) {object_object_line_splits_ = value; return *this;}

    line_split_kind array_object_line_splits() const override {return array_object_line_splits_;}
    basic_json_options<CharT>& array_object_line_splits(line_split_kind value) {array_object_line_splits_ = value; return *this;}

    line_split_kind object_array_line_splits() const override {return object_array_line_splits_;}
    basic_json_options<CharT>& object_array_line_splits(line_split_kind value) {object_array_line_splits_ = value; return *this;}

    line_split_kind array_array_line_splits() const override {return array_array_line_splits_;}
    basic_json_options<CharT>& array_array_line_splits(line_split_kind value) {array_array_line_splits_ = value; return *this;}

    size_t indent_size() const override
    {
        return indent_size_;
    }

    basic_json_options<CharT>& indent_size(size_t value)
    {
        indent_size_ = value;
        return *this;
    }

    spaces_option spaces_around_colon() const override
    {
        return spaces_around_colon_;
    }

    basic_json_options<CharT>& spaces_around_colon(spaces_option value)
    {
        spaces_around_colon_ = value;
        return *this;
    }

    spaces_option spaces_around_comma() const override
    {
        return spaces_around_comma_;
    }

    basic_json_options<CharT>& spaces_around_comma(spaces_option value)
    {
        spaces_around_comma_ = value;
        return *this;
    }

    bool pad_inside_object_braces() const override
    {
        return pad_inside_object_braces_;
    }

    basic_json_options<CharT>& pad_inside_object_braces(bool value)
    {
        pad_inside_object_braces_ = value;
        return *this;
    }

    bool pad_inside_array_brackets() const override
    {
        return pad_inside_array_brackets_;
    }

    basic_json_options<CharT>& pad_inside_array_brackets(bool value)
    {
        pad_inside_array_brackets_ = value;
        return *this;
    }

    std::basic_string<CharT> new_line_chars() const override
    {
        return new_line_chars_;
    }

    basic_json_options<CharT>& new_line_chars(const std::basic_string<CharT>& value)
    {
        new_line_chars_ = value;
        return *this;
    }

    bool is_nan_to_num() const override
    {
        return is_nan_to_num_;
    }

    bool is_inf_to_num() const override
    {
        return is_inf_to_num_;
    }

    bool is_neginf_to_num() const override
    {
        return is_neginf_to_num_ || is_inf_to_num_;
    }

    bool is_nan_to_str() const override
    {
        return is_nan_to_str_;
    }

    bool is_str_to_nan() const override
    {
        return is_str_to_nan_;
    }

    bool is_inf_to_str() const override
    {
        return is_inf_to_str_;
    }

    bool is_str_to_inf() const override
    {
        return is_str_to_inf_;
    }

    bool is_neginf_to_str() const override
    {
        return is_neginf_to_str_ || is_inf_to_str_;
    }

    bool is_str_to_neginf() const override
    {
        return is_str_to_neginf_ || is_str_to_inf_;
    }

    std::basic_string<CharT> nan_to_num() const override
    {
        if (is_nan_to_num_)
        {
            return nan_to_num_;
        }
#if !defined(JSONCONS_NO_DEPRECATED)
        else if (!can_read_nan_replacement_) // not string
        {
            return nan_replacement_;
        }
#endif
        else
        {
            return nan_to_num_; // empty string
        }
    }

    basic_json_options<CharT>& nan_to_num(const std::basic_string<CharT>& value) 
    {
        is_nan_to_num_ = true;
        nan_to_str_.clear();
        nan_to_num_ = value;
        return *this;
    }

    std::basic_string<CharT> inf_to_num() const override
    {
        if (is_inf_to_num_)
        {
            return inf_to_num_;
        }
#if !defined(JSONCONS_NO_DEPRECATED)
        else if (!can_read_pos_inf_replacement_) // not string
        {
            return pos_inf_replacement_;
        }
#endif
        else
        {
            return inf_to_num_; // empty string
        }
    }

    basic_json_options<CharT>& inf_to_num(const std::basic_string<CharT>& value) 
    {
        is_inf_to_num_ = true;
        inf_to_str_.clear();
        inf_to_num_ = value;
        return *this;
    }

    std::basic_string<CharT> neginf_to_num() const override
    {
        if (is_neginf_to_num_)
        {
            return neginf_to_num_;
        }
        else if (is_inf_to_num_)
        {
            std::basic_string<CharT> s;
            s.push_back('-');
            s.append(inf_to_num_);
            return s;
        }
#if !defined(JSONCONS_NO_DEPRECATED)
        else if (!can_read_neg_inf_replacement_) // not string
        {
            return neg_inf_replacement_;
        }
#endif
        else
        {
            return neginf_to_num_; // empty string
        }
    }

    basic_json_options<CharT>& neginf_to_num(const std::basic_string<CharT>& value) 
    {
        is_neginf_to_num_ = true;
        neginf_to_str_.clear();
        neginf_to_num_ = value;
        return *this;
    }

    std::basic_string<CharT> nan_to_str() const override
    {
        if (is_nan_to_str_)
        {
            return nan_to_str_;
        }
#if !defined(JSONCONS_NO_DEPRECATED)
        else if (can_read_nan_replacement_ && nan_replacement_.size() >= 2) // string
        {
            return nan_replacement_.substr(1,nan_replacement_.size()-2); // Remove quotes
        }
#endif
        else
        {
            return nan_to_str_; // empty string
        }
    }

    basic_json_options<CharT>& nan_to_str(const std::basic_string<CharT>& value, bool is_str_to_nan = true) 
    {
        is_nan_to_str_ = true;
        is_str_to_nan_ = is_str_to_nan;
        nan_to_num_.clear();
        nan_to_str_ = value;
        return *this;
    }

    std::basic_string<CharT> inf_to_str() const override
    {
        if (is_inf_to_str_)
        {
            return inf_to_str_;
        }
#if !defined(JSONCONS_NO_DEPRECATED)
        else if (can_read_pos_inf_replacement_ && pos_inf_replacement_.size() >= 2) // string
        {
            return pos_inf_replacement_.substr(1,pos_inf_replacement_.size()-2); // Strip quotes
        }
#endif
        else
        {
            return inf_to_str_; // empty string
        }
    }

    basic_json_options<CharT>& inf_to_str(const std::basic_string<CharT>& value, bool is_inf_to_str = true) 
    {
        is_inf_to_str_ = true;
        is_inf_to_str_ = is_inf_to_str;
        inf_to_num_.clear();
        inf_to_str_ = value;
        return *this;
    }

    std::basic_string<CharT> neginf_to_str() const override
    {
        if (is_neginf_to_str_)
        {
            return neginf_to_str_;
        }
        else if (is_inf_to_str_)
        {
            std::basic_string<CharT> s;
            s.push_back('-');
            s.append(inf_to_str_);
            return s;
        }
#if !defined(JSONCONS_NO_DEPRECATED)
        else if (can_read_neg_inf_replacement_ && neg_inf_replacement_.size() >= 2) // string
        {
            return neg_inf_replacement_.substr(1,neg_inf_replacement_.size()-2); // Strip quotes
        }
#endif
        else
        {
            return neginf_to_str_; // empty string
        }
    }

    basic_json_options<CharT>& neginf_to_str(const std::basic_string<CharT>& value, bool is_neginf_to_str = true) 
    {
        is_neginf_to_str_ = true;
        is_neginf_to_str_ = is_neginf_to_str;
        neginf_to_num_.clear();
        neginf_to_str_ = value;
        return *this;
    }

    bool lossless_number() const override
    {
        return lossless_number_;
    }

    basic_json_options<CharT>& lossless_number(bool value) 
    {
        lossless_number_ = value;
        return *this;
    }

    size_t line_length_limit() const override
    {
        return line_length_limit_;
    }

    basic_json_options<CharT>& line_length_limit(size_t value)
    {
        line_length_limit_ = value;
        return *this;
    }

    chars_format floating_point_format() const override
    {
        return floating_point_format_;
    }

    basic_json_options<CharT>& floating_point_format(chars_format value)
    {
        floating_point_format_ = value;
        return *this;
    }

    int precision() const override
    {
        return precision_;
    }

    basic_json_options<CharT>& precision(int value)
    {
        precision_ = value;
        return *this;
    }

    bool escape_all_non_ascii() const override
    {
        return escape_all_non_ascii_;
    }

    basic_json_options<CharT>& escape_all_non_ascii(bool value)
    {
        escape_all_non_ascii_ = value;
        return *this;
    }

    bool escape_solidus() const override
    {
        return escape_solidus_;
    }

    basic_json_options<CharT>& escape_solidus(bool value)
    {
        escape_solidus_ = value;
        return *this;
    }

    size_t max_nesting_depth() const override
    {
        return max_nesting_depth_;
    }

    void max_nesting_depth(size_t value)
    {
        max_nesting_depth_ = value;
    }

#if !defined(JSONCONS_NO_DEPRECATED)

    bool dec_to_str() const 
    {
        return lossless_number_;
    }

    basic_json_options<CharT>& dec_to_str(bool value) 
    {
        lossless_number_ = value;
        return *this;
    }

    size_t indent() const 
    {
        return indent_size();
    }

    basic_json_options<CharT>& indent(size_t value)
    {
        return indent_size(value);
    }

    bool can_read_nan_replacement() const {return can_read_nan_replacement_;}

    bool can_read_pos_inf_replacement() const {return can_read_pos_inf_replacement_;}

    bool can_read_neg_inf_replacement() const {return can_read_neg_inf_replacement_;}

    bool can_write_nan_replacement() const {return !nan_replacement_.empty();}

    bool can_write_pos_inf_replacement() const {return !pos_inf_replacement_.empty();}

    bool can_write_neg_inf_replacement() const {return !neg_inf_replacement_.empty();}

    basic_json_options<CharT>& replace_inf(bool replace)
    {
        can_read_pos_inf_replacement_ = replace;
        can_read_neg_inf_replacement_ = replace;
        return *this;
    }

    basic_json_options<CharT>& replace_pos_inf(bool replace)
    {
        can_read_pos_inf_replacement_ = replace;
        return *this;
    }

    basic_json_options<CharT>& replace_neg_inf(bool replace)
    {
        can_read_neg_inf_replacement_ = replace;
        return *this;
    }

    const string_type& nan_replacement() const
    {
        return nan_replacement_;
    }

    basic_json_options<CharT>& nan_replacement(const string_type& value)
    {
        nan_replacement_ = value;

        can_read_nan_replacement_ = is_string(value);

        return *this;
    }

    const string_type& pos_inf_replacement() const 
    {
        return pos_inf_replacement_;
    }

    basic_json_options<CharT>& pos_inf_replacement(const string_type& value)
    {
        pos_inf_replacement_ = value;
        can_read_pos_inf_replacement_ = is_string(value);
        return *this;
    }

    const string_type& neg_inf_replacement() const 
    {
        return neg_inf_replacement_;
    }

    basic_json_options<CharT>& neg_inf_replacement(const string_type& value)
    {
        neg_inf_replacement_ = value;
        can_read_neg_inf_replacement_ = is_string(value);
        return *this;
    }

    line_split_kind object_object_split_lines() const {return object_object_line_splits_;}
    basic_json_options<CharT>& object_object_split_lines(line_split_kind value) {object_object_line_splits_ = value; return *this;}

    line_split_kind array_object_split_lines() const {return array_object_line_splits_;}
    basic_json_options<CharT>& array_object_split_lines(line_split_kind value) {array_object_line_splits_ = value; return *this;}

    line_split_kind object_array_split_lines() const {return object_array_line_splits_;}
    basic_json_options<CharT>& object_array_split_lines(line_split_kind value) {object_array_line_splits_ = value; return *this;}

    line_split_kind array_array_split_lines() const {return array_array_line_splits_;}
    basic_json_options<CharT>& array_array_split_lines(line_split_kind value) {array_array_line_splits_ = value; return *this;}
#endif
private:
    enum class input_state {initial,begin_quote,character,end_quote,escape,error};
    bool is_string(const string_type& s) const
    {
        input_state state = input_state::initial;
        for (CharT c : s)
        {
            switch (c)
            {
            case '\t': case ' ': case '\n': case'\r':
                break;
            case '\\':
                state = input_state::escape;
                break;
            case '\"':
                switch (state)
                {
                case input_state::initial:
                    state = input_state::begin_quote;
                    break;
                case input_state::begin_quote:
                    state = input_state::end_quote;
                    break;
                case input_state::character:
                    state = input_state::end_quote;
                    break;
                case input_state::end_quote:
                    state = input_state::error;
                    break;
                case input_state::escape:
                    state = input_state::character;
                    break;
                default:
                    state = input_state::character;
                    break;
                }
                break;
            default:
                break;
            }

        }
        return state == input_state::end_quote;
    }
};

typedef basic_json_options<char> json_options;
typedef basic_json_options<wchar_t> wjson_options;

typedef basic_json_decode_options<char> json_decode_options;
typedef basic_json_decode_options<wchar_t> wjson_decode_options;

typedef basic_json_encode_options<char> json_encode_options;
typedef basic_json_encode_options<wchar_t> wjson_encode_options;

#if !defined(JSONCONS_NO_DEPRECATED)
typedef basic_json_options<char> output_format;
typedef basic_json_options<wchar_t> woutput_format;
typedef basic_json_options<char> serialization_options;
typedef basic_json_options<wchar_t> wserialization_options;
typedef basic_json_options<char> json_serializing_options;
typedef basic_json_options<wchar_t> wjson_serializing_options;
#endif

}
#endif
