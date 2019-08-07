// Copyright 2013 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_CSV_CSV_OPTIONS_HPP
#define JSONCONS_CSV_CSV_OPTIONS_HPP

#include <string>
#include <vector>
#include <utility> // std::pair
#include <unordered_map> // std::unordered_map
#include <limits> // std::numeric_limits
#include <cwchar>

namespace jsoncons { namespace csv {

namespace detail {
    JSONCONS_STRING_LITERAL(string,'s','t','r','i','n','g')
    JSONCONS_STRING_LITERAL(integer,'i','n','t','e','g','e','r')
    JSONCONS_STRING_LITERAL(float,'f','l','o','a','t')
    JSONCONS_STRING_LITERAL(boolean,'b','o','o','l','e','a','n')
}

enum class csv_column_type
{
    string_t,integer_t,float_t,boolean_t,repeat_t
};

enum class quote_style_type
{
    all,minimal,none,nonnumeric
};

typedef quote_style_type quote_styles;

enum class mapping_type
{
    n_rows, 
    n_objects, 
    m_columns
};

enum class column_state {sequence,label};

struct csv_type_info
{
    csv_type_info() = default;
    csv_type_info(const csv_type_info&) = default;
    csv_type_info(csv_type_info&&) = default;

    csv_type_info(csv_column_type ctype, size_t lev, size_t repcount = 0)
    {
        col_type = ctype;
        level = lev;
        rep_count = repcount;
    }

    csv_column_type col_type;
    size_t level;
    size_t rep_count;
};

template <class CharT>
class basic_csv_decode_options
{
public:
    typedef std::basic_string<CharT> string_type;

    virtual size_t header_lines() const = 0;

    virtual bool assume_header() const = 0;

    virtual bool ignore_empty_values() const = 0;

    virtual bool ignore_empty_lines() const = 0;

    virtual bool trim_leading() const = 0;

    virtual bool trim_trailing() const = 0;

    virtual bool trim_leading_inside_quotes() const = 0;

    virtual bool trim_trailing_inside_quotes() const = 0;

    virtual bool trim() const = 0;

    virtual bool trim_inside_quotes() const = 0;

    virtual bool unquoted_empty_value_is_null() const = 0;

    virtual std::vector<string_type> column_names() const = 0;

    virtual std::vector<csv_type_info> column_types() const = 0;

    virtual std::vector<string_type> column_defaults() const = 0;

    virtual CharT field_delimiter() const = 0;

    virtual const std::pair<CharT,bool>& subfield_delimiter() const = 0;

    virtual string_type line_delimiter() const = 0;

    virtual CharT quote_char() const = 0;

    virtual bool infer_types() const = 0;

    virtual bool lossless_number() const = 0;

    virtual CharT quote_escape_char() const = 0;

    virtual CharT comment_starter() const = 0;

    virtual mapping_type mapping() const = 0;

    virtual unsigned long max_lines() const = 0;
};

template <class CharT>
class basic_csv_encode_options
{
public:
    typedef std::basic_string<CharT> string_type;

    virtual chars_format floating_point_format() const = 0;

    virtual int precision() const = 0;

    virtual std::vector<string_type> column_names() const = 0;

    virtual CharT field_delimiter() const = 0;

    virtual const std::pair<CharT,bool>& subfield_delimiter() const = 0;

    virtual string_type line_delimiter() const = 0;

    virtual CharT quote_char() const = 0;

    virtual CharT quote_escape_char() const = 0;

    virtual quote_style_type quote_style() const = 0;
};

template <class CharT>
class basic_csv_options : public virtual basic_csv_decode_options<CharT>,
                          public virtual basic_csv_encode_options<CharT>
{
    typedef CharT char_type;
    typedef std::basic_string<CharT> string_type;

    chars_format floating_point_format_;
    int precision_;
    bool assume_header_;
    bool ignore_empty_values_;
    bool ignore_empty_lines_;
    bool trim_leading_;
    bool trim_trailing_;
    bool trim_leading_inside_quotes_;
    bool trim_trailing_inside_quotes_;
    bool unquoted_empty_value_is_null_;
    CharT field_delimiter_;
    std::pair<CharT,bool> subfield_delimiter_;
    CharT quote_char_;
    CharT quote_escape_char_;
    CharT comment_starter_;
    quote_style_type quote_style_;
    std::pair<mapping_type,bool> mapping_;
    unsigned long max_lines_;
    size_t header_lines_;
    string_type line_delimiter_;
    bool infer_types_;
    bool lossless_number_;

    std::vector<string_type> column_names_;
    std::vector<csv_type_info> column_types_;
    std::vector<string_type> column_defaults_;
public:
    static const size_t default_indent = 4;

    static const basic_csv_options<CharT>& default_options()
    {
        static basic_csv_options<CharT> options{};
        return options;
    }

//  Constructors

    basic_csv_options() :
        floating_point_format_(chars_format()),
        precision_(0),
        assume_header_(false),
        ignore_empty_values_(false),
        ignore_empty_lines_(true),
        trim_leading_(false),
        trim_trailing_(false),
        trim_leading_inside_quotes_(false),
        trim_trailing_inside_quotes_(false),
        unquoted_empty_value_is_null_(false),
        field_delimiter_(','),
        subfield_delimiter_(std::make_pair(',',false)),
        quote_char_('\"'),
        quote_escape_char_('\"'),
        comment_starter_('\0'),
        quote_style_(quote_style_type::minimal),
        mapping_({mapping_type::n_rows,false}),
        max_lines_((std::numeric_limits<unsigned long>::max)()),
        header_lines_(0),
        infer_types_(true),
        lossless_number_(false)
    {
        line_delimiter_.push_back('\n');
    }

//  Properties

    chars_format floating_point_format() const override
    {
        return floating_point_format_;
    }

    basic_csv_options<CharT>& floating_point_format(chars_format value)
    {
        floating_point_format_ = value;
        return *this;
    }

    int precision() const override
    {
        return precision_;
    }

    basic_csv_options<CharT>& precision(int value)
    {
        precision_ = value;
        return *this;
    }

    size_t header_lines() const override
    {
        return (assume_header_ && header_lines_ <= 1) ? 1 : header_lines_;
    }

    basic_csv_options& header_lines(size_t value)
    {
        header_lines_ = value;
        return *this;
    }

    bool assume_header() const override
    {
        return assume_header_;
    }

    basic_csv_options& assume_header(bool value)
    {
        assume_header_ = value;
        return *this;
    }

    bool ignore_empty_values() const override
    {
        return ignore_empty_values_;
    }

    basic_csv_options& ignore_empty_values(bool value)
    {
        ignore_empty_values_ = value;
        return *this;
    }

    bool ignore_empty_lines() const override
    {
        return ignore_empty_lines_;
    }

    basic_csv_options& ignore_empty_lines(bool value)
    {
        ignore_empty_lines_ = value;
        return *this;
    }

    bool trim_leading() const override
    {
        return trim_leading_;
    }

    basic_csv_options& trim_leading(bool value)
    {
        trim_leading_ = value;
        return *this;
    }

    bool trim_trailing() const override
    {
        return trim_trailing_;
    }

    basic_csv_options& trim_trailing(bool value)
    {
        trim_trailing_ = value;
        return *this;
    }

    bool trim_leading_inside_quotes() const override
    {
        return trim_leading_inside_quotes_;
    }

    basic_csv_options& trim_leading_inside_quotes(bool value)
    {
        trim_leading_inside_quotes_ = value;
        return *this;
    }

    bool trim_trailing_inside_quotes() const override
    {
        return trim_trailing_inside_quotes_;
    }

    basic_csv_options& trim_trailing_inside_quotes(bool value)
    {
        trim_trailing_inside_quotes_ = value;
        return *this;
    }

    bool trim() const override
    {
        return trim_leading_ && trim_trailing_;
    }

    basic_csv_options& trim(bool value)
    {
        trim_leading_ = value;
        trim_trailing_ = value;
        return *this;
    }

    bool trim_inside_quotes() const override
    {
        return trim_leading_inside_quotes_ && trim_trailing_inside_quotes_;
    }

    basic_csv_options& trim_inside_quotes(bool value)
    {
        trim_leading_inside_quotes_ = value;
        trim_trailing_inside_quotes_ = value;
        return *this;
    }

    bool unquoted_empty_value_is_null() const override
    {
        return unquoted_empty_value_is_null_;
    }

    basic_csv_options& unquoted_empty_value_is_null(bool value)
    {
        unquoted_empty_value_is_null_ = value;
        return *this;
    }

    std::vector<string_type> column_names() const override
    {
        return column_names_;
    }

#if !defined(JSONCONS_NO_DEPRECATED)
    basic_csv_options& column_names(const std::vector<string_type>& value)
    {
        column_names_ = value;
        return *this;
    }

    basic_csv_options& column_defaults(const std::vector<string_type>& value)
    {
        column_defaults_ = value;
        return *this;
    }

    basic_csv_options& column_types(const std::vector<string_type>& value)
    {
        if (value.size() > 0)
        {
            column_types_.reserve(value.size());
            for (size_t i = 0; i < value.size(); ++i)
            {
                if (value[i] == jsoncons::csv::detail::string_literal<CharT>()())
                {
                    column_types_.emplace_back(csv_column_type::string_t,0);
                }
                else if (value[i] == jsoncons::csv::detail::integer_literal<CharT>()())
                {
                    column_types_.emplace_back(csv_column_type::integer_t,0);
                }
                else if (value[i] == jsoncons::csv::detail::float_literal<CharT>()())
                {
                    column_types_.emplace_back(csv_column_type::float_t,0);
                }
                else if (value[i] == jsoncons::csv::detail::boolean_literal<CharT>()())
                {
                    column_types_.emplace_back(csv_column_type::boolean_t,0);
                }
            }
        }
        return *this;
    }
#endif
    basic_csv_options& column_names(const string_type& names)
    {
        column_names_ = parse_column_names(names);
        return *this;
    }

    std::vector<csv_type_info> column_types() const override
    {
        return column_types_;
    }

    basic_csv_options& column_types(const string_type& types)
    {
        column_types_ = parse_column_types(types);
        return *this;
    }

    std::vector<string_type> column_defaults() const override
    {
        return column_defaults_;
    }

    basic_csv_options& column_defaults(const string_type& defaults)
    {
        column_defaults_ = parse_column_names(defaults);
        return *this;
    }

    CharT field_delimiter() const override
    {
        return field_delimiter_;
    }

    const std::pair<CharT,bool>& subfield_delimiter() const override
    {
        return subfield_delimiter_;
    }

    basic_csv_options& field_delimiter(CharT value)
    {
        field_delimiter_ = value;
        return *this;
    }

    basic_csv_options& subfield_delimiter(CharT value)
    {
        subfield_delimiter_ = std::make_pair(value,true);
        return *this;
    }

    string_type line_delimiter() const override
    {
        return line_delimiter_;
    }

    basic_csv_options& line_delimiter(string_type value)
    {
        line_delimiter_ = value;
        return *this;
    }

    CharT quote_char() const override
    {
        return quote_char_;
    }

    basic_csv_options& quote_char(CharT value)
    {
        quote_char_ = value;
        return *this;
    }

    bool infer_types() const override
    {
        return infer_types_;
    }

    basic_csv_options& infer_types(bool value)
    {
        infer_types_ = value;
        return *this;
    }

    bool lossless_number() const override
    {
        return lossless_number_;
    }

    basic_csv_options& lossless_number(bool value) 
    {
        lossless_number_ = value;
        return *this;
    }

    CharT quote_escape_char() const override
    {
        return quote_escape_char_;
    }

    basic_csv_options& quote_escape_char(CharT value)
    {
        quote_escape_char_ = value;
        return *this;
    }

    CharT comment_starter() const override
    {
        return comment_starter_;
    }

    basic_csv_options& comment_starter(CharT value)
    {
        comment_starter_ = value;
        return *this;
    }

    quote_style_type quote_style() const override
    {
        return quote_style_;
    }

    mapping_type mapping() const override
    {
        return mapping_.second ? (mapping_.first) : (assume_header() || column_names_.size() > 0 ? mapping_type::n_objects : mapping_type::n_rows);
    }

    basic_csv_options& quote_style(quote_style_type value)
    {
        quote_style_ = value;
        return *this;
    }

    basic_csv_options& mapping(mapping_type value)
    {
        mapping_ = {value,true};
        return *this;
    }

    unsigned long max_lines() const override
    {
        return max_lines_;
    }

    basic_csv_options& max_lines(unsigned long value)
    {
        max_lines_ = value;
        return *this;
    }

    static std::vector<string_type> parse_column_names(const string_type& names)
    {
        std::vector<string_type> column_names;

        column_state state = column_state::sequence;
        string_type buffer;

        auto p = names.begin();
        while (p != names.end())
        {
            switch (state)
            {
                case column_state::sequence:
                {
                    switch (*p)
                    {
                        case ' ': case '\t':case '\r': case '\n':
                            ++p;
                            break;
                        default:
                            buffer.clear();
                            state = column_state::label;
                            break;
                    }
                    break;
                }
                case column_state::label:
                {
                    switch (*p)
                    {
                    case ',':
                        column_names.push_back(buffer);
                        buffer.clear();
                        ++p;
                        state = column_state::sequence;
                        break;
                    default:
                        buffer.push_back(*p);
                        ++p;
                        break;
                    }
                    break;
                }
            }
        }
        if (state == column_state::label)
        {
            column_names.push_back(buffer);
            buffer.clear();
        }
        return column_names;
    }

    static std::vector<csv_type_info> parse_column_types(const string_type& types)
    {
        const std::unordered_map<string_type,csv_column_type, std::hash<string_type>,std::equal_to<string_type>> type_dictionary =
        {

            {detail::string_literal<char_type>(),csv_column_type::string_t},
            {detail::integer_literal<char_type>(),csv_column_type::integer_t},
            {detail::float_literal<char_type>(),csv_column_type::float_t},
            {detail::boolean_literal<char_type>(),csv_column_type::boolean_t}
        };

        std::vector<csv_type_info> column_types;

        column_state state = column_state::sequence;
        int depth = 0;
        string_type buffer;

        auto p = types.begin();
        while (p != types.end())
        {
            switch (state)
            {
                case column_state::sequence:
                {
                    switch (*p)
                    {
                    case ' ': case '\t':case '\r': case '\n':
                        ++p;
                        break;
                    case '[':
                        ++depth;
                        ++p;
                        break;
                    case ']':
                        JSONCONS_ASSERT(depth > 0);
                        --depth;
                        ++p;
                        break;
                    case '*':
                        {
                            JSONCONS_ASSERT(column_types.size() != 0);
                            size_t offset = 0;
                            size_t level = column_types.size() > 0 ? column_types.back().level: 0;
                            if (level > 0)
                            {
                                for (auto it = column_types.rbegin();
                                     it != column_types.rend() && level == it->level;
                                     ++it)
                                {
                                    ++offset;
                                }
                            }
                            else
                            {
                                offset = 1;
                            }
                            column_types.emplace_back(csv_column_type::repeat_t,depth,offset);
                            ++p;
                            break;
                        }
                    default:
                        buffer.clear();
                        state = column_state::label;
                        break;
                    }
                    break;
                }
                case column_state::label:
                {
                    switch (*p)
                    {
                        case '*':
                        {
                            auto it = type_dictionary.find(buffer);
                            if (it != type_dictionary.end())
                            {
                                column_types.emplace_back(it->second,depth);
                                buffer.clear();
                            }
                            else
                            {
                                JSONCONS_ASSERT(false);
                            }
                            state = column_state::sequence;
                            break;
                        }
                        case ',':
                        {
                            auto it = type_dictionary.find(buffer);
                            if (it != type_dictionary.end())
                            {
                                column_types.emplace_back(it->second,depth);
                                buffer.clear();
                            }
                            else
                            {
                                JSONCONS_ASSERT(false);
                            }
                            ++p;
                            state = column_state::sequence;
                            break;
                        }
                        case ']':
                        {
                            JSONCONS_ASSERT(depth > 0);
                            auto it = type_dictionary.find(buffer);
                            if (it != type_dictionary.end())
                            {
                                column_types.emplace_back(it->second,depth);
                                buffer.clear();
                            }
                            else
                            {
                                JSONCONS_ASSERT(false);
                            }
                            --depth;
                            ++p;
                            state = column_state::sequence;
                            break;
                        }
                        default:
                        {
                            buffer.push_back(*p);
                            ++p;
                            break;
                        }
                    }
                    break;
                }
            }
        }
        if (state == column_state::label)
        {
            auto it = type_dictionary.find(buffer);
            if (it != type_dictionary.end())
            {
                column_types.emplace_back(it->second,depth);
                buffer.clear();
            }
            else
            {
                JSONCONS_ASSERT(false);
            }
        }
        return column_types;
    }

};

typedef basic_csv_options<char> csv_options;
typedef basic_csv_options<wchar_t> wcsv_options;

#if !defined(JSONCONS_NO_DEPRECATED)
typedef basic_csv_options<char> csv_parameters;
typedef basic_csv_options<wchar_t> wcsv_parameters;
typedef basic_csv_options<char> csv_serializing_options;
typedef basic_csv_options<wchar_t> wcsv_serializing_options;
#endif


}}
#endif
