// Copyright 2015 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_CSV_CSV_PARSER_HPP
#define JSONCONS_CSV_CSV_PARSER_HPP

#include <memory> // std::allocator
#include <string>
#include <sstream>
#include <vector>
#include <stdexcept>
#include <system_error>
#include <cctype>
#include <jsoncons/json_exception.hpp>
#include <jsoncons/json_content_handler.hpp>
#include <jsoncons/parse_error_handler.hpp>
#include <jsoncons/json_reader.hpp>
#include <jsoncons/json_filter.hpp>
#include <jsoncons/json.hpp>
#include <jsoncons/detail/parse_number.hpp>
#include <jsoncons_ext/csv/csv_error.hpp>
#include <jsoncons_ext/csv/csv_options.hpp>

namespace jsoncons { namespace csv {

enum class csv_mode_type 
{
    initial,
    header,
    data,
    subfields
};

enum class csv_state_type 
{
    start, 
    comment,
    expect_value,
    between_fields,
    quoted_string,
    unquoted_string,
    escaped_value,
    minus, 
    zero,  
    integer,
    fraction,
    exp1,
    exp2,
    exp3,
    done
};

template<class CharT,class Allocator=std::allocator<CharT>>
class basic_csv_parser : public ser_context
{
    typedef basic_string_view<CharT> string_view_type;
    typedef CharT char_type;
    typedef Allocator allocator_type;
    typedef typename std::allocator_traits<allocator_type>:: template rebind_alloc<CharT> char_allocator_type;
    typedef std::basic_string<CharT,std::char_traits<CharT>,char_allocator_type> string_type;
    typedef typename std::allocator_traits<allocator_type>:: template rebind_alloc<string_type> string_allocator_type;
    typedef typename std::allocator_traits<allocator_type>:: template rebind_alloc<csv_mode_type> csv_mode_allocator_type;
    typedef typename std::allocator_traits<allocator_type>:: template rebind_alloc<csv_type_info> csv_type_info_allocator_type;
    typedef typename std::allocator_traits<allocator_type>:: template rebind_alloc<std::vector<string_type,string_allocator_type>> string_vector_allocator_type;
    typedef basic_json<CharT,preserve_order_policy,Allocator> json_type;

    static const int default_depth = 3;

    default_parse_error_handler default_err_handler_;
    csv_state_type state_;
    int top_;
    std::vector<csv_mode_type,csv_mode_allocator_type> stack_;
    basic_json_content_handler<CharT>& handler_;
    parse_error_handler& err_handler_;
    unsigned long column_;
    unsigned long line_;
    CharT prev_char_;
    string_type value_buffer_;
    int depth_;
    const basic_csv_decode_options<CharT>& options_;
    std::vector<string_type,string_allocator_type> column_names_;
    std::vector<std::vector<string_type,string_allocator_type>,string_vector_allocator_type> column_values_;
    std::vector<csv_type_info,csv_type_info_allocator_type> column_types_;
    std::vector<string_type,string_allocator_type> column_defaults_;
    size_t column_index_;
    basic_json_fragment_filter<CharT> filter_;
    size_t level_;
    size_t offset_;
    jsoncons::detail::string_to_double to_double_; 
    std::vector<json_decoder<json_type>> decoders_;
    const CharT* begin_input_;
    const CharT* input_end_;
    const CharT* input_ptr_;
    bool continue_;

public:
    basic_csv_parser(basic_json_content_handler<CharT>& handler)
       : basic_csv_parser(handler, basic_csv_options<CharT>::default_options(), default_err_handler_)
    {
    }

    basic_csv_parser(basic_json_content_handler<CharT>& handler,
                     const basic_csv_decode_options<CharT>& options)
        : basic_csv_parser(handler, options, default_err_handler_)
    {
    }

    basic_csv_parser(basic_json_content_handler<CharT>& handler,
                     parse_error_handler& err_handler)
        : basic_csv_parser(handler, basic_csv_options<CharT>::default_options(), err_handler)
    {
    }

    basic_csv_parser(basic_json_content_handler<CharT>& handler,
                     const basic_csv_decode_options<CharT>& options,
                     parse_error_handler& err_handler)
       : top_(-1),
         stack_(default_depth),
         handler_(handler),
         err_handler_(err_handler),
         options_(options),
         filter_(handler),
         level_(0),
         offset_(0),
         begin_input_(nullptr),
         input_end_(nullptr),
         input_ptr_(nullptr),
         continue_(true)
    {
        depth_ = default_depth;
        state_ = csv_state_type::start;
        top_ = -1;
        line_ = 1;
        column_ = 0;
        column_index_ = 0;
    }

    ~basic_csv_parser()
    {
    }

    bool done() const
    {
        return state_ == csv_state_type::done;
    }

    bool stopped() const
    {
        return !continue_;
    }

    bool source_exhausted() const
    {
        return input_ptr_ == input_end_;
    }

    const std::vector<std::basic_string<CharT>>& column_labels() const
    {
        return column_names_;
    }

    void before_field()
    {
        switch (stack_[top_])
        {
            case csv_mode_type::header:
                if (options_.assume_header() && line_ == 1)
                {
                    column_names_.push_back(value_buffer_);
                }
                break;
            case csv_mode_type::data:
            case csv_mode_type::subfields:
                switch (options_.mapping())
                {
                    case mapping_type::n_objects:
                        if (!(options_.ignore_empty_values() && value_buffer_.size() == 0))
                        {
                            if (column_index_ < column_names_.size() + offset_)
                            {
                                continue_ = handler_.name(column_names_[column_index_ - offset_], *this);
                            }
                        }
                        break;
                    default:
                        break;
                }
                break;
            default:
                break;
        }
    }

    void before_multi_valued_field()
    {
        push_mode(csv_mode_type::subfields);
        switch (options_.mapping())
        {
            case mapping_type::n_rows:
            case mapping_type::n_objects:
                continue_ = handler_.begin_array(semantic_tag::none, *this);
                break;
            case mapping_type::m_columns:
                decoders_[column_index_].begin_array(semantic_tag::none, *this);
                break;
            default:
                break;
        }
    }

    void after_field()
    {
        if (stack_[top_] == csv_mode_type::subfields)
        {
            pop_mode(csv_mode_type::subfields);
            switch (options_.mapping())
            {
                case mapping_type::n_rows:
                case mapping_type::n_objects:
                    continue_ = handler_.end_array(*this);
                    break;
                case mapping_type::m_columns:
                    decoders_[column_index_].end_array(*this);
                    break;
                default:
                    break;
            }
        }
        ++column_index_;
    }

    void before_record()
    {
        offset_ = 0;
        if (stack_[top_] == csv_mode_type::data)
        {
            switch (options_.mapping())
            {
                case mapping_type::n_rows:
                    continue_ = handler_.begin_array(semantic_tag::none, *this);
                    break;
                case mapping_type::n_objects:
                    continue_ = handler_.begin_object(semantic_tag::none, *this);
                    break;
                case mapping_type::m_columns:
                    break;
                default:
                    break;
            }
        }
    }

    void after_record()
    {
        if (column_types_.size() > 0)
        {
            if (level_ > 0)
            {
                continue_ = handler_.end_array(*this);
                level_ = 0;
            }
        }
        if (stack_[top_] == csv_mode_type::header)
        {
            if (line_ >= options_.header_lines())
            {
                flip(csv_mode_type::header, csv_mode_type::data);
            }
            column_values_.resize(column_names_.size());
            switch (options_.mapping())
            {
                case mapping_type::n_rows:
                    if (column_names_.size() > 0)
                    {
                        continue_ = handler_.begin_array(semantic_tag::none, *this);
                        for (const auto& name : column_names_)
                        {
                            continue_ = handler_.string_value(name, semantic_tag::none, *this);
                        }
                        continue_ = handler_.end_array(*this);
                    }
                    break;
                case mapping_type::m_columns:
                    for (size_t i = 0; i < column_names_.size(); ++i)
                    {
                        decoders_.push_back(json_decoder<json_type>());
                        decoders_.back().begin_array(semantic_tag::none, *this);
                    }
                    break;
                default:
                    break;
            }
        }
        else if (stack_[top_] == csv_mode_type::data || stack_[top_] == csv_mode_type::subfields)
        {
            switch (options_.mapping())
            {
                case mapping_type::n_rows:
                    continue_ = handler_.end_array(*this);
                    break;
                case mapping_type::n_objects:
                    continue_ = handler_.end_object(*this);
                    break;
                default:
                    break;
            }
        }
        column_index_ = 0;
    }

    void reset()
    {
        push_mode(csv_mode_type::initial);

        for (auto name : options_.column_names())
        {
            column_names_.emplace_back(name.data(),name.size());
        }
        for (auto name : options_.column_types())
        {
            column_types_.push_back(name);
        }
        for (auto name : options_.column_defaults())
        {
            column_defaults_.emplace_back(name.data(), name.size());
        }
        if (options_.header_lines() > 0)
        {
            push_mode(csv_mode_type::header);
        }
        else
        {
            push_mode(csv_mode_type::data);
        }
        if (options_.mapping() != mapping_type::m_columns)
        {
            continue_ = handler_.begin_array(semantic_tag::none, *this);
        }
        state_ = csv_state_type::expect_value;
        column_index_ = 0;
        prev_char_ = 0;
        column_ = 1;
        level_ = 0;
    }

    void parse_some()
    {
        std::error_code ec;
        parse_some(ec);
        if (ec)
        {
            throw ser_error(ec,line_,column_);
        }
    }

    void parse_some(std::error_code& ec)
    {
        const CharT* local_input_end = input_end_;

        if (input_ptr_ == local_input_end && continue_)
        {
            switch (state_)
            {
                case csv_state_type::unquoted_string: 
                    if (options_.trim_leading() || options_.trim_trailing())
                    {
                        trim_string_buffer(options_.trim_leading(),options_.trim_trailing());
                    }
                    if (!options_.ignore_empty_lines() || (column_index_ > 0 || value_buffer_.length() > 0))
                    {
                        if (column_index_ == 0)
                        {
                            before_record();
                        }
                        if (stack_[top_] != csv_mode_type::subfields)
                        {
                            before_field();
                        }
                        end_unquoted_string_value();
                        after_field();
                    }
                    break;
                case csv_state_type::escaped_value:
                    if (options_.quote_escape_char() == options_.quote_char())
                    {
                        if (column_index_ == 0)
                        {
                            before_record();
                        }
                        if (stack_[top_] != csv_mode_type::subfields)
                        {
                            before_field();
                        }
                        end_quoted_string_value(ec);
                        if (ec) return;
                        after_field();
                    }
                    break;
                default:
                    break;
            }
            if (column_index_ > 0)
            {
                after_record();
            }
            switch (stack_[top_])
            {
                case csv_mode_type::header:
                    pop_mode(csv_mode_type::header);
                    break;
                case csv_mode_type::data:
                    pop_mode(csv_mode_type::data);
                    break;
                default:
                    break;
            }
            if (options_.mapping() == mapping_type::m_columns)
            {
                basic_json_fragment_filter<CharT> fragment_filter(handler_);
                continue_ = handler_.begin_object(semantic_tag::none, *this);
                for (size_t i = 0; i < column_names_.size(); ++i)
                {
                    continue_ = handler_.name(column_names_[i],*this);
                    decoders_[i].end_array(*this);
                    decoders_[i].flush();
                    decoders_[i].get_result().dump(fragment_filter);
                }
                continue_ = handler_.end_object(*this);
            }
            else
            {
                continue_ = handler_.end_array(*this);
            }
            if (!pop_mode(csv_mode_type::initial))
            {
                err_handler_.fatal_error(csv_errc::unexpected_eof, *this);
                ec = csv_errc::unexpected_eof;
                continue_ = false;
                return;
            }
            handler_.flush();
            continue_ = false;
        }

        for (; (input_ptr_ < local_input_end) && continue_; ++input_ptr_)
        {
            CharT curr_char = *input_ptr_;
all_csv_states:
            switch (state_)
            {
                case csv_state_type::comment:
                    if (curr_char == '\n')
                    {
                        state_ = csv_state_type::expect_value;
                    }
                    else if (prev_char_ == '\r')
                    {
                        state_ = csv_state_type::expect_value;
                        goto all_csv_states;
                    }
                    break;
                case csv_state_type::expect_value:
                    if (column_ == 1 && curr_char == options_.comment_starter())
                    {
                        state_ = csv_state_type::comment;
                    }
                    else
                    {
                        state_ = csv_state_type::unquoted_string;
                        goto all_csv_states;
                    }
                    break;
                case csv_state_type::escaped_value: 
                    {
                        if (curr_char == options_.quote_char())
                        {
                            value_buffer_.push_back(static_cast<CharT>(curr_char));
                            state_ = csv_state_type::quoted_string;
                        }
                        else if (options_.quote_escape_char() == options_.quote_char())
                        {
                            state_ = csv_state_type::between_fields;
                            goto all_csv_states;
                        }
                    }
                    break;
                case csv_state_type::quoted_string: 
                    {
                        if (curr_char == options_.quote_escape_char())
                        {
                            state_ = csv_state_type::escaped_value;
                        }
                        else if (curr_char == options_.quote_char())
                        {
                            state_ = csv_state_type::between_fields;
                        }
                        else
                        {
                            value_buffer_.push_back(static_cast<CharT>(curr_char));
                        }
                    }
                    break;
                case csv_state_type::between_fields:
                    if (prev_char_ == '\r' && curr_char == '\n')
                    {
                    }
                    else if (curr_char == '\r' || curr_char == '\n')
                    {
                        if (options_.trim_leading() || options_.trim_trailing())
                        {
                            trim_string_buffer(options_.trim_leading(),options_.trim_trailing());
                        }
                        if (!options_.ignore_empty_lines() || (column_index_ > 0 || value_buffer_.length() > 0))
                        {
                            if (column_index_ == 0)
                            {
                                before_record();
                            }
                            if (stack_[top_] != csv_mode_type::subfields)
                            {
                                before_field();
                            }
                            end_quoted_string_value(ec);
                            if (ec) return;
                            after_field();
                            after_record();
                        }
                        state_ = csv_state_type::expect_value;
                    }
                    else if (curr_char == options_.field_delimiter() || (options_.subfield_delimiter().second && curr_char == options_.subfield_delimiter().first))
                    {
                        if (column_index_ == 0 && stack_[top_] != csv_mode_type::subfields)
                        {
                            before_record();
                        }
                        if (options_.trim_leading() || options_.trim_trailing())
                        {
                            trim_string_buffer(options_.trim_leading(),options_.trim_trailing());
                        }
                        if (stack_[top_] != csv_mode_type::subfields)
                        {
                            before_field();
                            if (options_.subfield_delimiter().second && curr_char == options_.subfield_delimiter().first)
                            {
                                before_multi_valued_field();
                            }
                        }
                        end_quoted_string_value(ec);
                        if (ec) return;
                        if (curr_char == options_.field_delimiter())
                        {
                            after_field();
                        }
                        state_ = csv_state_type::unquoted_string;
                    }
                    break;
                case csv_state_type::unquoted_string: 
                    {
                        if (prev_char_ == '\r' && curr_char == '\n')
                        {
                        }
                        else if (curr_char == '\r' || curr_char == '\n')
                        {
                            if (options_.trim_leading() || options_.trim_trailing())
                            {
                                trim_string_buffer(options_.trim_leading(),options_.trim_trailing());
                            }
                            if (!options_.ignore_empty_lines() || (column_index_ > 0 || value_buffer_.length() > 0))
                            {
                                if (column_index_ == 0)
                                {
                                    before_record();
                                }
                                if (stack_[top_] != csv_mode_type::subfields)
                                {
                                    before_field();
                                }
                                end_unquoted_string_value();
                                after_field();
                                after_record();
                            }
                            state_ = csv_state_type::expect_value;
                        }
                        else if (curr_char == options_.field_delimiter() || (options_.subfield_delimiter().second && curr_char == options_.subfield_delimiter().first))
                        {
                            if (column_index_ == 0 && stack_[top_] != csv_mode_type::subfields)
                            {
                                before_record();
                            }
                            if (options_.trim_leading() || options_.trim_trailing())
                            {
                                trim_string_buffer(options_.trim_leading(),options_.trim_trailing());
                            }
                            if (stack_[top_] != csv_mode_type::subfields)
                            {
                                before_field();
                                if (options_.subfield_delimiter().second && curr_char == options_.subfield_delimiter().first)
                                {
                                    before_multi_valued_field();
                                }
                            }
                            end_unquoted_string_value();
                            if (curr_char == options_.field_delimiter())
                            {
                                after_field();
                            }
                            state_ = csv_state_type::unquoted_string;
                        }
                        else if (curr_char == options_.quote_char())
                        {
                            value_buffer_.clear();
                            state_ = csv_state_type::quoted_string;
                        }
                        else
                        {
                            value_buffer_.push_back(static_cast<CharT>(curr_char));
                        }
                    }
                    break;
                default:
                    err_handler_.fatal_error(csv_errc::invalid_state, *this);
                    ec = csv_errc::invalid_state;
                    continue_ = false;
                    return;
            }
            if (line_ > options_.max_lines())
            {
                state_ = csv_state_type::done;
                continue_ = false;
            }
            switch (curr_char)
            {
                case '\r':
                    ++line_;
                    column_ = 1;
                    break;
                case '\n':
                    if (prev_char_ != '\r')
                    {
                        ++line_;
                    }
                    column_ = 1;
                    break;
                default:
                    ++column_;
                    break;
            }
            prev_char_ = curr_char;
        }
    }

    void finish_parse()
    {
        std::error_code ec;
        finish_parse(ec);
        if (ec)
        {
            throw ser_error(ec,line_,column_);
        }
    }

    void finish_parse(std::error_code& ec)
    {
        while (continue_)
        {
            parse_some(ec);
        }
    }

    csv_state_type state() const
    {
        return state_;
    }

    void update(const string_view_type sv)
    {
        update(sv.data(),sv.length());
    }

    void update(const CharT* data, size_t length)
    {
        begin_input_ = data;
        input_end_ = data + length;
        input_ptr_ = begin_input_;
    }

    size_t line() const override
    {
        return line_;
    }

    size_t column() const override
    {
        return column_;
    }
private:

    void trim_string_buffer(bool trim_leading, bool trim_trailing)
    {
        size_t start = 0;
        size_t length = value_buffer_.length();
        if (trim_leading)
        {
            bool done = false;
            while (!done && start < value_buffer_.length())
            {
                if ((value_buffer_[start] < 256) && std::isspace(value_buffer_[start]))
                {
                    ++start;
                }
                else
                {
                    done = true;
                }
            }
        }
        if (trim_trailing)
        {
            bool done = false;
            while (!done && length > 0)
            {
                if ((value_buffer_[length-1] < 256) && std::isspace(value_buffer_[length-1]))
                {
                    --length;
                }
                else
                {
                    done = true;
                }
            }
        }
        if (start != 0 || length != value_buffer_.size())
        {
            value_buffer_ = value_buffer_.substr(start,length-start);
        }
    }

    void end_unquoted_string_value() 
    {
        switch (stack_[top_])
        {
            case csv_mode_type::data:
            case csv_mode_type::subfields:
                switch (options_.mapping())
                {
                case mapping_type::n_rows:
                    if (options_.unquoted_empty_value_is_null() && value_buffer_.length() == 0)
                    {
                        continue_ = handler_.null_value(semantic_tag::none, *this);
                    }
                    else
                    {
                        end_value(value_buffer_,column_index_,options_.infer_types(),handler_);
                    }
                    break;
                case mapping_type::n_objects:
                    if (!(options_.ignore_empty_values() && value_buffer_.size() == 0))
                    {
                        if (column_index_ < column_names_.size() + offset_)
                        {
                            if (options_.unquoted_empty_value_is_null() && value_buffer_.length() == 0)
                            {
                                continue_ = handler_.null_value(semantic_tag::none, *this);
                            }
                            else
                            {
                                end_value(value_buffer_,column_index_,options_.infer_types(),handler_);
                            }
                        }
                        else if (level_ > 0)
                        {
                            if (options_.unquoted_empty_value_is_null() && value_buffer_.length() == 0)
                            {
                                continue_ = handler_.null_value(semantic_tag::none, *this);
                            }
                            else
                            {
                                end_value(value_buffer_,column_index_,options_.infer_types(),handler_);
                            }
                        }
                    }
                    break;
                case mapping_type::m_columns:
                    if (column_index_ < decoders_.size())
                    {
                        if (!(options_.ignore_empty_values() && value_buffer_.size() == 0))
                        {
                            end_value(value_buffer_,column_index_,options_.infer_types(),decoders_[column_index_]);
                        }
                    }
                    break;
                }
                break;
            default:
                break;
        }
        state_ = csv_state_type::expect_value;
        value_buffer_.clear();
    }

    void end_quoted_string_value(std::error_code& ec) 
    {
        if (options_.trim_leading_inside_quotes() | options_.trim_trailing_inside_quotes())
        {
            trim_string_buffer(options_.trim_leading_inside_quotes(),options_.trim_trailing_inside_quotes());
        }
        switch (stack_[top_])
        {
            case csv_mode_type::header:
                break;
            case csv_mode_type::data:
            case csv_mode_type::subfields:
                switch (options_.mapping())
                {
                case mapping_type::n_rows:
                    end_value(value_buffer_,column_index_,false,handler_);
                    break;
                case mapping_type::n_objects:
                    if (!(options_.ignore_empty_values() && value_buffer_.size() == 0))
                    {
                        if (column_index_ < column_names_.size() + offset_)
                        {
                            if (options_.unquoted_empty_value_is_null() && value_buffer_.length() == 0)
                            {
                                continue_ = handler_.null_value(semantic_tag::none, *this);
                            }
                            else
                            {
                                end_value(value_buffer_,column_index_,false,handler_);
                            }
                        }
                        else if (level_ > 0)
                        {
                            if (options_.unquoted_empty_value_is_null() && value_buffer_.length() == 0)
                            {
                                continue_ = handler_.null_value(semantic_tag::none, *this);
                            }
                            else
                            {
                                end_value(value_buffer_,column_index_,false,handler_);
                            }
                        }
                    }
                    break;
                case mapping_type::m_columns:
                    if (column_index_ < decoders_.size())
                    {
                        end_value(value_buffer_,column_index_,options_.infer_types(),decoders_[column_index_]);
                    }
                    break;
                }
                break;
            default:
                err_handler_.fatal_error(csv_errc::invalid_csv_text, *this);
                ec = csv_errc::invalid_csv_text;
                continue_ = false;
                return;
        }
        state_ = csv_state_type::expect_value;
        value_buffer_.clear();
    }

    void end_value(const string_view_type& value, 
                   size_t column_index, 
                   bool infer_types, 
                   basic_json_content_handler<CharT>& handler)
    {
        if (column_index < column_types_.size() + offset_)
        {
            if (column_types_[column_index - offset_].col_type == csv_column_type::repeat_t)
            {
                offset_ = offset_ + column_types_[column_index - offset_].rep_count;
                if (column_index - offset_ + 1 < column_types_.size())
                {
                    if (column_index == offset_ || level_ > column_types_[column_index-offset_].level)
                    {
                        handler.end_array(*this);
                    }
                    level_ = column_index == offset_ ? 0 : column_types_[column_index - offset_].level;
                }
            }
            if (level_ < column_types_[column_index - offset_].level)
            {
                handler.begin_array(semantic_tag::none, *this);
                level_ = column_types_[column_index - offset_].level;
            }
            else if (level_ > column_types_[column_index - offset_].level)
            {
                handler.end_array(*this);
                level_ = column_types_[column_index - offset_].level;
            }
            switch (column_types_[column_index - offset_].col_type)
            {
                case csv_column_type::integer_t:
                    {
                        std::istringstream iss{ std::string(value) };
                        int64_t val;
                        iss >> val;
                        if (!iss.fail())
                        {
                            handler.int64_value(val, semantic_tag::none, *this);
                        }
                        else
                        {
                            if (column_index - offset_ < column_defaults_.size() && column_defaults_[column_index - offset_].length() > 0)
                            {
                                basic_json_parser<CharT> parser(err_handler_);
                                parser.update(column_defaults_[column_index - offset_].data(),column_defaults_[column_index - offset_].length());
                                parser.parse_some(filter_);
                                parser.finish_parse(filter_);
                            }
                            else
                            {
                                handler.null_value(semantic_tag::none, *this);
                            }
                        }
                    }
                    break;
                case csv_column_type::float_t:
                    {
                        if (options_.lossless_number())
                        {
                            handler.string_value(value,semantic_tag::bigdec, *this);
                        }
                        else
                        {
                            std::istringstream iss{ std::string(value) };
                            double val;
                            iss >> val;
                            if (!iss.fail())
                            {
                                handler.double_value(val, semantic_tag::none, *this);
                            }
                            else
                            {
                                if (column_index - offset_ < column_defaults_.size() && column_defaults_[column_index - offset_].length() > 0)
                                {
                                    basic_json_parser<CharT> parser(err_handler_);
                                    parser.update(column_defaults_[column_index - offset_].data(),column_defaults_[column_index - offset_].length());
                                    parser.parse_some(filter_);
                                    parser.finish_parse(filter_);
                                }
                                else
                                {
                                    handler.null_value(semantic_tag::none, *this);
                                }
                            }
                        }
                    }
                    break;
                case csv_column_type::boolean_t:
                    {
                        if (value.length() == 1 && value[0] == '0')
                        {
                            handler.bool_value(false, semantic_tag::none, *this);
                        }
                        else if (value.length() == 1 && value[0] == '1')
                        {
                            handler.bool_value(true, semantic_tag::none, *this);
                        }
                        else if (value.length() == 5 && ((value[0] == 'f' || value[0] == 'F') && (value[1] == 'a' || value[1] == 'A') && (value[2] == 'l' || value[2] == 'L') && (value[3] == 's' || value[3] == 'S') && (value[4] == 'e' || value[4] == 'E')))
                        {
                            handler.bool_value(false, semantic_tag::none, *this);
                        }
                        else if (value.length() == 4 && ((value[0] == 't' || value[0] == 'T') && (value[1] == 'r' || value[1] == 'R') && (value[2] == 'u' || value[2] == 'U') && (value[3] == 'e' || value[3] == 'E')))
                        {
                            handler.bool_value(true, semantic_tag::none, *this);
                        }
                        else
                        {
                            if (column_index - offset_ < column_defaults_.size() && column_defaults_[column_index - offset_].length() > 0)
                            {
                                basic_json_parser<CharT> parser(err_handler_);
                                parser.update(column_defaults_[column_index - offset_].data(),column_defaults_[column_index - offset_].length());
                                parser.parse_some(filter_);
                                parser.finish_parse(filter_);
                            }
                            else
                            {
                                handler.null_value(semantic_tag::none, *this);
                            }
                        }
                    }
                    break;
                default:
                    if (value.length() > 0)
                    {
                        handler.string_value(value, semantic_tag::none, *this);
                    }
                    else
                    {
                        if (column_index < column_defaults_.size() + offset_ && column_defaults_[column_index - offset_].length() > 0)
                        {
                            basic_json_parser<CharT> parser(err_handler_);
                            parser.update(column_defaults_[column_index - offset_].data(),column_defaults_[column_index - offset_].length());
                            parser.parse_some(filter_);
                            parser.finish_parse(filter_);
                        }
                        else
                        {
                            handler.string_value(string_view_type(), semantic_tag::none, *this);
                        }
                    }
                    break;  
            }
        }
        else
        {
            if (infer_types)
            {
                end_value_with_numeric_check(value, handler);
            }
            else
            {
                handler.string_value(value, semantic_tag::none, *this);
            }
        }
    }

    enum class numeric_check_state 
    {
        initial,
        null,
        boolean_true,
        boolean_false,
        minus,
        zero,
        integer,
        fraction1,
        fraction,
        exp1,
        exp,
        done
    };

    void end_value_with_numeric_check(const string_view_type& value, basic_json_content_handler<CharT>& handler)
    {
        numeric_check_state state = numeric_check_state::initial;
        bool is_negative = false;
        int precision = 0;
        uint8_t decimal_places = 0;

        auto last = value.end();

        std::string buffer;
        for (auto p = value.begin(); state != numeric_check_state::done && p != last; ++p)
        {
            switch (state)
            {
                case numeric_check_state::initial:
                {
                    switch (*p)
                    {
                    case 'n':case 'N':
                        if ((last-p) == 4 && (p[1] == 'u' || p[1] == 'U') && (p[2] == 'l' || p[2] == 'L') && (p[3] == 'l' || p[3] == 'L'))
                        {
                            state = numeric_check_state::null;
                        }
                        else
                        {
                            state = numeric_check_state::done;
                        }
                        break;
                    case 't':case 'T':
                        if ((last-p) == 4 && (p[1] == 'r' || p[1] == 'R') && (p[2] == 'u' || p[2] == 'U') && (p[3] == 'e' || p[3] == 'U'))
                        {
                            state = numeric_check_state::boolean_true;
                        }
                        else
                        {
                            state = numeric_check_state::done;
                        }
                        break;
                    case 'f':case 'F':
                        if ((last-p) == 5 && (p[1] == 'a' || p[1] == 'A') && (p[2] == 'l' || p[2] == 'L') && (p[3] == 's' || p[3] == 'S') && (p[4] == 'e' || p[4] == 'E'))
                        {
                            state = numeric_check_state::boolean_false;
                        }
                        else
                        {
                            state = numeric_check_state::done;
                        }
                        break;
                    case '-':
                        is_negative = true;
                        buffer.push_back(*p);
                        state = numeric_check_state::minus;
                        break;
                    case '0':
                        ++precision;
                        buffer.push_back(*p);
                        state = numeric_check_state::zero;
                        break;
                    case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8':case '9':
                        ++precision;
                        buffer.push_back(*p);
                        state = numeric_check_state::integer;
                        break;
                    default:
                        state = numeric_check_state::done;
                        break;
                    }
                    break;
                }
                case numeric_check_state::zero:
                {
                    switch (*p)
                    {
                    case '.':
                        buffer.push_back(to_double_.get_decimal_point());
                        state = numeric_check_state::fraction1;
                        break;
                    case 'e':case 'E':
                        buffer.push_back(*p);
                        state = numeric_check_state::exp1;
                        break;
                    default:
                        state = numeric_check_state::done;
                        break;
                    }
                    break;
                }
                case numeric_check_state::integer:
                {
                    switch (*p)
                    {
                    case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8':case '9':
                        ++precision;
                        buffer.push_back(*p);
                        break;
                    case '.':
                        buffer.push_back(to_double_.get_decimal_point());
                        state = numeric_check_state::fraction1;
                        break;
                    case 'e':case 'E':
                        buffer.push_back(*p);
                        state = numeric_check_state::exp1;
                        break;
                    default:
                        state = numeric_check_state::done;
                        break;
                    }
                    break;
                }
                case numeric_check_state::minus:
                {
                    switch (*p)
                    {
                    case '0':
                        ++precision;
                        buffer.push_back(*p);
                        state = numeric_check_state::zero;
                        break;
                    case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8':case '9':
                        ++precision;
                        buffer.push_back(*p);
                        state = numeric_check_state::integer;
                        break;
                    case 'e':case 'E':
                        buffer.push_back(*p);
                        state = numeric_check_state::exp1;
                        break;
                    default:
                        state = numeric_check_state::done;
                        break;
                    }
                    break;
                }
                case numeric_check_state::fraction1:
                {
                    switch (*p)
                    {
                    case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8':case '9':
                        ++precision;
                        ++decimal_places;
                        buffer.push_back(*p);
                        state = numeric_check_state::fraction;
                        break;
                    default:
                        state = numeric_check_state::done;
                        break;
                    }
                    break;
                }
                case numeric_check_state::fraction:
                {
                    switch (*p)
                    {
                    case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8':case '9':
                        ++precision;
                        ++decimal_places;
                        buffer.push_back(*p);
                        break;
                    case 'e':case 'E':
                        buffer.push_back(*p);
                        state = numeric_check_state::exp1;
                        break;
                    default:
                        state = numeric_check_state::done;
                        break;
                    }
                    break;
                }
                case numeric_check_state::exp1:
                {
                    switch (*p)
                    {
                    case '-':
                        buffer.push_back(*p);
                        state = numeric_check_state::exp;
                        break;
                    case '+':
                        state = numeric_check_state::exp;
                        break;
                    case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8':case '9':
                        buffer.push_back(*p);
                        state = numeric_check_state::integer;
                        break;
                    default:
                        state = numeric_check_state::done;
                        break;
                    }
                    break;
                }
                case numeric_check_state::exp:
                {
                    switch (*p)
                    {
                    case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8':case '9':
                        buffer.push_back(*p);
                        break;
                    default:
                        state = numeric_check_state::done;
                        break;
                    }
                    break;
                }
                default:
                    break;
            }
        }

        switch (state)
        {
            case numeric_check_state::null:
                handler.null_value(semantic_tag::none, *this);
                break;
            case numeric_check_state::boolean_true:
                handler.bool_value(true, semantic_tag::none, *this);
                break;
            case numeric_check_state::boolean_false:
                handler.bool_value(false, semantic_tag::none, *this);
                break;
            case numeric_check_state::zero:
            case numeric_check_state::integer:
            {
                if (is_negative)
                {
                    auto result = jsoncons::detail::to_integer<int64_t>(value.data(), value.length());
                    if (result.ec == jsoncons::detail::to_integer_errc())
                    {
                        handler.int64_value(result.value, semantic_tag::none, *this);
                    }
                    else // Must be overflow
                    {
                        handler.string_value(value, semantic_tag::bigint, *this);
                    }
                }
                else
                {
                    auto result = jsoncons::detail::to_integer<uint64_t>(value.data(), value.length());
                    if (result.ec == jsoncons::detail::to_integer_errc())
                    {
                        handler.uint64_value(result.value, semantic_tag::none, *this);
                    }
                    else if (result.ec == jsoncons::detail::to_integer_errc::overflow)
                    {
                        handler.string_value(value, semantic_tag::bigint, *this);
                    }
                    else
                    {
                        JSONCONS_THROW(json_runtime_error<std::invalid_argument>(make_error_code(result.ec).message()));
                    }
                }
                break;
            }
            case numeric_check_state::fraction:
            case numeric_check_state::exp:
            {
                if (options_.lossless_number())
                {
                    handler.string_value(value,semantic_tag::bigdec, *this);
                }
                else
                {
                    double d = to_double_(buffer.c_str(), buffer.length());
                    handler.double_value(d, semantic_tag::none, *this);
                }
                break;
            }
            default:
                handler.string_value(value, semantic_tag::none, *this);
        }
    }

    void push_mode(csv_mode_type mode)
    {
        ++top_;
        if (top_ >= depth_)
        {
            depth_ *= 2;
            stack_.resize(depth_);
        }
        stack_[top_] = mode;
    }

    int peek()
    {
        return stack_[top_];
    }

    bool peek(csv_mode_type mode)
    {
        return stack_[top_] == mode;
    }

    bool flip(csv_mode_type mode1, csv_mode_type mode2)
    {
        if (top_ < 0 || stack_[top_] != mode1)
        {
            return false;
        }
        stack_[top_] = mode2;
        return true;
    }

    bool pop_mode(csv_mode_type mode)
    {
        if (top_ < 0 || stack_[top_] != mode)
        {
            return false;
        }
        --top_;
        return true;
    }
};

typedef basic_csv_parser<char> csv_parser;
typedef basic_csv_parser<wchar_t> wcsv_parser;

}}

#endif

