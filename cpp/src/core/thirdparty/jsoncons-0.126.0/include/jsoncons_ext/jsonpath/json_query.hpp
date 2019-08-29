// Copyright 2013 Daniel Parkerpath_single_quoted
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_JSONPATH_JSON_QUERY_HPP
#define JSONCONS_JSONPATH_JSON_QUERY_HPP

#include <array> // std::array
#include <string>
#include <vector>
#include <memory>
#include <type_traits> // std::is_const
#include <limits> // std::numeric_limits
#include <utility> // std::move
#include <regex>
#include <set> // std::set
#include <iterator> // std::make_move_iterator
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath_filter.hpp>
#include <jsoncons_ext/jsonpath/jsonpath_error.hpp>
#include <jsoncons_ext/jsonpath/jsonpath_function.hpp>

namespace jsoncons { namespace jsonpath {

struct array_slice
{
    size_t start_;
    bool is_start_positive;
    size_t end_;
    bool is_end_positive;
    bool is_end_defined;
    size_t step_;
    bool is_step_positive;

    array_slice()
        : start_(0), is_start_positive(true), 
          end_(0), is_end_positive(true), is_end_defined(false), 
          step_(1), is_step_positive(true)
    {
    }

    array_slice(size_t start, bool is_start_positive, 
                size_t end, bool is_end_positive, bool is_end_defined,
                size_t step, bool is_step_positive)
        : start_(start), is_start_positive(is_start_positive), 
          end_(end), is_end_positive(is_end_positive), is_end_defined(is_end_defined), 
          step_(step), is_step_positive(is_step_positive)
    {
    }

    size_t get_start(size_t size) const
    {
        return is_start_positive ? start_ : size - start_;
    }

    size_t get_end(size_t size) const
    {
        if (is_end_defined)
        {
            return is_end_positive ? end_ : size - end_;
        }
        else
        {
            return size;
        }
    }

    size_t step() const
    {
        return step_;
    }

    array_slice(const array_slice&) = default;

    array_slice& operator=(const array_slice&) = default;
};

// work around for std::make_unique not being available until C++14
template<typename T, typename... Args>
std::unique_ptr<T> make_unique_ptr(Args&&... args)
{
    return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}

enum class result_type {value,path};

template<class Json>
Json json_query(const Json& root, const typename Json::string_view_type& path, result_type result_t = result_type::value)
{
    if (result_t == result_type::value)
    {
        jsoncons::jsonpath::detail::jsonpath_evaluator<Json,const Json&,detail::VoidPathConstructor<Json>> evaluator;
        evaluator.evaluate(root, path);
        return evaluator.get_values();
    }
    else
    {
        jsoncons::jsonpath::detail::jsonpath_evaluator<Json,const Json&,detail::PathConstructor<Json>> evaluator;
        evaluator.evaluate(root, path);
        return evaluator.get_normalized_paths();
    }
}

template<class Json, class T>
void json_replace(Json& root, const typename Json::string_view_type& path, T&& new_value)
{
    jsoncons::jsonpath::detail::jsonpath_evaluator<Json,Json&,detail::VoidPathConstructor<Json>> evaluator;
    evaluator.evaluate(root, path);
    evaluator.replace(std::forward<T>(new_value));
}

namespace detail {

template<class CharT>
bool try_string_to_index(const CharT *s, size_t length, size_t* value, bool* positive)
{
    static const size_t max_value = (std::numeric_limits<size_t>::max)();
    static const size_t max_value_div_10 = max_value / 10;

    size_t start = 0;
    size_t n = 0;
    if (length > 0)
    {
        if (s[start] == '-')
        {
            *positive = false;
            ++start;
        }
        else
        {
            *positive = true;
        }
    }
    if (length > start)
    {
        for (size_t i = start; i < length; ++i)
        {
            CharT c = s[i];
            switch (c)
            {
                case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8':case '9':
                {
                    size_t x = c - '0';
                    if (n > max_value_div_10)
                    {
                        return false;
                    }
                    n = n * 10;
                    if (n > max_value - x)
                    {
                        return false;
                    }

                    n += x;
                    break;
                }
                default:
                    return false;
                    break;
            }
        }
        *value = n;
        return true;
    }
    else
    {
        return false;
    }
}
 
enum class path_state 
{
    start,
    dot_or_left_bracket,
    name_or_left_bracket,
    name,
    unquoted_name,
    unquoted_name2,
    single_quoted_name,
    double_quoted_name,
    bracketed_unquoted_name,
    bracketed_single_quoted_name,
    bracketed_double_quoted_name,
    bracketed_name_or_path,
    bracketed_wildcard_or_path,
    wildcard_or_rpath_or_slice_or_filter,
    slice_end_or_end_step,
    slice_end,
    slice_step,
    slice_step2,
    comma_or_right_bracket,
    path_or_function_name,
    function,
    arg_or_right_paren,
    path_argument,
    unquoted_arg,
    single_quoted_arg,
    double_quoted_arg,
    more_args_or_right_paren,
    dot,
    path,
    path2,
    path_single_quoted,
    path_double_quoted
};

struct state_item
{
    path_state state;
    bool is_recursive_descent;
    bool is_union;

    state_item()
        : state(path_state::start), is_recursive_descent(false), is_union(false)
    {
    }

    explicit state_item(path_state state)
        : state(state), is_recursive_descent(false), is_union(false)
    {
    }

    state_item(path_state state, const state_item& parent)
        : state(state), 
          is_recursive_descent(parent.is_recursive_descent), 
          is_union(parent.is_union)
    {
    }

    state_item(const state_item&) = default;
    state_item& operator=(const state_item&) = default;
};

JSONCONS_STRING_LITERAL(length, 'l', 'e', 'n', 'g', 't', 'h')

template<class Json,
         class JsonReference,
         class PathCons>
class jsonpath_evaluator : public ser_context
{
    typedef typename Json::char_type char_type;
    typedef typename Json::char_traits_type char_traits_type;
    typedef std::basic_string<char_type,char_traits_type> string_type;
    typedef typename Json::string_view_type string_view_type;
    typedef JsonReference reference;
    using pointer = typename std::conditional<std::is_const<typename std::remove_reference<JsonReference>::type>::value,typename Json::const_pointer,typename Json::pointer>::type;
    typedef typename Json::const_pointer const_pointer;

    struct node_type
    {
        string_type path;
        pointer val_ptr;

        node_type() = default;
        node_type(const string_type& p, const pointer& valp)
            : path(p),val_ptr(valp)
        {
        }
        node_type(string_type&& p, pointer&& valp)
            : path(std::move(p)),val_ptr(valp)
        {
        }
        node_type(const node_type&) = default;
        node_type(node_type&& other)
            : path(std::move(other.path)), val_ptr(other.val_ptr)
        {

        }
        node_type& operator=(const node_type&) = default;
        node_type& operator=(node_type&& other) 
        {
            path.swap(other.path);
            val_ptr = other.val_ptr;
        }

    };
    typedef std::vector<node_type> node_set;

    struct node_less
    {
        bool operator()(const node_type& a, const node_type& b) const
        {
            return *(a.val_ptr) < *(b.val_ptr);
        }
    };

    class selector_base
    {
    public:
        virtual ~selector_base()
        {
        }
        virtual void select(jsonpath_evaluator& evaluator,
                            const string_type& path, reference val, node_set& nodes) = 0;

        virtual bool is_filter() const
        {
            return false;
        }
    };

    class path_selector final : public selector_base
    {
    private:
         std::basic_string<char_type> path_;
    public:
        path_selector(const std::basic_string<char_type>& path)
            : path_(path)
        {
        }

        void select(jsonpath_evaluator&,
                    const string_type& path, reference val, 
                    node_set& nodes) override
        {
            std::error_code ec;
            jsonpath_evaluator<Json,JsonReference,PathCons> e;
            e.evaluate(val, path_, ec);
            if (!ec)
            {
                for (auto ptr : e.get_pointers())
                {
                    nodes.emplace_back(PathCons()(path,path_),ptr);
                }
            }
        }
    };

    class expr_selector final : public selector_base
    {
    private:
         jsonpath_filter_expr<Json> result_;
    public:
        expr_selector(const jsonpath_filter_expr<Json>& result)
            : result_(result)
        {
        }

        void select(jsonpath_evaluator& evaluator,
                    const string_type& path, reference val, 
                    node_set& nodes) override
        {
            auto index = result_.eval(val);
            if (index.template is<size_t>())
            {
                size_t start = index.template as<size_t>();
                if (val.is_array() && start < val.size())
                {
                    nodes.emplace_back(PathCons()(path,start),std::addressof(val[start]));
                }
            }
            else if (index.is_string())
            {
                name_selector selector(index.as_string_view());
                selector.select(evaluator, path, val, nodes);
            }
        }
    };

    class filter_selector final : public selector_base
    {
    private:
         jsonpath_filter_expr<Json> result_;
    public:
        filter_selector(const jsonpath_filter_expr<Json>& result)
            : result_(result)
        {
        }

        bool is_filter() const override
        {
            return true;
        }

        void select(jsonpath_evaluator&,
                    const string_type& path, reference val, 
                    node_set& nodes) override
        {
            //std::cout << "filter_selector select ";
            if (val.is_array())
            {
                //std::cout << "from array \n";
                for (size_t i = 0; i < val.size(); ++i)
                {
                    if (result_.exists(val[i]))
                    {
                        nodes.emplace_back(PathCons()(path,i),std::addressof(val[i]));
                    }
                }
            }
            else if (val.is_object())
            {
                //std::cout << "from object \n";
                if (result_.exists(val))
                {
                    nodes.emplace_back(path, std::addressof(val));
                }
            }
        }
    };

    class name_selector final : public selector_base
    {
    private:
        string_type name_;
    public:
        name_selector(const string_view_type& name)
            : name_(name)
        {
        }

        void select(jsonpath_evaluator& evaluator,
                    const string_type& path, reference val,
                    node_set& nodes) override
        {
            bool is_start_positive = true;

            if (val.is_object() && val.contains(name_))
            {
                nodes.emplace_back(PathCons()(path,name_),std::addressof(val.at(name_)));
            }
            else if (val.is_array())
            {
                size_t pos = 0;
                if (try_string_to_index(name_.data(), name_.size(), &pos, &is_start_positive))
                {
                    size_t index = is_start_positive ? pos : val.size() - pos;
                    if (index < val.size())
                    {
                        nodes.emplace_back(PathCons()(path,index),std::addressof(val[index]));
                    }
                }
                else if (name_ == length_literal<char_type>() && val.size() > 0)
                {
                    pointer ptr = evaluator.create_temp(val.size());
                    nodes.emplace_back(PathCons()(path, name_), ptr);
                }
            }
            else if (val.is_string())
            {
                size_t pos = 0;
                string_view_type sv = val.as_string_view();
                if (try_string_to_index(name_.data(), name_.size(), &pos, &is_start_positive))
                {
                    size_t index = is_start_positive ? pos : sv.size() - pos;
                    auto sequence = unicons::sequence_at(sv.data(), sv.data() + sv.size(), index);
                    if (sequence.length() > 0)
                    {
                        pointer ptr = evaluator.create_temp(sequence.begin(),sequence.length());
                        nodes.emplace_back(PathCons()(path, index), ptr);
                    }
                }
                else if (name_ == length_literal<char_type>() && sv.size() > 0)
                {
                    size_t count = unicons::u32_length(sv.begin(),sv.end());
                    pointer ptr = evaluator.create_temp(count);
                    nodes.emplace_back(PathCons()(path, name_), ptr);
                }
            }
        }
    };

    class array_slice_selector final : public selector_base
    {
    private:
        array_slice slice_;
    public:
        array_slice_selector(const array_slice& slice)
            : slice_(slice) 
        {
        }

        void select(jsonpath_evaluator&,
                    const string_type& path, reference val,
                    node_set& nodes) override
        {
            if (slice_.is_step_positive)
            {
                end_array_slice1(path, val, nodes);
            }
            else
            {
                end_array_slice2(path, val, nodes);
            }
        }

        void end_array_slice1(const string_type& path, reference val, node_set& nodes)
        {
            if (val.is_array())
            {
                size_t start = slice_.get_start(val.size());
                size_t end = slice_.get_end(val.size());
                for (size_t j = start; j < end; j += slice_.step())
                {
                    if (j < val.size())
                    {
                        nodes.emplace_back(PathCons()(path,j),std::addressof(val[j]));
                    }
                }
            }
        }

        void end_array_slice2(const string_type& path, reference val, node_set& nodes)
        {
            if (val.is_array())
            {
                size_t start = slice_.get_start(val.size());
                size_t end = slice_.get_end(val.size());

                size_t j = end + slice_.step() - 1;
                while (j > (start+slice_.step()-1))
                {
                    j -= slice_.step();
                    if (j < val.size())
                    {
                        nodes.emplace_back(PathCons()(path,j),std::addressof(val[j]));
                    }
                }
            }
        }
    };

    function_table<Json,pointer> functions_;

    default_parse_error_handler default_err_handler_;
    node_set nodes_;
    std::vector<node_set> stack_;
    size_t line_;
    size_t column_;
    const char_type* begin_input_;
    const char_type* end_input_;
    const char_type* p_;
    std::vector<std::unique_ptr<selector_base>> selectors_;
    std::vector<std::unique_ptr<Json>> temp_json_values_;

    typedef std::vector<pointer> argument_type;
    std::vector<argument_type> function_stack_;
    std::vector<state_item> state_stack_;

public:
    jsonpath_evaluator()
        : line_(1), column_(1),
          begin_input_(nullptr), end_input_(nullptr),
          p_(nullptr)
    {
    }

    jsonpath_evaluator(size_t line, size_t column)
        : line_(line), column_(column),
          begin_input_(nullptr), end_input_(nullptr),
          p_(nullptr)
    {
    }

    size_t line() const
    {
        return line_;
    }

    size_t column() const
    {
        return column_;
    }

    Json get_values() const
    {
        Json result = typename Json::array();

        if (stack_.size() > 0)
        {
            result.reserve(stack_.back().size());
            for (const auto& p : stack_.back())
            {
                result.push_back(*(p.val_ptr));
            }
        }
        return result;
    }

    std::vector<pointer> get_pointers() const
    {
        std::vector<pointer> result;

        if (stack_.size() > 0)
        {
            result.reserve(stack_.back().size());
            for (const auto& p : stack_.back())
            {
                result.push_back(p.val_ptr);
            }
        }
        return result;
    }

    void call_function(const string_type& function_name, std::error_code& ec)
    {
        auto f = functions_.get(function_name, ec);
        if (ec)
        {
            return;
        }
        auto result = f(function_stack_, ec);
        if (ec)
        {
            return;
        }

        string_type s = {'$'};
        node_set v;
        pointer ptr = create_temp(std::move(result));
        v.emplace_back(s,ptr);
        stack_.push_back(v);
    }

    template <typename... Args>
    pointer create_temp(Args&& ... args)
    {
        auto temp = make_unique_ptr<Json>(std::forward<Args>(args)...);
        pointer ptr = temp.get();
        temp_json_values_.emplace_back(std::move(temp));
        return ptr;
    }

    Json get_normalized_paths() const
    {
        Json result = typename Json::array();
        if (stack_.size() > 0)
        {
            result.reserve(stack_.back().size());
            for (const auto& p : stack_.back())
            {
                result.push_back(p.path);
            }
        }
        return result;
    }

    template <class T>
    void replace(T&& new_value)
    {
        if (stack_.size() > 0)
        {
            for (size_t i = 0; i < stack_.back().size(); ++i)
            {
                *(stack_.back()[i].val_ptr) = new_value;
            }
        }
    }

    void evaluate(reference root, const string_view_type& path)
    {
        std::error_code ec;
        evaluate(root, path.data(), path.length(), ec);
        if (ec)
        {
            throw jsonpath_error(ec, line_, column_);
        }
    }

    void evaluate(reference root, const string_view_type& path, std::error_code& ec)
    {
        try
        {
            evaluate(root, path.data(), path.length(), ec);
        }
        catch (...)
        {
            ec = jsonpath_errc::unidentified_error;
        }
    }
 
    void evaluate(reference root, 
                  const char_type* path, 
                  size_t length,
                  std::error_code& ec)
    {
        state_stack_.emplace_back(path_state::start);

        string_type function_name;
        string_type buffer;

        begin_input_ = path;
        end_input_ = path + length;
        p_ = begin_input_;

        string_type s = {'$'};
        node_set v;
        v.emplace_back(std::move(s),std::addressof(root));
        stack_.push_back(v);

        array_slice slice;

        while (p_ < end_input_)
        {
            switch (state_stack_.back().state)
            {
                case path_state::start: 
                {
                    switch (*p_)
                    {
                        case ' ':case '\t':case '\r':case '\n':
                            advance_past_space_character();
                            break;
                        case '$':
                        {
                            state_stack_.emplace_back(path_state::dot_or_left_bracket, state_stack_.back());
                            ++p_;
                            ++column_;
                            break;
                        }
                        default:
                        {
                            switch (*p_)
                            {
                                case '.':
                                case '[':
                                    ec = jsonpath_errc::expected_root;
                                    return;
                                default: // might be function, validate name later
                                    state_stack_.emplace_back(path_state::dot_or_left_bracket, state_stack_.back());
                                    state_stack_.emplace_back(path_state::path_or_function_name, state_stack_.back());
                                    break;
                            }
                            break;
                        }

                        return;
                    };
                    break;
                }
                case path_state::path_or_function_name:
                    switch (*p_)
                    {
                        case ' ':case '\t':case '\r':case '\n':
                        {
                            selectors_.push_back(make_unique_ptr<name_selector>(buffer));
                            apply_selectors();
                            buffer.clear();
                            state_stack_.pop_back();
                            advance_past_space_character();
                            break;
                        }
                        case '(':
                            state_stack_.back().state = path_state::arg_or_right_paren;
                            function_name = std::move(buffer);
                            buffer.clear();
                            ++p_;
                            ++column_;
                            break;
                        case '[':
                        {
                            if (buffer.size() > 0)
                            {
                                selectors_.push_back(make_unique_ptr<name_selector>(buffer));
                                apply_selectors();
                                buffer.clear();
                            }
                            slice.start_ = 0;

                            state_stack_.back().state = path_state::wildcard_or_rpath_or_slice_or_filter;
                            ++p_;
                            ++column_;
                            break;
                        }
                        case '.':
                        {
                            if (buffer.size() > 0)
                            {
                                selectors_.push_back(make_unique_ptr<name_selector>(buffer));
                                apply_selectors();
                                buffer.clear();
                            }
                            state_stack_.back().state = path_state::dot;
                            ++p_;
                            ++column_;
                            break;
                        }
                        case '*':
                        {
                            end_all();
                            transfer_nodes();
                            state_stack_.back().state = path_state::dot;
                            ++p_;
                            ++column_;
                            break;
                        }
                        case '\'':
                        {
                            buffer.clear();
                            state_stack_.back().state = path_state::single_quoted_name;
                            ++p_;
                            ++column_;
                            break;
                        }
                        case '\"':
                        {
                            buffer.clear();
                            state_stack_.back().state = path_state::double_quoted_name;
                            ++p_;
                            ++column_;
                            break;
                        }
                        default:
                            buffer.push_back(*p_);
                            ++p_;
                            ++column_;
                            break;
                    }
                    break;
                case path_state::arg_or_right_paren:
                    switch (*p_)
                    {
                        case ' ':case '\t':case '\r':case '\n':
                            advance_past_space_character();
                            break;
                        case '$':
                            buffer.clear();
                            buffer.push_back(*p_);
                            state_stack_.emplace_back(path_state::path_argument, state_stack_.back());
                            ++p_;
                            ++column_;
                            break;
                        case '\'':
                            buffer.clear();
                            buffer.push_back('\"');
                            state_stack_.back().state = path_state::more_args_or_right_paren;
                            state_stack_.emplace_back(path_state::single_quoted_arg, state_stack_.back());
                            ++p_;
                            ++column_;
                            break;
                        case '\"':
                            buffer.clear();
                            buffer.push_back('\"');
                            state_stack_.back().state = path_state::more_args_or_right_paren;
                            state_stack_.emplace_back(path_state::double_quoted_arg, state_stack_.back());
                            ++p_;
                            ++column_;
                            break;
                        case ')':
                        {
                            jsonpath_evaluator<Json,JsonReference,PathCons> evaluator;
                            evaluator.evaluate(root, buffer, ec);
                            if (ec)
                            {
                                return;
                            }
                            function_stack_.push_back(evaluator.get_pointers());

                            call_function(function_name, ec);
                            if (ec)
                            {
                                return;
                            }
                            state_stack_.pop_back();
                            ++p_;
                            ++column_;
                            break;
                        }
                        default:
                            buffer.clear();
                            state_stack_.back().state = path_state::more_args_or_right_paren;
                            state_stack_.emplace_back(path_state::unquoted_arg, state_stack_.back());
                            ++p_;
                            ++column_;
                            break;
                    }
                    break;
                case path_state::path_argument:
                    switch (*p_)
                    {
                        case ',':
                        {
                            jsonpath_evaluator<Json, JsonReference, PathCons> evaluator;
                            evaluator.evaluate(root, buffer, ec);
                            if (ec)
                            {
                                return;
                            }
                            function_stack_.push_back(evaluator.get_pointers());
                            state_stack_.pop_back();
                            ++p_;
                            ++column_;
                            break;
                        }
                        case ')':
                        {
                            state_stack_.pop_back();
                            break;
                        }
                        default:
                            buffer.push_back(*p_); // path_argument
                            ++p_;
                            ++column_;
                            break;
                    }
                    break;
                case path_state::unquoted_arg:
                    switch (*p_)
                    {
                        case ',':
                            try
                            {
                                auto val = Json::parse(buffer);
                                auto temp = create_temp(val);
                                function_stack_.push_back(std::vector<pointer>{temp});
                            }
                            catch (const ser_error&)
                            {
                                ec = jsonpath_errc::argument_parse_error;
                                return;
                            }
                            buffer.clear();
                            //state_ = path_state::arg_or_right_paren;
                            state_stack_.pop_back();
                            break;
                        case ')':
                        {
                            try
                            {
                                auto val = Json::parse(buffer);
                                auto temp = create_temp(val);
                                function_stack_.push_back(std::vector<pointer>{temp});
                            }
                            catch (const ser_error&)
                            {
                                ec = jsonpath_errc::argument_parse_error;
                                return;
                            }
                            call_function(function_name, ec);
                            if (ec)
                            {
                                return;
                            }
                            state_stack_.pop_back();
                            break;
                        }
                        default:
                            buffer.push_back(*p_);
                            break;
                    }
                    ++p_;
                    ++column_;
                    break;
                case path_state::single_quoted_arg:
                    switch (*p_)
                    {
                        case '\'':
                            buffer.push_back('\"');
                            state_stack_.pop_back();
                            break;
                        case '\"':
                            buffer.push_back('\\');
                            buffer.push_back('\"');
                            state_stack_.pop_back();
                            break;
                        default:
                            buffer.push_back(*p_);
                            break;
                    }
                    ++p_;
                    ++column_;
                    break;
                case path_state::double_quoted_arg:
                    switch (*p_)
                    {
                        case '\"':
                            buffer.push_back('\"');
                            state_stack_.pop_back();
                            break;
                        default:
                            buffer.push_back(*p_);
                            break;
                    }
                    ++p_;
                    ++column_;
                    break;
                case path_state::more_args_or_right_paren:
                    switch (*p_)
                    {
                        case ' ':case '\t':case '\r':case '\n':
                            advance_past_space_character();
                            break;
                        case ',':
                            try
                            {
                                auto val = Json::parse(buffer);
                                auto temp = create_temp(val);
                                function_stack_.push_back(std::vector<pointer>{temp});
                            }
                            catch (const ser_error&)
                            {
                                ec = jsonpath_errc::argument_parse_error;
                                return;
                            }
                            buffer.clear();
                            //state_ = path_state::arg_or_right_paren;
                            state_stack_.pop_back();
                            ++p_;
                            ++column_;
                            break;
                        case ')':
                        {
                            try
                            {
                                auto val = Json::parse(buffer);
                                auto temp = create_temp(val);
                                function_stack_.push_back(std::vector<pointer>{temp});
                            }
                            catch (const ser_error&)
                            {
                                ec = jsonpath_errc::argument_parse_error;
                                return;
                            }
                            call_function(function_name, ec);
                            if (ec)
                            {
                                return;
                            }
                            state_stack_.pop_back();
                            ++p_;
                            ++column_;
                            break;
                        }
                        default:
                            ec = jsonpath_errc::invalid_filter_unsupported_operator;
                            return;
                    }
                    break;
                case path_state::dot:
                    switch (*p_)
                    {
                        case '.':
                            state_stack_.back().is_recursive_descent = true;
                            ++p_;
                            ++column_;
                            state_stack_.back().state = path_state::name_or_left_bracket;
                            break;
                        default:
                            state_stack_.back().state = path_state::name;
                            break;
                    }
                    break;
                case path_state::name_or_left_bracket: 
                    switch (*p_)
                    {
                        case ' ':case '\t':case '\r':case '\n':
                            advance_past_space_character();
                            break;
                        case '[': // [ can follow ..
                            state_stack_.back().state = path_state::wildcard_or_rpath_or_slice_or_filter;
                            ++p_;
                            ++column_;
                            break;
                        default:
                            buffer.clear();
                            state_stack_.back().state = path_state::name;
                            break;
                    }
                    break;
                case path_state::name: 
                    switch (*p_)
                    {
                        case ' ':case '\t':case '\r':case '\n':
                            advance_past_space_character();
                            break;
                        case '*':
                            end_all();
                            transfer_nodes();
                            state_stack_.pop_back();
                            ++p_;
                            ++column_;
                            break;
                        case '\'':
                            state_stack_.back().state = path_state::single_quoted_name;
                            ++p_;
                            ++column_;
                            break;
                        case '\"':
                            state_stack_.back().state = path_state::double_quoted_name;
                            ++p_;
                            ++column_;
                            break;
                        case '[': 
                        case '.':
                            ec = jsonpath_errc::expected_name;
                            return;
                        default:
                            buffer.clear();
                            state_stack_.back().state = path_state::unquoted_name;
                            break;
                    }
                    break;
                case path_state::dot_or_left_bracket: 
                    switch (*p_)
                    {
                        case ' ':case '\t':case '\r':case '\n':
                            advance_past_space_character();
                            break;
                        case '.':
                            state_stack_.emplace_back(path_state::dot, state_stack_.back());
                            ++p_;
                            ++column_;
                            break;
                        case '[':
                            state_stack_.emplace_back(path_state::wildcard_or_rpath_or_slice_or_filter, state_stack_.back());
                            ++p_;
                            ++column_;
                            break;
                        default:
                            ec = jsonpath_errc::expected_separator;
                            return;
                    };
                    break;
                case path_state::unquoted_name: 
                    switch (*p_)
                    {
                        case '[':
                        case '.':
                        case ' ':case '\t':
                        case '\r':
                        case '\n':
                            state_stack_.back().state = path_state::unquoted_name2;
                            break;
                        default:
                            buffer.push_back(*p_);
                            ++p_;
                            ++column_;
                            break;
                    };
                    break;
                case path_state::unquoted_name2: 
                    switch (*p_)
                    {
                        case ' ':case '\t':case '\r':case '\n':
                            advance_past_space_character();
                            break;
                        case '[':
                            selectors_.push_back(make_unique_ptr<name_selector>(buffer));
                            apply_selectors();
                            buffer.clear();
                            slice.start_ = 0;
                            state_stack_.pop_back();
                            break;
                        case '.':
                            selectors_.push_back(make_unique_ptr<name_selector>(buffer));
                            apply_selectors();
                            buffer.clear();
                            state_stack_.pop_back();
                            break;
                        default:
                            ec = jsonpath_errc::expected_name;
                            return;
                    };
                    break;
                case path_state::single_quoted_name:
                    switch (*p_)
                    {
                        case '\'':
                            selectors_.push_back(make_unique_ptr<name_selector>(buffer));
                            apply_selectors();
                            buffer.clear();
                            state_stack_.pop_back();
                            break;
                        case '\\':
                            if (p_+1 < end_input_)
                            {
                                ++p_;
                                ++column_;
                                buffer.push_back(*p_);
                            }
                            else
                            {
                                ec = jsonpath_errc::unexpected_end_of_input;
                                return;
                            }
                            break;
                        default:
                            buffer.push_back(*p_);
                            break;
                    };
                    ++p_;
                    ++column_;
                    break;
                case path_state::double_quoted_name: 
                    switch (*p_)
                    {
                        case '\"':
                            selectors_.push_back(make_unique_ptr<name_selector>(buffer));
                            apply_selectors();
                            buffer.clear();
                            state_stack_.pop_back();
                            break;
                        case '\\':
                            if (p_+1 < end_input_)
                            {
                                ++p_;
                                ++column_;
                                buffer.push_back(*p_);
                            }
                            else
                            {
                                ec = jsonpath_errc::unexpected_end_of_input;
                                return;
                            }
                            break;
                        default:
                            buffer.push_back(*p_);
                            break;
                    };
                    ++p_;
                    ++column_;
                    break;
                case path_state::comma_or_right_bracket:
                    switch (*p_)
                    {
                        case ' ':case '\t':case '\r':case '\n':
                            advance_past_space_character();
                            break;
                        case ',':
                            state_stack_.back().is_union = true;
                            state_stack_.back().state = path_state::wildcard_or_rpath_or_slice_or_filter;
                            ++p_;
                            ++column_;
                            break;
                        case ']':
                            apply_selectors();
                            state_stack_.pop_back();
                            ++p_;
                            ++column_;
                            break;
                        default:
                            ec = jsonpath_errc::expected_right_bracket;
                            return;
                    }
                    break;
                case path_state::wildcard_or_rpath_or_slice_or_filter:
                    switch (*p_)
                    {
                        case ' ':case '\t':case '\r':case '\n':
                            advance_past_space_character();
                            break;
                        case '(':
                        {
                            jsonpath_filter_parser<Json> parser(line_,column_);
                            auto result = parser.parse(root, p_,end_input_,&p_);
                            line_ = parser.line();
                            column_ = parser.column();
                            selectors_.push_back(make_unique_ptr<expr_selector>(result));
                            state_stack_.back().state = path_state::comma_or_right_bracket;
                            break;
                        }
                        case '?':
                        {
                            jsonpath_filter_parser<Json> parser(line_,column_);
                            auto result = parser.parse(root,p_,end_input_,&p_);
                            line_ = parser.line();
                            column_ = parser.column();
                            selectors_.push_back(make_unique_ptr<filter_selector>(result));
                            state_stack_.back().state = path_state::comma_or_right_bracket;
                            break;                   
                        }
                        case ':':
                            slice = array_slice();
                            buffer.clear();
                            state_stack_.back().state = path_state::comma_or_right_bracket;
                            state_stack_.emplace_back(path_state::slice_end_or_end_step, state_stack_.back());
                            ++p_;
                            ++column_;
                            break;
                        case '*':
                            state_stack_.back().state = path_state::comma_or_right_bracket;
                            state_stack_.emplace_back(path_state::bracketed_wildcard_or_path, state_stack_.back());
                            ++p_;
                            ++column_;
                            break;
                        case '\'':
                            state_stack_.back().state = path_state::comma_or_right_bracket;
                            state_stack_.emplace_back(path_state::bracketed_single_quoted_name, state_stack_.back());
                            ++p_;
                            ++column_;
                            break;
                        case '\"':
                            state_stack_.back().state = path_state::comma_or_right_bracket;
                            state_stack_.emplace_back(path_state::bracketed_double_quoted_name, state_stack_.back());
                            ++p_;
                            ++column_;
                            break;
                        default:
                            slice = array_slice();
                            buffer.clear();
                            buffer.push_back(*p_);
                            state_stack_.back().state = path_state::comma_or_right_bracket;
                            state_stack_.emplace_back(path_state::bracketed_unquoted_name, state_stack_.back());
                            ++p_;
                            ++column_;
                            break;
                    }
                    break;
                case path_state::bracketed_unquoted_name:
                    switch (*p_)
                    {
                        case ':':
                            if (!try_string_to_index(buffer.data(), buffer.size(), &slice.start_, &slice.is_start_positive))
                            {
                                ec = jsonpath_errc::expected_slice_start;
                                return;
                            }
                            state_stack_.back().state = path_state::slice_end_or_end_step;
                            ++p_;
                            ++column_;
                            break;
                        case '.':
                        case '[':
                        case ',': 
                        case ']': 
                            state_stack_.back().state = path_state::bracketed_name_or_path;
                            break;
                        default:
                            buffer.push_back(*p_);
                            ++p_;
                            ++column_;
                            break;
                    }
                    break;
                case path_state::bracketed_name_or_path:
                    switch (*p_)
                    {
                        case ' ':case '\t':case '\r':case '\n':
                            advance_past_space_character();
                            break;
                        case '.':
                            buffer.push_back(*p_);
                            state_stack_.back().state = path_state::path;
                            ++p_;
                            ++column_;
                            break;
                        case '[':
                            buffer.push_back(*p_);
                            state_stack_.back().state = path_state::path2;
                            ++p_;
                            ++column_;
                            break;
                        case ',': 
                        case ']': 
                            if (!buffer.empty())
                            {
                                selectors_.push_back(make_unique_ptr<name_selector>(buffer));
                                buffer.clear();
                            }
                            state_stack_.pop_back();
                            break;
                        default:
                            ec = jsonpath_errc::expected_right_bracket;
                            return;
                    }
                    break;
                case path_state::bracketed_wildcard_or_path:
                    switch (*p_)
                    {
                        case ' ':case '\t':case '\r':case '\n':
                            advance_past_space_character();
                            break;
                        case '.':
                            buffer.push_back('*');
                            buffer.push_back(*p_);
                            state_stack_.back().state = path_state::path;
                            ++p_;
                            ++column_;
                            break;
                        case '[':
                            buffer.push_back('*');
                            buffer.push_back(*p_);
                            state_stack_.back().state = path_state::path2;
                            ++p_;
                            ++column_;
                            break;
                        case ',': 
                        case ']': 
                            end_all();
                            state_stack_.pop_back();
                            break;
                        default:
                            ec = jsonpath_errc::expected_right_bracket;
                            return;
                    }
                    break;
                case path_state::path:
                    switch (*p_)
                    {
                        case '\'':
                            buffer.push_back(*p_);
                            state_stack_.emplace_back(path_state::path_single_quoted, state_stack_.back());
                            ++p_;
                            ++column_;
                            break;
                        case '\"':
                            buffer.push_back(*p_);
                            state_stack_.emplace_back(path_state::path_double_quoted, state_stack_.back());
                            ++p_;
                            ++column_;
                            break;
                        case ',': 
                        case ']': 
                            if (!buffer.empty())
                            {
                                selectors_.push_back(make_unique_ptr<path_selector>(buffer));
                                buffer.clear();
                            }
                            state_stack_.pop_back();
                            break;
                        default:
                            buffer.push_back(*p_);
                            ++p_;
                            ++column_;
                            break;
                    }
                    break;
                case path_state::path_double_quoted:
                    switch (*p_)
                    {
                        case '\"': 
                            buffer.push_back(*p_);
                            state_stack_.pop_back();
                            break;
                        case '\\':
                            if (p_+1 < end_input_)
                            {
                                ++p_;
                                ++column_;
                                buffer.push_back(*p_);
                            }
                            else
                            {
                                ec = jsonpath_errc::unexpected_end_of_input;
                                return;
                            }
                            break;
                        default:
                            buffer.push_back(*p_);
                            break;
                    }
                    ++p_;
                    ++column_;
                    break;
                case path_state::path_single_quoted:
                    switch (*p_)
                    {
                        case '\'': 
                            buffer.push_back(*p_);
                            state_stack_.pop_back();
                            break;
                        case '\\':
                            if (p_+1 < end_input_)
                            {
                                ++p_;
                                ++column_;
                                buffer.push_back(*p_);
                            }
                            else
                            {
                                ec = jsonpath_errc::unexpected_end_of_input;
                                return;
                            }
                            break;
                        default:
                            buffer.push_back(*p_);
                            break;
                    }
                    ++p_;
                    ++column_;
                    break;
                case path_state::path2:
                    switch (*p_)
                    {
                        case ']': 
                            buffer.push_back(*p_);
                            state_stack_.back().state = path_state::path;
                            break;
                        default:
                            buffer.push_back(*p_);
                            break;
                    }
                    ++p_;
                    ++column_;
                    break;
                case path_state::slice_end_or_end_step:
                    switch (*p_)
                    {
                        case '-':
                            slice.is_end_positive = false;
                            state_stack_.back().state = path_state::slice_end;
                            ++p_;
                            ++column_;
                            break;
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8':case '9':
                            slice.is_end_defined = true;
                            slice.end_ = static_cast<size_t>(*p_-'0');
                            state_stack_.back().state = path_state::slice_end;
                            ++p_;
                            ++column_;
                            break;
                        case ':':
                            slice.step_ = 0;
                            state_stack_.back().state = path_state::slice_step;
                            ++p_;
                            ++column_;
                            break;
                        case ',':
                        case ']':
                            selectors_.push_back(make_unique_ptr<array_slice_selector>(slice));
                            state_stack_.pop_back();
                            break;
                        default:
                            ec = jsonpath_errc::expected_minus_or_digit_or_colon_or_comma_or_right_bracket;
                            return;
                    }
                    break;
                case path_state::slice_end:
                    switch (*p_)
                    {
                        case ':':
                            slice.step_ = 0;
                            state_stack_.back().state = path_state::slice_step;
                            ++p_;
                            ++column_;
                            break;
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8':case '9':
                            slice.is_end_defined = true;
                            slice.end_ = slice.end_*10 + static_cast<size_t>(*p_-'0');
                            ++p_;
                            ++column_;
                            break;
                        case ',':
                        case ']':
                            if (!slice.is_end_defined)
                            {
                                ec = jsonpath_errc::expected_slice_end;
                                return;
                            }
                            selectors_.push_back(make_unique_ptr<array_slice_selector>(slice));
                            state_stack_.pop_back();
                            break;
                        default:
                            ec = jsonpath_errc::expected_digit_or_colon_or_comma_or_right_bracket;
                            return;
                    }
                    break;
                case path_state::slice_step:
                    switch (*p_)
                    {
                        case '-':
                            slice.is_step_positive = false;
                            slice.step_ = 0;
                            state_stack_.back().state = path_state::slice_step2;
                            ++p_;
                            ++column_;
                            break;
                        default:
                            slice.step_ = 0;
                            state_stack_.back().state = path_state::slice_step2;
                            break;
                    }
                    break;
                case path_state::slice_step2:
                    switch (*p_)
                    {
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8':case '9':
                            slice.step_ = slice.step_*10 + static_cast<size_t>(*p_-'0');
                            ++p_;
                            ++column_;
                            break;
                        case ',':
                        case ']':
                            if (slice.step_ == 0)
                            {
                                ec = jsonpath_errc::expected_slice_step;
                                return;
                            }
                            selectors_.push_back(make_unique_ptr<array_slice_selector>(slice));
                            state_stack_.pop_back();
                            break;
                        default:
                            ec = jsonpath_errc::expected_minus_or_digit_or_comma_or_right_bracket;
                            return;
                    }
                    break;
                case path_state::bracketed_single_quoted_name:
                    switch (*p_)
                    {
                        case '\'':
                            state_stack_.back().state = path_state::bracketed_name_or_path;
                            break;
                        case '\\':
                            if (p_+1 < end_input_)
                            {
                                ++p_;
                                ++column_;
                                buffer.push_back(*p_);
                            }
                            else
                            {
                                ec = jsonpath_errc::unexpected_end_of_input;
                                return;
                            }
                            break;
                        default:
                            buffer.push_back(*p_);
                            break;
                    };
                    ++p_;
                    ++column_;
                    break;
                case path_state::bracketed_double_quoted_name: 
                    switch (*p_)
                    {
                        case '\"':
                            state_stack_.back().state = path_state::bracketed_name_or_path;
                            break;
                        case '\\':
                            if (p_+1 < end_input_)
                            {
                                ++p_;
                                ++column_;
                                buffer.push_back(*p_);
                            }
                            else
                            {
                                ec = jsonpath_errc::unexpected_end_of_input;
                                return;
                            }
                            break;
                        default:
                            buffer.push_back(*p_);
                            break;
                    };
                    ++p_;
                    ++column_;
                    break;
                default:
                    ++p_;
                    ++column_;
                    break;
            }
        }

        switch (state_stack_.back().state)
        {
            case path_state::unquoted_name: 
            case path_state::unquoted_name2: 
            {
                selectors_.push_back(make_unique_ptr<name_selector>(buffer));
                apply_selectors();
                buffer.clear();
                state_stack_.pop_back(); // unquoted_name
                break;
            }
            default:
                break;
        }

        if (state_stack_.size() > 2)
        {
            ec = jsonpath_errc::unexpected_end_of_input;
            return;
        }

        JSONCONS_ASSERT(state_stack_.size() == 2);
        state_stack_.pop_back(); 

        JSONCONS_ASSERT(state_stack_.back().state == path_state::start);
        state_stack_.pop_back();
    }

    void end_all()
    {
        for (const auto& node : stack_.back())
        {
            const auto& path = node.path;
            pointer p = node.val_ptr;

            if (p->is_array())
            {
                for (auto it = p->array_range().begin(); it != p->array_range().end(); ++it)
                {
                    nodes_.emplace_back(PathCons()(path,it - p->array_range().begin()),std::addressof(*it));
                }
            }
            else if (p->is_object())
            {
                for (auto it = p->object_range().begin(); it != p->object_range().end(); ++it)
                {
                    nodes_.emplace_back(PathCons()(path,it->key()),std::addressof(it->value()));
                }
            }

        }
    }

    void apply_selectors()
    {
        //std::cout << "apply_selectors count: " << selectors_.size() << "\n";
        if (selectors_.size() > 0)
        {
            for (auto& node : stack_.back())
            {
                //std::cout << "apply selector to:\n" << pretty_print(*(node.val_ptr)) << "\n";
                for (auto& selector : selectors_)
                {
                    apply_selector(node.path, *(node.val_ptr), *selector, true);
                }
            }
            selectors_.clear();
        }
        transfer_nodes();
    }

    void apply_selector(const string_type& path, reference val, selector_base& selector, bool process)
    {
        if (process)
        {
            selector.select(*this, path, val, nodes_);
        }
        //std::cout << "*it: " << val << "\n";
        //std::cout << "apply_selectors 1 done\n";
        if (state_stack_.back().is_recursive_descent)
        {
            //std::cout << "is_recursive_descent\n";
            if (val.is_object())
            {
                //std::cout << "is_object\n";
                for (auto& nvp : val.object_range())
                {
                    if (nvp.value().is_array() || nvp.value().is_object())
                    {                        
                        apply_selector(PathCons()(path,nvp.key()), nvp.value(), selector, true);
                    } 
                }
            }
            else if (val.is_array())
            {
                //std::cout << "is_array\n";
                auto first = val.array_range().begin();
                auto last = val.array_range().end();
                for (auto it = first; it != last; ++it)
                {
                    if (it->is_array())
                    {
                        apply_selector(PathCons()(path,it - first), *it,selector, true);
                        //std::cout << "*it: " << *it << "\n";
                    }
                    else if (it->is_object())
                    {
                        apply_selector(PathCons()(path,it - first), *it, selector, !selector.is_filter());
                    }
                }
            }
        }
    }

    void transfer_nodes()
    {
        if (state_stack_.back().is_union)
        {
            std::set<node_type, node_less> temp(nodes_.begin(), nodes_.end());
            stack_.push_back(std::vector<node_type>(temp.begin(),temp.end()));
        }
        else
        {
            stack_.push_back(std::move(nodes_));
        }
        nodes_.clear();
        state_stack_.back().is_recursive_descent = false;
        state_stack_.back().is_union = false;
    }

    void advance_past_space_character()
    {
        switch (*p_)
        {
            case ' ':case '\t':
                ++p_;
                ++column_;
                break;
            case '\r':
                if (p_+1 < end_input_ && *(p_+1) == '\n')
                    ++p_;
                ++line_;
                column_ = 1;
                ++p_;
                break;
            case '\n':
                ++line_;
                column_ = 1;
                ++p_;
                break;
            default:
                break;
        }
    }
};

}

}}

#endif
