// Copyright 2013 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_JSON_FILTER_HPP
#define JSONCONS_JSON_FILTER_HPP

#include <string>

#include <jsoncons/json_content_handler.hpp>
#include <jsoncons/parse_error_handler.hpp>

namespace jsoncons {

template <class CharT>
class basic_json_filter : public basic_json_content_handler<CharT>
{
public:
    using typename basic_json_content_handler<CharT>::string_view_type;
private:
    basic_json_content_handler<CharT>& to_handler_;

    // noncopyable and nonmoveable
    basic_json_filter<CharT>(const basic_json_filter<CharT>&) = delete;
    basic_json_filter<CharT>& operator=(const basic_json_filter<CharT>&) = delete;
public:
    basic_json_filter(basic_json_content_handler<CharT>& handler)
        : to_handler_(handler)
    {
    }

#if !defined(JSONCONS_NO_DEPRECATED)
    basic_json_content_handler<CharT>& input_handler()
    {
        return to_handler_;
    }

    basic_json_content_handler<CharT>& downstream_handler()
    {
        return to_handler_;
    }

    basic_json_content_handler<CharT>& destination_handler()
    {
        return to_handler_;
    }
#endif

    basic_json_content_handler<CharT>& to_handler()
    {
        return to_handler_;
    }

private:
    void do_flush() override
    {
        to_handler_.flush();
    }

    bool do_begin_object(semantic_tag tag, const ser_context& context) override
    {
        return to_handler_.begin_object(tag, context);
    }

    bool do_begin_object(size_t length, semantic_tag tag, const ser_context& context) override
    {
        return to_handler_.begin_object(length, tag, context);
    }

    bool do_end_object(const ser_context& context) override
    {
        return to_handler_.end_object(context);
    }

    bool do_begin_array(semantic_tag tag, const ser_context& context) override
    {
        return to_handler_.begin_array(tag, context);
    }

    bool do_begin_array(size_t length, semantic_tag tag, const ser_context& context) override
    {
        return to_handler_.begin_array(length, tag, context);
    }

    bool do_end_array(const ser_context& context) override
    {
        return to_handler_.end_array(context);
    }

    bool do_name(const string_view_type& name,
                 const ser_context& context) override
    {
        return to_handler_.name(name, context);
    }

    bool do_string_value(const string_view_type& value,
                         semantic_tag tag,
                         const ser_context& context) override
    {
        return to_handler_.string_value(value, tag, context);
    }

    bool do_byte_string_value(const byte_string_view& b, 
                              semantic_tag tag,
                              const ser_context& context) override
    {
        return to_handler_.byte_string_value(b, tag, context);
    }

    bool do_double_value(double value, 
                         semantic_tag tag,
                         const ser_context& context) override
    {
        return to_handler_.double_value(value, tag, context);
    }

    bool do_int64_value(int64_t value,
                        semantic_tag tag,
                        const ser_context& context) override
    {
        return to_handler_.int64_value(value, tag, context);
    }

    bool do_uint64_value(uint64_t value,
                         semantic_tag tag,
                         const ser_context& context) override
    {
        return to_handler_.uint64_value(value, tag, context);
    }

    bool do_bool_value(bool value, semantic_tag tag, const ser_context& context) override
    {
        return to_handler_.bool_value(value, tag, context);
    }

    bool do_null_value(semantic_tag tag, const ser_context& context) override
    {
        return to_handler_.null_value(tag, context);
    }

};

// Filters out begin_document and end_document events
template <class CharT>
class basic_json_fragment_filter : public basic_json_filter<CharT>
{
public:
    using typename basic_json_filter<CharT>::string_view_type;

    basic_json_fragment_filter(basic_json_content_handler<CharT>& handler)
        : basic_json_filter<CharT>(handler)
    {
    }
private:
    void do_flush() override
    {
    }
};

template <class CharT>
class basic_rename_object_member_filter : public basic_json_filter<CharT>
{
public:
    using typename basic_json_filter<CharT>::string_view_type;

private:
    std::basic_string<CharT> name_;
    std::basic_string<CharT> new_name_;
public:
    basic_rename_object_member_filter(const std::basic_string<CharT>& name,
                             const std::basic_string<CharT>& new_name,
                             basic_json_content_handler<CharT>& handler)
        : basic_json_filter<CharT>(handler), 
          name_(name), new_name_(new_name)
    {
    }

private:
    bool do_name(const string_view_type& name,
                 const ser_context& context) override
    {
        if (name == name_)
        {
            return this->to_handler().name(new_name_,context);
        }
        else
        {
            return this->to_handler().name(name,context);
        }
    }
};

template <class From,class To,class Enable=void>
class json_content_handler_adaptor : public From
{
public:
    using typename From::string_view_type;
private:
    To* to_handler_;

    // noncopyable
    json_content_handler_adaptor(const json_content_handler_adaptor&) = delete;
    json_content_handler_adaptor& operator=(const json_content_handler_adaptor&) = delete;
public:

    json_content_handler_adaptor()
        : to_handler_(nullptr)
    {
    }
    json_content_handler_adaptor(To& handler)
        : to_handler_(std::addressof(handler))
    {
    }

    // moveable
    json_content_handler_adaptor(json_content_handler_adaptor&&) = default;
    json_content_handler_adaptor& operator=(json_content_handler_adaptor&&) = default;

    To& to_handler()
    {
        return *to_handler_;
    }

private:
    void do_flush() override
    {
        to_handler_->flush();
    }

    bool do_begin_object(semantic_tag tag, 
                         const ser_context& context) override
    {
        return to_handler_->begin_object(tag, context);
    }

    bool do_begin_object(size_t length, 
                         semantic_tag tag, 
                         const ser_context& context) override
    {
        return to_handler_->begin_object(length, tag, context);
    }

    bool do_end_object(const ser_context& context) override
    {
        return to_handler_->end_object(context);
    }

    bool do_begin_array(semantic_tag tag, 
                        const ser_context& context) override
    {
        return to_handler_->begin_array(tag, context);
    }

    bool do_begin_array(size_t length, 
                        semantic_tag tag, 
                        const ser_context& context) override
    {
        return to_handler_->begin_array(length, tag, context);
    }

    bool do_end_array(const ser_context& context) override
    {
        return to_handler_->end_array(context);
    }

    bool do_name(const string_view_type& name,
                 const ser_context& context) override
    {
        std::basic_string<typename To::char_type> target;
        auto result = unicons::convert(name.begin(),name.end(),std::back_inserter(target),unicons::conv_flags::strict);
        if (result.ec != unicons::conv_errc())
        {
            throw ser_error(result.ec);
        }
        return to_handler().name(target, context);
    }

    bool do_string_value(const string_view_type& value,
                         semantic_tag tag,
                         const ser_context& context) override
    {
        std::basic_string<typename To::char_type> target;
        auto result = unicons::convert(value.begin(),value.end(),std::back_inserter(target),unicons::conv_flags::strict);
        if (result.ec != unicons::conv_errc())
        {
            throw ser_error(result.ec);
        }
        return to_handler().string_value(target, tag, context);
    }

    bool do_byte_string_value(const byte_string_view& b, 
                              semantic_tag tag,
                              const ser_context& context) override
    {
        return to_handler_->byte_string_value(b, tag, context);
    }

    bool do_double_value(double value, 
                         semantic_tag tag,
                         const ser_context& context) override
    {
        return to_handler_->double_value(value, tag, context);
    }

    bool do_int64_value(int64_t value,
                        semantic_tag tag,
                        const ser_context& context) override
    {
        return to_handler_->int64_value(value, tag, context);
    }

    bool do_uint64_value(uint64_t value,
                         semantic_tag tag,
                         const ser_context& context) override
    {
        return to_handler_->uint64_value(value, tag, context);
    }

    bool do_bool_value(bool value, semantic_tag tag, const ser_context& context) override
    {
        return to_handler_->bool_value(value, tag, context);
    }

    bool do_null_value(semantic_tag tag, const ser_context& context) override
    {
        return to_handler_->null_value(tag, context);
    }

};

template <class From,class To>
class json_content_handler_adaptor<From,To,typename std::enable_if<std::is_convertible<To*,From*>::value>::type>
{
public:
    typedef typename From::char_type char_type;
    typedef typename From::char_traits_type char_traits_type;
    typedef typename From::string_view_type string_view_type;
private:
    To* to_handler_;
public:
    json_content_handler_adaptor()
        : to_handler_(nullptr)
    {
    }
    json_content_handler_adaptor(To& handler)
        : to_handler_(std::addressof(handler))
    {
    }

    operator From&() { return *to_handler_; }

    // moveable
    json_content_handler_adaptor(json_content_handler_adaptor&&) = default;
    json_content_handler_adaptor& operator=(json_content_handler_adaptor&&) = default;

    To& to_handler()
    {
        return *to_handler_;
    }
};

template <class From,class To>
json_content_handler_adaptor<From,To> make_json_content_handler_adaptor(To& to)
{
    return json_content_handler_adaptor<From, To>(to);
}

typedef basic_json_filter<char> json_filter;
typedef basic_json_filter<wchar_t> wjson_filter;
typedef basic_rename_object_member_filter<char> rename_object_member_filter;
typedef basic_rename_object_member_filter<wchar_t> wrename_object_member_filter;

#if !defined(JSONCONS_NO_DEPRECATED)
typedef basic_rename_object_member_filter<char> rename_name_filter;
typedef basic_rename_object_member_filter<wchar_t> wrename_name_filter;
#endif

}

#endif
