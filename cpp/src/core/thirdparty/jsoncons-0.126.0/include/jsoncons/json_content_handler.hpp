// Copyright 2018 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_JSON_CONTENT_HANDLER_HPP
#define JSONCONS_JSON_CONTENT_HANDLER_HPP

#include <string>
#include <jsoncons/json_exception.hpp>
#include <jsoncons/bignum.hpp>
#include <jsoncons/ser_context.hpp>
#include <jsoncons/json_options.hpp>

namespace jsoncons {

// null_type

struct null_type
{
};

enum class semantic_tag : uint8_t 
{
    none = 0,
    undefined = 0x01,
    datetime = 0x02,
    timestamp = 0x03,
    bigint = 0x04,
    bigdec = 0x05,
    bigfloat = 0x06,
    base16 = 0x07,
    base64 = 0x08,
    base64url = 0x09,
    uri = 0x0a
#if !defined(JSONCONS_NO_DEPRECATED)
    , big_integer = bigint
    , big_decimal = bigdec
    , big_float = bigfloat
    , date_time = datetime
#endif
};

#if !defined(JSONCONS_NO_DEPRECATED)
    typedef semantic_tag semantic_tag_type;
#endif

template <class CharT>
class basic_json_content_handler
{
#if !defined(JSONCONS_NO_DEPRECATED)
    std::basic_string<CharT> buffer_;
#endif
public:
    typedef CharT char_type;
    typedef std::char_traits<char_type> char_traits_type;

    typedef basic_string_view<char_type,char_traits_type> string_view_type;

    basic_json_content_handler(basic_json_content_handler&&) = default;

    basic_json_content_handler& operator=(basic_json_content_handler&&) = default;

    basic_json_content_handler() = default;

    virtual ~basic_json_content_handler() {}

    void flush()
    {
        do_flush();
    }

    bool begin_object(semantic_tag tag=semantic_tag::none,
                      const ser_context& context=null_ser_context())
    {
        return do_begin_object(tag, context);
    }

    bool begin_object(size_t length, 
                      semantic_tag tag=semantic_tag::none, 
                      const ser_context& context = null_ser_context())
    {
        return do_begin_object(length, tag, context);
    }

    bool end_object(const ser_context& context = null_ser_context())
    {
        return do_end_object(context);
    }

    bool begin_array(semantic_tag tag=semantic_tag::none,
                     const ser_context& context=null_ser_context())
    {
        return do_begin_array(tag, context);
    }

    bool begin_array(size_t length, 
                     semantic_tag tag=semantic_tag::none,
                     const ser_context& context=null_ser_context())
    {
        return do_begin_array(length, tag, context);
    }

    bool end_array(const ser_context& context=null_ser_context())
    {
        return do_end_array(context);
    }

    bool name(const string_view_type& name, const ser_context& context=null_ser_context())
    {
        return do_name(name, context);
    }

    bool string_value(const string_view_type& value, 
                      semantic_tag tag = semantic_tag::none, 
                      const ser_context& context=null_ser_context()) 
    {
        return do_string_value(value, tag, context);
    }

    bool byte_string_value(const byte_string_view& b, 
                           semantic_tag tag=semantic_tag::none, 
                           const ser_context& context=null_ser_context())
    {
        return do_byte_string_value(b, tag, context);
    }

    bool byte_string_value(const uint8_t* p, size_t size, 
                           semantic_tag tag=semantic_tag::none, 
                           const ser_context& context=null_ser_context())
    {
        return do_byte_string_value(byte_string(p, size), tag, context);
    }
#if !defined(JSONCONS_NO_DEPRECATED)
    bool byte_string_value(const byte_string_view& b, 
                           byte_string_chars_format encoding_hint, 
                           semantic_tag tag=semantic_tag::none, 
                           const ser_context& context=null_ser_context())
    {
        switch (encoding_hint)
        {
            case byte_string_chars_format::base16:
                tag = semantic_tag::base16;
                break;
            case byte_string_chars_format::base64:
                tag = semantic_tag::base64;
                break;
            case byte_string_chars_format::base64url:
                tag = semantic_tag::base64url;
                break;
            default:
                break;
        }
        return do_byte_string_value(b, tag, context);
    }

    bool byte_string_value(const uint8_t* p, size_t size, 
                           byte_string_chars_format encoding_hint, 
                           semantic_tag tag=semantic_tag::none, 
                           const ser_context& context=null_ser_context())
    {
        switch (encoding_hint)
        {
            case byte_string_chars_format::base16:
                tag = semantic_tag::base16;
                break;
            case byte_string_chars_format::base64:
                tag = semantic_tag::base64;
                break;
            case byte_string_chars_format::base64url:
                tag = semantic_tag::base64url;
                break;
            default:
                break;
        }
        return do_byte_string_value(byte_string(p, size), tag, context);
    }
    bool big_integer_value(const string_view_type& s, const ser_context& context=null_ser_context()) 
    {
        return do_string_value(s, semantic_tag::bigint, context);
    }

    bool big_decimal_value(const string_view_type& s, const ser_context& context=null_ser_context()) 
    {
        return do_string_value(s, semantic_tag::bigdec, context);
    }

    bool date_time_value(const string_view_type& s, const ser_context& context=null_ser_context()) 
    {
        return do_string_value(s, semantic_tag::datetime, context);
    }

    bool timestamp_value(int64_t val, const ser_context& context=null_ser_context()) 
    {
        return do_int64_value(val, semantic_tag::timestamp, context);
    }
#endif

    bool int64_value(int64_t value, 
                     semantic_tag tag = semantic_tag::none, 
                     const ser_context& context=null_ser_context())
    {
        return do_int64_value(value, tag, context);
    }

    bool uint64_value(uint64_t value, 
                      semantic_tag tag = semantic_tag::none, 
                      const ser_context& context=null_ser_context())
    {
        return do_uint64_value(value, tag, context);
    }

    bool double_value(double value, 
                      semantic_tag tag = semantic_tag::none, 
                      const ser_context& context=null_ser_context())
    {
        return do_double_value(value, tag, context);
    }

    bool bool_value(bool value, 
                    semantic_tag tag = semantic_tag::none,
                    const ser_context& context=null_ser_context()) 
    {
        return do_bool_value(value, tag, context);
    }

    bool null_value(semantic_tag tag = semantic_tag::none,
                    const ser_context& context=null_ser_context()) 
    {
        return do_null_value(tag, context);
    }

#if !defined(JSONCONS_NO_DEPRECATED)

    bool begin_document()
    {
        return true;
    }

    bool end_document()
    {
        flush();
        return true;
    }

    void begin_json()
    {
    }

    void end_json()
    {
        end_document();
    }

    void name(const CharT* p, size_t length, const ser_context& context) 
    {
        name(string_view_type(p, length), context);
    }

    void integer_value(int64_t value)
    {
        int64_value(value);
    }

    void integer_value(int64_t value, const ser_context& context)
    {
        int64_value(value,context);
    }

    void uinteger_value(uint64_t value)
    {
        uint64_value(value);
    }

    void uinteger_value(uint64_t value, const ser_context& context)
    {
        uint64_value(value,context);
    }

    bool bignum_value(const string_view_type& s, const ser_context& context=null_ser_context()) 
    {
        return do_string_value(s, semantic_tag::bigint, context);
    }

    bool decimal_value(const string_view_type& s, const ser_context& context=null_ser_context()) 
    {
        return do_string_value(s, semantic_tag::bigdec, context);
    }

    bool epoch_time_value(int64_t val, const ser_context& context=null_ser_context()) 
    {
        return do_int64_value(val, semantic_tag::timestamp, context);
    }

#endif

private:
    virtual void do_flush() = 0;

    virtual bool do_begin_object(semantic_tag, const ser_context& context) = 0;

    virtual bool do_begin_object(size_t, semantic_tag tag, const ser_context& context)
    {
        return do_begin_object(tag, context);
    }

    virtual bool do_end_object(const ser_context& context) = 0;

    virtual bool do_begin_array(semantic_tag, const ser_context& context) = 0;

    virtual bool do_begin_array(size_t, semantic_tag tag, const ser_context& context)
    {
        return do_begin_array(tag, context);
    }

    virtual bool do_end_array(const ser_context& context) = 0;

    virtual bool do_name(const string_view_type& name, const ser_context& context) = 0;

    virtual bool do_null_value(semantic_tag, const ser_context& context) = 0;

    virtual bool do_string_value(const string_view_type& value, semantic_tag tag, const ser_context& context) = 0;

    virtual bool do_byte_string_value(const byte_string_view& b, 
                                      semantic_tag tag, 
                                      const ser_context& context) = 0;

    virtual bool do_double_value(double value, 
                                 semantic_tag tag,
                                 const ser_context& context) = 0;

    virtual bool do_int64_value(int64_t value, 
                                semantic_tag tag,
                                const ser_context& context) = 0;

    virtual bool do_uint64_value(uint64_t value, 
                                 semantic_tag tag, 
                                 const ser_context& context) = 0;

    virtual bool do_bool_value(bool value, semantic_tag tag, const ser_context& context) = 0;
};

template <class CharT>
class basic_null_json_content_handler final : public basic_json_content_handler<CharT>
{
public:
    using typename basic_json_content_handler<CharT>::string_view_type;
private:
    void do_flush() override
    {
    }

    bool do_begin_object(semantic_tag, const ser_context&) override
    {
        return true;
    }

    bool do_end_object(const ser_context&) override
    {
        return true;
    }

    bool do_begin_array(semantic_tag, const ser_context&) override
    {
        return true;
    }

    bool do_end_array(const ser_context&) override
    {
        return true;
    }

    bool do_name(const string_view_type&, const ser_context&) override
    {
        return true;
    }

    bool do_null_value(semantic_tag, const ser_context&) override
    {
        return true;
    }

    bool do_string_value(const string_view_type&, semantic_tag, const ser_context&) override
    {
        return true;
    }

    bool do_byte_string_value(const byte_string_view&,
                              semantic_tag, 
                              const ser_context&) override
    {
        return true;
    }

    bool do_int64_value(int64_t, 
                        semantic_tag, 
                        const ser_context&) override
    {
        return true;
    }

    bool do_uint64_value(uint64_t, 
                         semantic_tag, 
                         const ser_context&) override
    {
        return true;
    }

    bool do_double_value(double, 
                         semantic_tag,
                         const ser_context&) override
    {
        return true;
    }

    bool do_bool_value(bool, semantic_tag, const ser_context&) override
    {
        return true;
    }
};

typedef basic_json_content_handler<char> json_content_handler;
typedef basic_json_content_handler<wchar_t> wjson_content_handler;

typedef basic_null_json_content_handler<char> null_json_content_handler;
typedef basic_null_json_content_handler<wchar_t> wnull_json_content_handler;

}

#endif
