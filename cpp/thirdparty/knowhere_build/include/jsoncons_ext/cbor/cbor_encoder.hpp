// Copyright 2018 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_CBOR_CBOR_ENCODER_HPP
#define JSONCONS_CBOR_CBOR_ENCODER_HPP

#include <string>
#include <vector>
#include <limits> // std::numeric_limits
#include <memory>
#include <utility> // std::move
#include <jsoncons/json_exception.hpp> // jsoncons::ser_error
#include <jsoncons/json_content_handler.hpp>
#include <jsoncons/config/binary_detail.hpp>
#include <jsoncons/result.hpp>
#include <jsoncons/detail/parse_number.hpp>
#include <jsoncons_ext/cbor/cbor_error.hpp>
#include <jsoncons_ext/cbor/cbor_options.hpp>

namespace jsoncons { namespace cbor {

enum class cbor_container_type {object, indefinite_length_object, array, indefinite_length_array};

template<class Result=jsoncons::binary_stream_result>
class basic_cbor_encoder final : public basic_json_content_handler<char>
{
    enum class decimal_parse_state { start, integer, exp1, exp2, fraction1 };
    enum class hexfloat_parse_state { start, expect_0, expect_x, integer, exp1, exp2, fraction1 };

public:
    typedef char char_type;
    typedef Result result_type;
    using typename basic_json_content_handler<char>::string_view_type;

private:
    struct stack_item
    {
        cbor_container_type type_;
        size_t length_;
        size_t count_;

        stack_item(cbor_container_type type, size_t length = 0)
           : type_(type), length_(length), count_(0)
        {
        }

        size_t length() const
        {
            return length_;
        }

        size_t count() const
        {
            return count_;
        }

        bool is_object() const
        {
            return type_ == cbor_container_type::object || type_ == cbor_container_type::indefinite_length_object;
        }

        bool is_indefinite_length() const
        {
            return type_ == cbor_container_type::indefinite_length_array || type_ == cbor_container_type::indefinite_length_object;
        }

    };
    std::vector<stack_item> stack_;
    Result result_;
    const cbor_encode_options& options_;

    // Noncopyable and nonmoveable
    basic_cbor_encoder(const basic_cbor_encoder&) = delete;
    basic_cbor_encoder& operator=(const basic_cbor_encoder&) = delete;

    std::map<std::string,size_t> stringref_map_;
    std::map<byte_string,size_t> bytestringref_map_;
    size_t next_stringref_ = 0;
public:
    explicit basic_cbor_encoder(result_type result)
       : result_(std::move(result)), options_(cbor_options::default_options())
    {
    }
    basic_cbor_encoder(result_type result, const cbor_encode_options& options)
       : result_(std::move(result)), options_(options)
    {
        if (options.pack_strings())
        {
            // tag(256)
            result_.push_back(0xd9);
            result_.push_back(0x01);
            result_.push_back(0x00);
        }
    }

    ~basic_cbor_encoder()
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
        stack_.push_back(stack_item(cbor_container_type::indefinite_length_object));
        
        result_.push_back(0xbf);
        return true;
    }

    bool do_begin_object(size_t length, semantic_tag, const ser_context&) override
    {
        stack_.push_back(stack_item(cbor_container_type::object, length));

        if (length <= 0x17)
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0xa0 + length), 
                                  std::back_inserter(result_));
        } 
        else if (length <= 0xff)
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0xb8), 
                                  std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(length), 
                                  std::back_inserter(result_));
        } 
        else if (length <= 0xffff)
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0xb9), 
                                  std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint16_t>(length), 
                                  std::back_inserter(result_));
        } 
        else if (length <= 0xffffffff)
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0xba), 
                                  std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint32_t>(length), 
                                  std::back_inserter(result_));
        } 
        else if (length <= 0xffffffffffffffff)
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0xbb), 
                                  std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint64_t>(length), 
                                  std::back_inserter(result_));
        }

        return true;
    }

    bool do_end_object(const ser_context&) override
    {
        JSONCONS_ASSERT(!stack_.empty());
        if (stack_.back().is_indefinite_length())
        {
            result_.push_back(0xff);
        }
        else
        {
            if (stack_.back().count() < stack_.back().length())
            {
                throw ser_error(cbor_errc::too_few_items);
            }
            if (stack_.back().count() > stack_.back().length())
            {
                throw ser_error(cbor_errc::too_many_items);
            }
        }

        stack_.pop_back();
        end_value();

        return true;
    }

    bool do_begin_array(semantic_tag, const ser_context&) override
    {
        stack_.push_back(stack_item(cbor_container_type::indefinite_length_array));
        result_.push_back(0x9f);
        return true;
    }

    bool do_begin_array(size_t length, semantic_tag tag, const ser_context&) override
    {
#if !defined(JSONCONS_NO_DEPRECATED)
        if (length == 2 && tag == semantic_tag::bigfloat)
        {
            result_.push_back(0xc5);
        }
#endif
        stack_.push_back(stack_item(cbor_container_type::array, length));
        if (length <= 0x17)
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x80 + length), 
                                  std::back_inserter(result_));
        } 
        else if (length <= 0xff)
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x98), 
                                  std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(length), 
                                  std::back_inserter(result_));
        } 
        else if (length <= 0xffff)
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x99), 
                                  std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint16_t>(length), 
                                  std::back_inserter(result_));
        } 
        else if (length <= 0xffffffff)
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x9a), 
                                  std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint32_t>(length), 
                                  std::back_inserter(result_));
        } 
        else if (length <= 0xffffffffffffffff)
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x9b), 
                                  std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint64_t>(length), 
                                  std::back_inserter(result_));
        }
        return true;
    }

    bool do_end_array(const ser_context&) override
    {
        JSONCONS_ASSERT(!stack_.empty());

        if (stack_.back().is_indefinite_length())
        {
            result_.push_back(0xff);
        }
        else
        {
            if (stack_.back().count() < stack_.back().length())
            {
                throw ser_error(cbor_errc::too_few_items);
            }
            if (stack_.back().count() > stack_.back().length())
            {
                throw ser_error(cbor_errc::too_many_items);
            }
        }

        stack_.pop_back();
        end_value();

        return true;
    }

    bool do_name(const string_view_type& name, const ser_context&) override
    {
        write_string(name);
        return true;
    }

    bool do_null_value(semantic_tag tag, const ser_context&) override
    {
        if (tag == semantic_tag::undefined)
        {
            result_.push_back(0xf7);
        }
        else
        {
            result_.push_back(0xf6);
        }

        end_value();
        return true;
    }

    void write_string(const string_view& sv)
    {
        auto result = unicons::validate(sv.begin(), sv.end());
        if (result.ec != unicons::conv_errc())
        {
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Illegal unicode"));
        }

        if (options_.pack_strings() && sv.size() >= jsoncons::cbor::detail::min_length_for_stringref(next_stringref_))
        {
            std::string s(sv);
            auto it = stringref_map_.find(s);
            if (it == stringref_map_.end())
            {
                stringref_map_.insert(std::make_pair(std::move(s), next_stringref_++));
                write_utf8_string(sv);
            }
            else
            {
                // tag(25)
                result_.push_back(0xd8); 
                result_.push_back(0x19); 
                write_uint64_value(it->second);
            }
        }
        else
        {
            write_utf8_string(sv);
        }
    }

    void write_utf8_string(const string_view& sv)
    {
        const size_t length = sv.size();

        if (length <= 0x17)
        {
            // fixstr stores a byte array whose length is upto 31 bytes
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x60 + length), 
                                            std::back_inserter(result_));
        }
        else if (length <= 0xff)
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x78), 
                                            std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(length), 
                                            std::back_inserter(result_));
        }
        else if (length <= 0xffff)
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x79), 
                                            std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint16_t>(length), 
                                            std::back_inserter(result_));
        }
        else if (length <= 0xffffffff)
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x7a), 
                                            std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint32_t>(length), 
                                            std::back_inserter(result_));
        }
        else if (length <= 0xffffffffffffffff)
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x7b), 
                                            std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint64_t>(length), 
                                            std::back_inserter(result_));
        }

        for (auto c : sv)
        {
            result_.push_back(c);
        }
    }

    void write_bignum(const bignum& n)
    {
        int signum;
        std::vector<uint8_t> data;
        n.dump(signum, data);
        size_t length = data.size();

        if (signum == -1)
        {
            result_.push_back(0xc3);
        }
        else
        {
            result_.push_back(0xc2);
        }

        if (length <= 0x17)
        {
            // fixstr stores a byte array whose length is upto 31 bytes
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x40 + length), 
                                  std::back_inserter(result_));
        }
        else if (length <= 0xff)
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x58), 
                                  std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(length), 
                                  std::back_inserter(result_));
        }
        else if (length <= 0xffff)
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x59), 
                                  std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint16_t>(length), 
                                  std::back_inserter(result_));
        }
        else if (length <= 0xffffffff)
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x5a), 
                                  std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint32_t>(length), 
                                  std::back_inserter(result_));
        }
        else if (length <= 0xffffffffffffffff)
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x5b), 
                                  std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint64_t>(length), 
                                  std::back_inserter(result_));
        }

        for (auto c : data)
        {
            result_.push_back(c);
        }
    }

    void write_decimal_value(const string_view_type& sv, const ser_context& context)
    {
        decimal_parse_state state = decimal_parse_state::start;
        std::basic_string<char> s;
        std::basic_string<char> exponent;
        int64_t scale = 0;
        for (auto c : sv)
        {
            switch (state)
            {
                case decimal_parse_state::start:
                {
                    switch (c)
                    {
                        case '-':
                            s.push_back(c);
                            state = decimal_parse_state::integer;
                            break;
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8': case '9':
                            s.push_back(c);
                            state = decimal_parse_state::integer;
                            break;
                        default:
                            throw std::invalid_argument("Invalid decimal string");
                    }
                    break;
                }
                case decimal_parse_state::integer:
                {
                    switch (c)
                    {
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8': case '9':
                            s.push_back(c);
                            state = decimal_parse_state::integer;
                            break;
                        case 'e': case 'E':
                            state = decimal_parse_state::exp1;
                            break;
                        case '.':
                            state = decimal_parse_state::fraction1;
                            break;
                        default:
                            throw std::invalid_argument("Invalid decimal string");
                    }
                    break;
                }
                case decimal_parse_state::exp1:
                {
                    switch (c)
                    {
                        case '+':
                            state = decimal_parse_state::exp2;
                            break;
                        case '-':
                            exponent.push_back(c);
                            state = decimal_parse_state::exp2;
                            break;
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8': case '9':
                            exponent.push_back(c);
                            state = decimal_parse_state::exp2;
                            break;
                        default:
                            throw std::invalid_argument("Invalid decimal string");
                    }
                    break;
                }
                case decimal_parse_state::exp2:
                {
                    switch (c)
                    {
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8': case '9':
                            exponent.push_back(c);
                            break;
                        default:
                            throw std::invalid_argument("Invalid decimal string");
                    }
                    break;
                }
                case decimal_parse_state::fraction1:
                {
                    switch (c)
                    {
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8': case '9':
                            s.push_back(c);
                            --scale;
                            break;
                        default:
                            throw std::invalid_argument("Invalid decimal string");
                    }
                    break;
                }
            }
        }

        result_.push_back(0xc4);
        do_begin_array((size_t)2, semantic_tag::none, context);
        if (exponent.length() > 0)
        {
            auto result = jsoncons::detail::to_integer<int64_t>(exponent.data(), exponent.length());
            if (result.ec != jsoncons::detail::to_integer_errc())
            {
                JSONCONS_THROW(json_runtime_error<std::invalid_argument>(make_error_code(result.ec).message()));
            }
            scale += result.value;
        }
        do_int64_value(scale, semantic_tag::none, context);

        auto result = jsoncons::detail::to_integer<int64_t>(s.data(),s.length());
        if (result.ec == jsoncons::detail::to_integer_errc())
        {
            do_int64_value(result.value, semantic_tag::none, context);
        }
        else if (result.ec == jsoncons::detail::to_integer_errc::overflow)
        {
            bignum n(s.data(), s.length());
            write_bignum(n);
            end_value();
        }
        else
        {
            JSONCONS_THROW(json_runtime_error<std::invalid_argument>(make_error_code(result.ec).message()));
        }
        do_end_array(context);
    }

    void write_hexfloat_value(const string_view_type& sv, const ser_context& context)
    {
        hexfloat_parse_state state = hexfloat_parse_state::start;
        std::basic_string<char> s;
        std::basic_string<char> exponent;
        int64_t scale = 0;

        for (auto c : sv)
        {
            switch (state)
            {
                case hexfloat_parse_state::start:
                {
                    switch (c)
                    {
                        case '-':
                            s.push_back(c);
                            state = hexfloat_parse_state::expect_0;
                            break;
                        case '0':
                            state = hexfloat_parse_state::expect_x;
                            break;
                        default:
                            throw std::invalid_argument("Invalid hexfloat string");
                    }
                    break;
                }
                case hexfloat_parse_state::expect_0:
                {
                    switch (c)
                    {
                        case '0':
                            state = hexfloat_parse_state::expect_x;
                            break;
                        default:
                            throw std::invalid_argument("Invalid hexfloat string");
                    }
                    break;
                }
                case hexfloat_parse_state::expect_x:
                {
                    switch (c)
                    {
                        case 'x':
                        case 'X':
                            state = hexfloat_parse_state::integer;
                            break;
                        default:
                            throw std::invalid_argument("Invalid hexfloat string");
                    }
                    break;
                }
                case hexfloat_parse_state::integer:
                {
                    switch (c)
                    {
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8': case '9':case 'a':case 'b':case 'c':case 'd':case 'e':case 'f':case 'A':case 'B':case 'C':case 'D':case 'E':case 'F':
                            s.push_back(c);
                            state = hexfloat_parse_state::integer;
                            break;
                        case 'p': case 'P':
                            state = hexfloat_parse_state::exp1;
                            break;
                        case '.':
                            state = hexfloat_parse_state::fraction1;
                            break;
                        default:
                            throw std::invalid_argument("Invalid hexfloat string");
                    }
                    break;
                }
                case hexfloat_parse_state::exp1:
                {
                    switch (c)
                    {
                        case '+':
                            state = hexfloat_parse_state::exp2;
                            break;
                        case '-':
                            exponent.push_back(c);
                            state = hexfloat_parse_state::exp2;
                            break;
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8': case '9':case 'a':case 'b':case 'c':case 'd':case 'e':case 'f':case 'A':case 'B':case 'C':case 'D':case 'E':case 'F':
                            exponent.push_back(c);
                            state = hexfloat_parse_state::exp2;
                            break;
                        default:
                            throw std::invalid_argument("Invalid hexfloat string");
                    }
                    break;
                }
                case hexfloat_parse_state::exp2:
                {
                    switch (c)
                    {
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8': case '9':case 'a':case 'b':case 'c':case 'd':case 'e':case 'f':case 'A':case 'B':case 'C':case 'D':case 'E':case 'F':
                            exponent.push_back(c);
                            break;
                        default:
                            throw std::invalid_argument("Invalid hexfloat string");
                    }
                    break;
                }
                case hexfloat_parse_state::fraction1:
                {
                    switch (c)
                    {
                        case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8': case '9':case 'a':case 'b':case 'c':case 'd':case 'e':case 'f':case 'A':case 'B':case 'C':case 'D':case 'E':case 'F':
                            s.push_back(c);
                            scale -= 4;
                            break;
                        default:
                            throw std::invalid_argument("Invalid hexfloat string");
                    }
                    break;
                }
            }
        }

        result_.push_back(0xc5);
        do_begin_array((size_t)2, semantic_tag::none, context);
        if (exponent.length() > 0)
        {
            auto result = jsoncons::detail::base16_to_integer<int64_t>(exponent.data(), exponent.length());
            if (result.ec != jsoncons::detail::to_integer_errc())
            {
                JSONCONS_THROW(json_runtime_error<std::invalid_argument>(make_error_code(result.ec).message()));
            }
            scale += result.value;
        }
        do_int64_value(scale, semantic_tag::none, context);

        auto result = jsoncons::detail::base16_to_integer<int64_t>(s.data(),s.length());
        if (result.ec == jsoncons::detail::to_integer_errc())
        {
            do_int64_value(result.value, semantic_tag::none, context);
        }
        else if (result.ec == jsoncons::detail::to_integer_errc::overflow)
        {
            bignum n(s.data(), s.length(), 16);
            write_bignum(n);
            end_value();
        }
        else
        {
            JSONCONS_THROW(json_runtime_error<std::invalid_argument>(make_error_code(result.ec).message()));
        }
        do_end_array(context);
    }

    bool do_string_value(const string_view_type& sv, semantic_tag tag, const ser_context& context) override
    {
        switch (tag)
        {
            case semantic_tag::bigint:
            {
                bignum n(sv.data(), sv.length());
                write_bignum(n);
                end_value();
                break;
            }
            case semantic_tag::bigdec:
            {
                write_decimal_value(sv, context);
                break;
            }
            case semantic_tag::bigfloat:
            {
                write_hexfloat_value(sv, context);
                break;
            }
            case semantic_tag::datetime:
            {
                result_.push_back(0xc0);
                write_string(sv);
                end_value();
                break;
            }
            case semantic_tag::uri:
            {
                result_.push_back(32);
                write_string(sv);
                end_value();
                break;
            }
            case semantic_tag::base64url:
            {
                result_.push_back(33);
                write_string(sv);
                end_value();
                break;
            }
            case semantic_tag::base64:
            {
                result_.push_back(34);
                write_string(sv);
                end_value();
                break;
            }
            default:
            {
                write_string(sv);
                end_value();
                break;
            }
        }
        return true;
    }

    bool do_byte_string_value(const byte_string_view& b, 
                              semantic_tag tag, 
                              const ser_context&) override
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
        switch (encoding_hint)
        {
            case byte_string_chars_format::base64url:
                result_.push_back(0xd5);
                break;
            case byte_string_chars_format::base64:
                result_.push_back(0xd6);
                break;
            case byte_string_chars_format::base16:
                result_.push_back(0xd7);
                break;
            default:
                break;
        }
        if (options_.pack_strings() && b.length() >= jsoncons::cbor::detail::min_length_for_stringref(next_stringref_))
        {
            auto it = bytestringref_map_.find(byte_string(b));
            if (it == bytestringref_map_.end())
            {
                bytestringref_map_.insert(std::make_pair(byte_string(b), next_stringref_++));
                write_byte_string_value(b);
            }
            else
            {
                // tag(25)
                result_.push_back(0xd8); 
                result_.push_back(0x19); 
                write_uint64_value(it->second);
            }
        }
        else
        {
            write_byte_string_value(b);
        }

        end_value();
        return true;
    }

    void write_byte_string_value(const byte_string_view& b) 
    {
        if (b.length() <= 0x17)
        {
            // fixstr stores a byte array whose length is upto 31 bytes
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x40 + b.length()), 
                                            std::back_inserter(result_));
        }
        else if (b.length() <= 0xff)
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x58), 
                                            std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(b.length()), 
                                            std::back_inserter(result_));
        }
        else if (b.length() <= 0xffff)
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x59), 
                                            std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint16_t>(b.length()), 
                                            std::back_inserter(result_));
        }
        else if (b.length() <= 0xffffffff)
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x5a), 
                                            std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint32_t>(b.length()), 
                                            std::back_inserter(result_));
        }
        else if (b.length() <= 0xffffffffffffffff)
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x5b), 
                                            std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint64_t>(b.length()), 
                                            std::back_inserter(result_));
        }

        for (auto c : b)
        {
            result_.push_back(c);
        }
    }

    bool do_double_value(double val, 
                         semantic_tag tag,
                         const ser_context&) override
    {
        if (tag == semantic_tag::timestamp)
        {
            result_.push_back(0xc1);
        }

        float valf = (float)val;
        if ((double)valf == val)
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0xfa), 
                                  std::back_inserter(result_));
            jsoncons::detail::to_big_endian(valf, std::back_inserter(result_));
        }
        else
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0xfb), 
                                  std::back_inserter(result_));
            jsoncons::detail::to_big_endian(val, std::back_inserter(result_));
        }

        // write double

        end_value();
        return true;
    }

    bool do_int64_value(int64_t value, 
                        semantic_tag tag, 
                        const ser_context&) override
    {
        if (tag == semantic_tag::timestamp)
        {
            result_.push_back(0xc1);
        }
        if (value >= 0)
        {
            if (value <= 0x17)
            {
                jsoncons::detail::to_big_endian(static_cast<uint8_t>(value), 
                                  std::back_inserter(result_));
            } 
            else if (value <= (std::numeric_limits<uint8_t>::max)())
            {
                jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x18), 
                                  std::back_inserter(result_));
                jsoncons::detail::to_big_endian(static_cast<uint8_t>(value), 
                                  std::back_inserter(result_));
            } 
            else if (value <= (std::numeric_limits<uint16_t>::max)())
            {
                jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x19), 
                                  std::back_inserter(result_));
                jsoncons::detail::to_big_endian(static_cast<uint16_t>(value), 
                                  std::back_inserter(result_));
            } 
            else if (value <= (std::numeric_limits<uint32_t>::max)())
            {
                jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x1a), 
                                  std::back_inserter(result_));
                jsoncons::detail::to_big_endian(static_cast<uint32_t>(value), 
                                  std::back_inserter(result_));
            } 
            else if (value <= (std::numeric_limits<int64_t>::max)())
            {
                jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x1b), 
                                  std::back_inserter(result_));
                jsoncons::detail::to_big_endian(static_cast<int64_t>(value), 
                                  std::back_inserter(result_));
            }
        } else
        {
            const auto posnum = -1 - value;
            if (value >= -24)
            {
                jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x20 + posnum), 
                                  std::back_inserter(result_));
            } 
            else if (posnum <= (std::numeric_limits<uint8_t>::max)())
            {
                jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x38), 
                                  std::back_inserter(result_));
                jsoncons::detail::to_big_endian(static_cast<uint8_t>(posnum), 
                                  std::back_inserter(result_));
            } 
            else if (posnum <= (std::numeric_limits<uint16_t>::max)())
            {
                jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x39), 
                                  std::back_inserter(result_));
                jsoncons::detail::to_big_endian(static_cast<uint16_t>(posnum), 
                                  std::back_inserter(result_));
            } 
            else if (posnum <= (std::numeric_limits<uint32_t>::max)())
            {
                jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x3a), 
                                  std::back_inserter(result_));
                jsoncons::detail::to_big_endian(static_cast<uint32_t>(posnum), 
                                  std::back_inserter(result_));
            } 
            else if (posnum <= (std::numeric_limits<int64_t>::max)())
            {
                jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x3b), 
                                  std::back_inserter(result_));
                jsoncons::detail::to_big_endian(static_cast<int64_t>(posnum), 
                                  std::back_inserter(result_));
            }
        }
        end_value();
        return true;
    }

    bool do_uint64_value(uint64_t value, 
                         semantic_tag tag, 
                         const ser_context&) override
    {
        if (tag == semantic_tag::timestamp)
        {
            result_.push_back(0xc1);
        }

        write_uint64_value(value);
        end_value();
        return true;
    }

    void write_uint64_value(uint64_t value) 
    {
        if (value <= 0x17)
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(value), 
                                            std::back_inserter(result_));
        } 
        else if (value <=(std::numeric_limits<uint8_t>::max)())
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x18), 
                                            std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(value), 
                                            std::back_inserter(result_));
        } 
        else if (value <=(std::numeric_limits<uint16_t>::max)())
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x19), 
                                            std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint16_t>(value), 
                                            std::back_inserter(result_));
        } 
        else if (value <=(std::numeric_limits<uint32_t>::max)())
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x1a), 
                                            std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint32_t>(value), 
                                            std::back_inserter(result_));
        } 
        else if (value <=(std::numeric_limits<uint64_t>::max)())
        {
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(0x1b), 
                                            std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint64_t>(value), 
                                            std::back_inserter(result_));
        }
    }

    bool do_bool_value(bool value, semantic_tag, const ser_context&) override
    {
        if (value)
        {
            result_.push_back(0xf5);
        }
        else
        {
            result_.push_back(0xf4);
        }

        end_value();
        return true;
    }

    void end_value()
    {
        if (!stack_.empty())
        {
            ++stack_.back().count_;
        }
    }
};

typedef basic_cbor_encoder<jsoncons::binary_stream_result> cbor_encoder;
typedef basic_cbor_encoder<jsoncons::bytes_result> cbor_bytes_encoder;

#if !defined(JSONCONS_NO_DEPRECATED)
typedef basic_cbor_encoder<jsoncons::bytes_result> cbor_bytes_serializer;

template<class Result=jsoncons::binary_stream_result>
using basic_cbor_serializer = basic_cbor_encoder<Result>; 

typedef basic_cbor_encoder<jsoncons::binary_stream_result> cbor_serializer;
typedef basic_cbor_encoder<jsoncons::bytes_result> cbor_buffer_serializer;
#endif

}}
#endif
