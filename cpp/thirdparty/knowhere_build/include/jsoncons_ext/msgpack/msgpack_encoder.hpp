// Copyright 2018 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_MSGPACK_MSGPACK_ENCODER_HPP
#define JSONCONS_MSGPACK_MSGPACK_ENCODER_HPP

#include <string>
#include <vector>
#include <limits> // std::numeric_limits
#include <memory>
#include <utility> // std::move
#include <jsoncons/json_exception.hpp>
#include <jsoncons/json_content_handler.hpp>
#include <jsoncons/config/binary_detail.hpp>
#include <jsoncons/result.hpp>
#include <jsoncons/detail/parse_number.hpp>
#include <jsoncons_ext/msgpack/msgpack_detail.hpp>
#include <jsoncons_ext/msgpack/msgpack_error.hpp>

namespace jsoncons { namespace msgpack {

enum class msgpack_container_type {object, indefinite_length_object, array, indefinite_length_array};

template<class Result=jsoncons::binary_stream_result>
class basic_msgpack_encoder final : public basic_json_content_handler<char>
{
    enum class decimal_parse_state { start, integer, exp1, exp2, fraction1 };
public:
    typedef char char_type;
    using typename basic_json_content_handler<char>::string_view_type;
    typedef Result result_type;

private:
    struct stack_item
    {
        msgpack_container_type type_;
        size_t length_;
        size_t count_;

        stack_item(msgpack_container_type type, size_t length = 0)
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
            return type_ == msgpack_container_type::object || type_ == msgpack_container_type::indefinite_length_object;
        }

        bool is_indefinite_length() const
        {
            return type_ == msgpack_container_type::indefinite_length_array || type_ == msgpack_container_type::indefinite_length_object;
        }

    };
    std::vector<stack_item> stack_;
    Result result_;

    // Noncopyable and nonmoveable
    basic_msgpack_encoder(const basic_msgpack_encoder&) = delete;
    basic_msgpack_encoder& operator=(const basic_msgpack_encoder&) = delete;
public:
    explicit basic_msgpack_encoder(result_type result)
       : result_(std::move(result))
    {
    }

    ~basic_msgpack_encoder()
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
        throw ser_error(msgpack_errc::object_length_required);
    }

    bool do_begin_object(size_t length, semantic_tag, const ser_context&) override
    {
        stack_.push_back(stack_item(msgpack_container_type::object, length));

        if (length <= 15)
        {
            // fixmap
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(jsoncons::msgpack::detail::msgpack_format ::fixmap_base_cd | (length & 0xf)), 
                                  std::back_inserter(result_));
        }
        else if (length <= 65535)
        {
            // map 16
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(jsoncons::msgpack::detail::msgpack_format ::map16_cd), 
                                  std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint16_t>(length), 
                                  std::back_inserter(result_));
        }
        else if (length <= 4294967295)
        {
            // map 32
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(jsoncons::msgpack::detail::msgpack_format ::map32_cd), 
                                  std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint32_t>(length),
                                  std::back_inserter(result_));
        }

        return true;
    }

    bool do_end_object(const ser_context&) override
    {
        JSONCONS_ASSERT(!stack_.empty());

        if (stack_.back().count() < stack_.back().length())
        {
            throw ser_error( msgpack_errc::too_few_items);
        }
        else if (stack_.back().count() > stack_.back().length())
        {
            throw ser_error( msgpack_errc::too_many_items);
        }

        stack_.pop_back();
        end_value();
        return true;
    }

    bool do_begin_array(semantic_tag, const ser_context&) override
    {
        throw ser_error(msgpack_errc::array_length_required);
    }

    bool do_begin_array(size_t length, semantic_tag, const ser_context&) override
    {
        stack_.push_back(stack_item(msgpack_container_type::array, length));
        if (length <= 15)
        {
            // fixarray
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(jsoncons::msgpack::detail::msgpack_format ::fixarray_base_cd | (length & 0xf)), std::back_inserter(result_));
        }
        else if (length <= (std::numeric_limits<uint16_t>::max)())
        {
            // array 16
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(jsoncons::msgpack::detail::msgpack_format ::array16_cd), std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint16_t>(length),std::back_inserter(result_));
        }
        else if (length <= (std::numeric_limits<uint32_t>::max)())
        {
            // array 32
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(jsoncons::msgpack::detail::msgpack_format ::array32_cd), std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint32_t>(length),std::back_inserter(result_));
        }
        return true;
    }

    bool do_end_array(const ser_context&) override
    {
        JSONCONS_ASSERT(!stack_.empty());

        if (stack_.back().count() < stack_.back().length())
        {
            throw ser_error(msgpack_errc::too_few_items);
        }
        else if (stack_.back().count() > stack_.back().length())
        {
            throw ser_error(msgpack_errc::too_many_items);
        }

        stack_.pop_back();
        end_value();
        return true;
    }

    bool do_name(const string_view_type& name, const ser_context&) override
    {
        write_string_value(name);
        return true;
    }

    bool do_null_value(semantic_tag, const ser_context&) override
    {
        // nil
        jsoncons::detail::to_big_endian(static_cast<uint8_t>(jsoncons::msgpack::detail::msgpack_format ::nil_cd), std::back_inserter(result_));
        end_value();
        return true;
    }

    bool do_string_value(const string_view_type& sv, semantic_tag, const ser_context&) override
    {
        write_string_value(sv);
        end_value();
        return true;
    }

    void write_string_value(const string_view_type& sv) 
    {
        auto result = unicons::validate(sv.begin(), sv.end());
        if (result.ec != unicons::conv_errc())
        {
            throw ser_error(msgpack_errc::invalid_utf8_text_string);
        }

        const size_t length = sv.length();
        if (length <= 31)
        {
            // fixstr stores a byte array whose length is upto 31 bytes
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(jsoncons::msgpack::detail::msgpack_format ::fixstr_base_cd | length), std::back_inserter(result_));
        }
        else if (length <= (std::numeric_limits<uint8_t>::max)())
        {
            // str 8 stores a byte array whose length is upto (2^8)-1 bytes
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(jsoncons::msgpack::detail::msgpack_format ::str8_cd), std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(length), std::back_inserter(result_));
        }
        else if (length <= (std::numeric_limits<uint16_t>::max)())
        {
            // str 16 stores a byte array whose length is upto (2^16)-1 bytes
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(jsoncons::msgpack::detail::msgpack_format ::str16_cd), std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint16_t>(length), std::back_inserter(result_));
        }
        else if (length <= (std::numeric_limits<uint32_t>::max)())
        {
            // str 32 stores a byte array whose length is upto (2^32)-1 bytes
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(jsoncons::msgpack::detail::msgpack_format ::str32_cd), std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint32_t>(length),std::back_inserter(result_));
        }

        for (auto c : sv)
        {
            result_.push_back(c);
        }
    }

    bool do_byte_string_value(const byte_string_view& b, 
                              semantic_tag, 
                              const ser_context&) override
    {

        const size_t length = b.length();
        if (length <= (std::numeric_limits<uint8_t>::max)())
        {
            // str 8 stores a byte array whose length is upto (2^8)-1 bytes
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(jsoncons::msgpack::detail::msgpack_format ::bin8_cd), std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(length), std::back_inserter(result_));
        }
        else if (length <= (std::numeric_limits<uint16_t>::max)())
        {
            // str 16 stores a byte array whose length is upto (2^16)-1 bytes
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(jsoncons::msgpack::detail::msgpack_format ::bin16_cd), std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint16_t>(length), std::back_inserter(result_));
        }
        else if (length <= (std::numeric_limits<uint32_t>::max)())
        {
            // str 32 stores a byte array whose length is upto (2^32)-1 bytes
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(jsoncons::msgpack::detail::msgpack_format ::bin32_cd), std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint32_t>(length),std::back_inserter(result_));
        }

        for (auto c : b)
        {
            result_.push_back(c);
        }

        end_value();
        return true;
    }

    bool do_double_value(double val, 
                         semantic_tag,
                         const ser_context&) override
    {
        float valf = (float)val;
        if ((double)valf == val)
        {
            // float 32
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(jsoncons::msgpack::detail::msgpack_format ::float32_cd), std::back_inserter(result_));
            jsoncons::detail::to_big_endian(valf,std::back_inserter(result_));
        }
        else
        {
            // float 64
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(jsoncons::msgpack::detail::msgpack_format ::float64_cd), std::back_inserter(result_));
            jsoncons::detail::to_big_endian(val,std::back_inserter(result_));
        }

        // write double

        end_value();
        return true;
    }

    bool do_int64_value(int64_t val, 
                        semantic_tag, 
                        const ser_context&) override
    {
        if (val >= 0)
        {
            if (val <= 0x7f)
            {
                // positive fixnum stores 7-bit positive integer
                jsoncons::detail::to_big_endian(static_cast<uint8_t>(val),std::back_inserter(result_));
            }
            else if (val <= (std::numeric_limits<uint8_t>::max)())
            {
                // uint 8 stores a 8-bit unsigned integer
                jsoncons::detail::to_big_endian(static_cast<uint8_t>(jsoncons::msgpack::detail::msgpack_format ::uint8_cd), std::back_inserter(result_));
                jsoncons::detail::to_big_endian(static_cast<uint8_t>(val),std::back_inserter(result_));
            }
            else if (val <= (std::numeric_limits<uint16_t>::max)())
            {
                // uint 16 stores a 16-bit big-endian unsigned integer
                jsoncons::detail::to_big_endian(static_cast<uint8_t>(jsoncons::msgpack::detail::msgpack_format ::uint16_cd), std::back_inserter(result_));
                jsoncons::detail::to_big_endian(static_cast<uint16_t>(val),std::back_inserter(result_));
            }
            else if (val <= (std::numeric_limits<uint32_t>::max)())
            {
                // uint 32 stores a 32-bit big-endian unsigned integer
                jsoncons::detail::to_big_endian(static_cast<uint8_t>(jsoncons::msgpack::detail::msgpack_format ::uint32_cd), std::back_inserter(result_));
                jsoncons::detail::to_big_endian(static_cast<uint32_t>(val),std::back_inserter(result_));
            }
            else if (val <= (std::numeric_limits<int64_t>::max)())
            {
                // int 64 stores a 64-bit big-endian signed integer
                jsoncons::detail::to_big_endian(static_cast<uint8_t>(jsoncons::msgpack::detail::msgpack_format ::uint64_cd), std::back_inserter(result_));
                jsoncons::detail::to_big_endian(static_cast<uint64_t>(val),std::back_inserter(result_));
            }
        }
        else
        {
            if (val >= -32)
            {
                // negative fixnum stores 5-bit negative integer
                jsoncons::detail::to_big_endian(static_cast<int8_t>(val), std::back_inserter(result_));
            }
            else if (val >= (std::numeric_limits<int8_t>::lowest)())
            {
                // int 8 stores a 8-bit signed integer
                jsoncons::detail::to_big_endian(static_cast<uint8_t>(jsoncons::msgpack::detail::msgpack_format ::int8_cd), std::back_inserter(result_));
                jsoncons::detail::to_big_endian(static_cast<int8_t>(val),std::back_inserter(result_));
            }
            else if (val >= (std::numeric_limits<int16_t>::lowest)())
            {
                // int 16 stores a 16-bit big-endian signed integer
                jsoncons::detail::to_big_endian(static_cast<uint8_t>(jsoncons::msgpack::detail::msgpack_format ::int16_cd), std::back_inserter(result_));
                jsoncons::detail::to_big_endian(static_cast<int16_t>(val),std::back_inserter(result_));
            }
            else if (val >= (std::numeric_limits<int32_t>::lowest)())
            {
                // int 32 stores a 32-bit big-endian signed integer
                jsoncons::detail::to_big_endian(static_cast<uint8_t>(jsoncons::msgpack::detail::msgpack_format ::int32_cd), std::back_inserter(result_));
                jsoncons::detail::to_big_endian(static_cast<int32_t>(val),std::back_inserter(result_));
            }
            else if (val >= (std::numeric_limits<int64_t>::lowest)())
            {
                // int 64 stores a 64-bit big-endian signed integer
                jsoncons::detail::to_big_endian(static_cast<uint8_t>(jsoncons::msgpack::detail::msgpack_format ::int64_cd), std::back_inserter(result_));
                jsoncons::detail::to_big_endian(static_cast<int64_t>(val),std::back_inserter(result_));
            }
        }
        end_value();
        return true;
    }

    bool do_uint64_value(uint64_t val, 
                         semantic_tag, 
                         const ser_context&) override
    {
        if (val <= (std::numeric_limits<int8_t>::max)())
        {
            // positive fixnum stores 7-bit positive integer
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(val), std::back_inserter(result_));
        }
        else if (val <= (std::numeric_limits<uint8_t>::max)())
        {
            // uint 8 stores a 8-bit unsigned integer
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(jsoncons::msgpack::detail::msgpack_format ::uint8_cd), std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(val), std::back_inserter(result_));
        }
        else if (val <= (std::numeric_limits<uint16_t>::max)())
        {
            // uint 16 stores a 16-bit big-endian unsigned integer
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(jsoncons::msgpack::detail::msgpack_format ::uint16_cd), std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint16_t>(val),std::back_inserter(result_));
        }
        else if (val <= (std::numeric_limits<uint32_t>::max)())
        {
            // uint 32 stores a 32-bit big-endian unsigned integer
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(jsoncons::msgpack::detail::msgpack_format ::uint32_cd), std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint32_t>(val),std::back_inserter(result_));
        }
        else if (val <= (std::numeric_limits<uint64_t>::max)())
        {
            // uint 64 stores a 64-bit big-endian unsigned integer
            jsoncons::detail::to_big_endian(static_cast<uint8_t>(jsoncons::msgpack::detail::msgpack_format ::uint64_cd), std::back_inserter(result_));
            jsoncons::detail::to_big_endian(static_cast<uint64_t>(val),std::back_inserter(result_));
        }
        end_value();
        return true;
    }

    bool do_bool_value(bool val, semantic_tag, const ser_context&) override
    {
        // true and false
        result_.push_back(static_cast<uint8_t>(val ? jsoncons::msgpack::detail::msgpack_format ::true_cd : jsoncons::msgpack::detail::msgpack_format ::false_cd));
        //jsoncons::detail::to_big_endian(static_cast<uint8_t>(val ? jsoncons::msgpack::detail::msgpack_format ::true_cd : jsoncons::msgpack::detail::msgpack_format ::false_cd), std::back_inserter(result_));

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

typedef basic_msgpack_encoder<jsoncons::binary_stream_result> msgpack_encoder;
typedef basic_msgpack_encoder<jsoncons::bytes_result> msgpack_bytes_encoder;

#if !defined(JSONCONS_NO_DEPRECATED)
typedef basic_msgpack_encoder<jsoncons::bytes_result> msgpack_bytes_serializer;

template<class Result=jsoncons::binary_stream_result>
using basic_msgpack_serializer = basic_msgpack_encoder<Result>; 

typedef basic_msgpack_serializer<jsoncons::binary_stream_result> msgpack_serializer;
typedef basic_msgpack_serializer<jsoncons::bytes_result> msgpack_buffer_serializer;
#endif

}}
#endif
