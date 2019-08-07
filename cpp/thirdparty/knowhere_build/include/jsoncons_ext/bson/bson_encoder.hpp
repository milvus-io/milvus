// Copyright 2018 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_BSON_BSON_ENCODER_HPP
#define JSONCONS_BSON_BSON_ENCODER_HPP

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
#include <jsoncons_ext/bson/bson_detail.hpp>

namespace jsoncons { namespace bson {

template<class Result=jsoncons::binary_stream_result>
class basic_bson_encoder final : public basic_json_content_handler<char>
{
    enum class decimal_parse_state { start, integer, exp1, exp2, fraction1 };
public:
    typedef char char_type;
    using typename basic_json_content_handler<char>::string_view_type;
    typedef Result result_type;

private:
    struct stack_item
    {
        jsoncons::bson::detail::bson_container_type type_;
        size_t offset_;
        size_t name_offset_;
        size_t index_;

        stack_item(jsoncons::bson::detail::bson_container_type type, size_t offset)
           : type_(type), offset_(offset), name_offset_(0), index_(0)
        {
        }

        size_t offset() const
        {
            return offset_;
        }

        size_t member_offset() const
        {
            return name_offset_;
        }

        void member_offset(size_t offset) 
        {
            name_offset_ = offset;
        }

        size_t next_index()
        {
            return index_++;
        }

        bool is_object() const
        {
            return type_ == jsoncons::bson::detail::bson_container_type::document;
        }


    };

    std::vector<stack_item> stack_;
    std::vector<uint8_t> buffer_;
    result_type result_;

    // Noncopyable and nonmoveable
    basic_bson_encoder(const basic_bson_encoder&) = delete;
    basic_bson_encoder& operator=(const basic_bson_encoder&) = delete;
public:
    explicit basic_bson_encoder(result_type result)
       : result_(std::move(result))
    {
    }

    ~basic_bson_encoder()
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
        if (buffer_.size() > 0)
        {
            before_value(jsoncons::bson::detail::bson_format::document_cd);
        }
        stack_.emplace_back(jsoncons::bson::detail::bson_container_type::document, buffer_.size());
        buffer_.insert(buffer_.end(), sizeof(int32_t), 0);

        return true;
    }

    bool do_end_object(const ser_context&) override
    {
        JSONCONS_ASSERT(!stack_.empty());

        buffer_.push_back(0x00);

        size_t length = buffer_.size() - stack_.back().offset();
        jsoncons::detail::to_little_endian(static_cast<uint32_t>(length), buffer_.begin()+stack_.back().offset());

        stack_.pop_back();
        if (stack_.empty())
        {
            for (auto c : buffer_)
            {
                result_.push_back(c);
            }
        }
        return true;
    }

    bool do_begin_array(semantic_tag, const ser_context&) override
    {
        if (buffer_.size() > 0)
        {
            before_value(jsoncons::bson::detail::bson_format::array_cd);
        }
        stack_.emplace_back(jsoncons::bson::detail::bson_container_type::array, buffer_.size());
        buffer_.insert(buffer_.end(), sizeof(int32_t), 0);
        return true;
    }

    bool do_end_array(const ser_context&) override
    {
        JSONCONS_ASSERT(!stack_.empty());

        buffer_.push_back(0x00);

        size_t length = buffer_.size() - stack_.back().offset();
        jsoncons::detail::to_little_endian(static_cast<uint32_t>(length), buffer_.begin()+stack_.back().offset());

        stack_.pop_back();
        if (stack_.empty())
        {
            for (auto c : buffer_)
            {
                result_.push_back(c);
            }
        }
        return true;
    }

    bool do_name(const string_view_type& name, const ser_context&) override
    {
        stack_.back().member_offset(buffer_.size());
        buffer_.push_back(0x00); // reserve space for code
        for (auto c : name)
        {
            buffer_.push_back(c);
        }
        buffer_.push_back(0x00);
        return true;
    }

    bool do_null_value(semantic_tag, const ser_context&) override
    {
        before_value(jsoncons::bson::detail::bson_format::null_cd);
        return true;
    }

    bool do_bool_value(bool val, semantic_tag, const ser_context&) override
    {
        before_value(jsoncons::bson::detail::bson_format::bool_cd);
        if (val)
        {
            buffer_.push_back(0x01);
        }
        else
        {
            buffer_.push_back(0x00);
        }

        return true;
    }

    bool do_string_value(const string_view_type& sv, semantic_tag, const ser_context&) override
    {
        before_value(jsoncons::bson::detail::bson_format::string_cd);

        size_t offset = buffer_.size();
        buffer_.insert(buffer_.end(), sizeof(int32_t), 0);
        size_t string_offset = buffer_.size();

        auto result = unicons::validate(sv.begin(), sv.end());
        if (result.ec != unicons::conv_errc())
        {
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Illegal unicode"));
        }
        for (auto c : sv)
        {
            buffer_.push_back(c);
        }
        buffer_.push_back(0x00);
        size_t length = buffer_.size() - string_offset;
        jsoncons::detail::to_little_endian(static_cast<uint32_t>(length), buffer_.begin()+offset);

        return true;
    }

    bool do_byte_string_value(const byte_string_view& b, 
                              semantic_tag, 
                              const ser_context&) override
    {
        before_value(jsoncons::bson::detail::bson_format::binary_cd);

        size_t offset = buffer_.size();
        buffer_.insert(buffer_.end(), sizeof(int32_t), 0);
        size_t string_offset = buffer_.size();

        for (auto c : b)
        {
            buffer_.push_back(c);
        }
        size_t length = buffer_.size() - string_offset;
        jsoncons::detail::to_little_endian(static_cast<uint32_t>(length), buffer_.begin()+offset);

        return true;
    }

    bool do_int64_value(int64_t val, 
                        semantic_tag tag, 
                        const ser_context&) override
    {
        if (tag == semantic_tag::timestamp)
        {
            before_value(jsoncons::bson::detail::bson_format::datetime_cd);
        }
        else
        {
            before_value(jsoncons::bson::detail::bson_format::int64_cd);
        }
        if (val >= (std::numeric_limits<int32_t>::lowest)() && val <= (std::numeric_limits<int32_t>::max)())
        {
            jsoncons::detail::to_little_endian(static_cast<uint32_t>(val),std::back_inserter(buffer_));
        }
        else if (val >= (std::numeric_limits<int64_t>::lowest)() && val <= (std::numeric_limits<int64_t>::max)())
        {
            jsoncons::detail::to_little_endian(static_cast<int64_t>(val),std::back_inserter(buffer_));
        }
        else
        {
            // error
        }

        return true;
    }

    bool do_uint64_value(uint64_t val, 
                         semantic_tag tag, 
                         const ser_context&) override
    {
        if (tag == semantic_tag::timestamp)
        {
            before_value(jsoncons::bson::detail::bson_format::datetime_cd);
        }
        else
        {
            before_value(jsoncons::bson::detail::bson_format::int64_cd);
        }
        if (val <= (std::numeric_limits<int32_t>::max)())
        {
            jsoncons::detail::to_little_endian(static_cast<uint32_t>(val),std::back_inserter(buffer_));
        }
        else if (val <= (uint64_t)(std::numeric_limits<int64_t>::max)())
        {
            jsoncons::detail::to_little_endian(static_cast<uint64_t>(val),std::back_inserter(buffer_));
        }
        else
        {
            // error
        }

        return true;
    }

    bool do_double_value(double val, 
                         semantic_tag,
                         const ser_context&) override
    {
        before_value(jsoncons::bson::detail::bson_format::double_cd);

        jsoncons::detail::to_little_endian(val,std::back_inserter(buffer_));

        return true;
    }

    void before_value(uint8_t code) 
    {
        if (stack_.back().is_object())
        {
            buffer_[stack_.back().member_offset()] = code;
        }
        else
        {
            buffer_.push_back(code);
            std::string name = std::to_string(stack_.back().next_index());
            buffer_.insert(buffer_.end(), name.begin(), name.end());
            buffer_.push_back(0x00);
        }
    }
};

typedef basic_bson_encoder<jsoncons::binary_stream_result> bson_encoder;
typedef basic_bson_encoder<jsoncons::bytes_result> bson_bytes_encoder;

#if !defined(JSONCONS_NO_DEPRECATED)
template<class Result=jsoncons::binary_stream_result>
using basic_bson_serializer = basic_bson_encoder<Result>; 

typedef basic_bson_encoder<jsoncons::binary_stream_result> bson_serializer;
typedef basic_bson_encoder<jsoncons::bytes_result> bson_buffer_serializer;

#endif

}}
#endif
