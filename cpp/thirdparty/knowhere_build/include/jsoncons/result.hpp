// Copyright 2018 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_RESULT_HPP
#define JSONCONS_RESULT_HPP

#include <stdexcept>
#include <string>
#include <vector>
#include <ostream>
#include <cmath>
#include <exception>
#include <memory> // std::addressof
#include <cstring> // std::memcpy
#include <jsoncons/config/jsoncons_config.hpp>
#include <jsoncons/detail/type_traits.hpp>

namespace jsoncons { 

// stream_result

template <class CharT>
class stream_result
{
public:
    typedef CharT value_type;
    typedef std::basic_ostream<CharT> output_type;

private:
    static const size_t default_buffer_length = 16384;

    std::basic_ostream<CharT>* os_;
    std::vector<CharT> buffer_;
    CharT * begin_buffer_;
    const CharT* end_buffer_;
    CharT* p_;

    // Noncopyable
    stream_result(const stream_result&) = delete;
    stream_result& operator=(const stream_result&) = delete;

public:
    stream_result(stream_result&&) = default;

    stream_result(std::basic_ostream<CharT>& os)
        : os_(std::addressof(os)), buffer_(default_buffer_length), begin_buffer_(buffer_.data()), end_buffer_(begin_buffer_+buffer_.size()), p_(begin_buffer_)
    {
    }
    stream_result(std::basic_ostream<CharT>& os, size_t buflen)
    : os_(std::addressof(os)), buffer_(buflen), begin_buffer_(buffer_.data()), end_buffer_(begin_buffer_+buffer_.size()), p_(begin_buffer_)
    {
    }
    ~stream_result()
    {
        os_->write(begin_buffer_, buffer_length());
        os_->flush();
    }

    stream_result& operator=(stream_result&&) = default;

    void flush()
    {
        os_->write(begin_buffer_, buffer_length());
        os_->flush();
        p_ = buffer_.data();
    }

    void append(const CharT* s, size_t length)
    {
        size_t diff = end_buffer_ - p_;
        if (diff >= length)
        {
            std::memcpy(p_, s, length*sizeof(CharT));
            p_ += length;
        }
        else
        {
            os_->write(begin_buffer_, buffer_length());
            os_->write(s,length);
            p_ = begin_buffer_;
        }
    }

    void push_back(CharT ch)
    {
        if (p_ < end_buffer_)
        {
            *p_++ = ch;
        }
        else
        {
            os_->write(begin_buffer_, buffer_length());
            p_ = begin_buffer_;
            push_back(ch);
        }
    }
private:

    size_t buffer_length() const
    {
        return p_ - begin_buffer_;
    }
};

// binary_stream_result

class binary_stream_result
{
public:
    typedef uint8_t value_type;
    typedef std::basic_ostream<char> output_type;
private:
    static const size_t default_buffer_length = 16384;

    std::basic_ostream<char>* os_;
    std::vector<uint8_t> buffer_;
    uint8_t * begin_buffer_;
    const uint8_t* end_buffer_;
    uint8_t* p_;

    // Noncopyable
    binary_stream_result(const binary_stream_result&) = delete;
    binary_stream_result& operator=(const binary_stream_result&) = delete;

public:
    binary_stream_result(binary_stream_result&&) = default;

    binary_stream_result(std::basic_ostream<char>& os)
        : os_(std::addressof(os)), 
          buffer_(default_buffer_length), 
          begin_buffer_(buffer_.data()), 
          end_buffer_(begin_buffer_+buffer_.size()), 
          p_(begin_buffer_)
    {
    }
    binary_stream_result(std::basic_ostream<char>& os, size_t buflen)
        : os_(std::addressof(os)), 
          buffer_(buflen), 
          begin_buffer_(buffer_.data()), 
          end_buffer_(begin_buffer_+buffer_.size()), 
          p_(begin_buffer_)
    {
    }
    ~binary_stream_result()
    {
        os_->write((char*)begin_buffer_, buffer_length());
        os_->flush();
    }

    binary_stream_result& operator=(binary_stream_result&&) = default;

    void flush()
    {
        os_->write((char*)begin_buffer_, buffer_length());
        p_ = buffer_.data();
    }

    void append(const uint8_t* s, size_t length)
    {
        size_t diff = end_buffer_ - p_;
        if (diff >= length)
        {
            std::memcpy(p_, s, length*sizeof(uint8_t));
            p_ += length;
        }
        else
        {
            os_->write((char*)begin_buffer_, buffer_length());
            os_->write((const char*)s,length);
            p_ = begin_buffer_;
        }
    }

    void push_back(uint8_t ch)
    {
        if (p_ < end_buffer_)
        {
            *p_++ = ch;
        }
        else
        {
            os_->write((char*)begin_buffer_, buffer_length());
            p_ = begin_buffer_;
            push_back(ch);
        }
    }
private:

    size_t buffer_length() const
    {
        return p_ - begin_buffer_;
    }
};

// string_result

template <class StringT>
class string_result 
{
public:
    typedef typename StringT::value_type value_type;
    typedef StringT output_type;
private:
    output_type* s_;

    // Noncopyable
    string_result(const string_result&) = delete;
    string_result& operator=(const string_result&) = delete;
public:
    string_result(string_result&& val)
        : s_(nullptr)
    {
        std::swap(s_,val.s_);
    }

    string_result(output_type& s)
        : s_(std::addressof(s))
    {
    }

    string_result& operator=(string_result&& val)
    {
        std::swap(s_, val.s_);
    }

    void flush()
    {
    }

    void append(const value_type* s, size_t length)
    {
        s_->insert(s_->end(), s, s+length);
    }

    void push_back(value_type ch)
    {
        s_->push_back(ch);
    }
};

// bytes_result

class bytes_result 
{
public:
    typedef uint8_t value_type;
    typedef std::vector<uint8_t> output_type;
private:
    output_type& s_;

    // Noncopyable
    bytes_result(const bytes_result&) = delete;
    bytes_result& operator=(const bytes_result&) = delete;
public:
    bytes_result(bytes_result&&) = default;

    bytes_result(output_type& s)
        : s_(s)
    {
    }

    bytes_result& operator=(bytes_result&&) = default;

    void flush()
    {
    }

    void append(const uint8_t* s, size_t length)
    {
        s_.insert(s_.end(), s, s+length);
    }

    void push_back(uint8_t ch)
    {
        s_.push_back(ch);
    }
};

}

#endif
