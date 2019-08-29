// Copyright 2019 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_STRING_VIEW_HPP
#define JSONCONS_STRING_VIEW_HPP

#include <stdexcept>
#include <string>
#include <vector>
#include <ostream>
#include <cmath>
#include <algorithm> // std::find, std::min, std::reverse
#include <memory>
#include <iterator>
#include <exception>
#include <stdexcept>
#include <istream> // std::basic_istream

namespace jsoncons { namespace detail {

template <class CharT, class Traits = std::char_traits<CharT>>
class basic_string_view
{
private:
    const CharT* data_;
    size_t length_;
public:
    typedef CharT value_type;
    typedef const CharT& const_reference;
    typedef Traits traits_type;
    typedef std::size_t size_type;
    static const size_type npos = size_type(-1);
    typedef const CharT* const_iterator;
    typedef const CharT* iterator;
    typedef std::reverse_iterator<const_iterator> const_reverse_iterator;

    basic_string_view()
        : data_(nullptr), length_(0)
    {
    }
    basic_string_view(const CharT* data, size_t length)
        : data_(data), length_(length)
    {
    }
    basic_string_view(const CharT* data)
        : data_(data), length_(Traits::length(data))
    {
    }
    basic_string_view(const basic_string_view& other) = default;

    template <class Allocator>
    basic_string_view(const std::basic_string<CharT,Traits,Allocator>& s)
        : data_(s.data()), length_(s.length())
    {
    }

    template <class Allocator>
    explicit operator std::basic_string<CharT,Traits,Allocator>() const
    { 
        return std::basic_string<CharT,Traits>(data_,length_); 
    }

    // iterator support 
    const_iterator begin() const noexcept
    {
        return data_;
    }
    const_iterator end() const noexcept
    {
        return data_ + length_;
    }
    const_iterator cbegin() const noexcept 
    { 
        return data_; 
    }
    const_iterator cend() const noexcept 
    { 
        return data_ + length_; 
    }
    const_reverse_iterator rbegin() const noexcept 
    { 
        return const_reverse_iterator(end()); 
    }
    const_reverse_iterator rend() const noexcept 
    { 
        return const_reverse_iterator(begin()); 
    }
    const_reverse_iterator crbegin() const noexcept 
    { 
        return const_reverse_iterator(end()); 
    }
    const_reverse_iterator crend() const noexcept 
    { 
        return const_reverse_iterator(begin()); 
    }

    // capacity

    size_t size() const
    {
        return length_;
    }

    size_t length() const
    {
        return length_;
    }
    size_type max_size() const noexcept 
    { 
        return length_; 
    }
    bool empty() const noexcept 
    { 
        return length_ == 0; 
    }

    // element access

    const_reference operator[](size_type pos) const 
    { 
        return data_[pos]; 
    }

    const_reference at(size_t pos) const 
    {
        if (pos >= length_)
        {
            throw std::out_of_range("pos exceeds length");
        }
        return data_[pos];
    }

    const_reference front() const                
    { 
        return data_[0]; 
    }
    const_reference back()  const                
    { 
        return data_[length_-1]; 
    }

    const CharT* data() const
    {
        return data_;
    }

    // string operations

    basic_string_view substr(size_type pos, size_type n=npos) const 
    {
        if (pos > length_)
        {
            throw std::out_of_range("pos exceeds size");
        }
        if (n == npos || pos + n > length_)
        {
            n = length_ - pos;
        }
        return basic_string_view(data_ + pos, n);
    }

    int compare(const basic_string_view& s) const 
    {
        const int rc = Traits::compare(data_, s.data_, (std::min)(length_, s.length_));
        return rc != 0 ? rc : (length_ == s.length_ ? 0 : length_ < s.length_ ? -1 : 1);
    }

    int compare(const CharT* data) const 
    {
        const size_t length = Traits::length(data);
        const int rc = Traits::compare(data_, data, (std::min)(length_, length));
        return rc != 0 ? rc : (length_ == length? 0 : length_ < length? -1 : 1);
    }

    template <class Allocator>
    int compare(const std::basic_string<CharT,Traits,Allocator>& s) const 
    {
        const int rc = Traits::compare(data_, s.data(), (std::min)(length_, s.length()));
        return rc != 0 ? rc : (length_ == s.length() ? 0 : length_ < s.length() ? -1 : 1);
    }

    size_type find(basic_string_view s, size_type pos = 0) const noexcept 
    {
        if (pos > length_)
        {
            return npos;
        }
        if (s.length_ == 0)
        {
            return pos;
        }
        const_iterator it = std::search(cbegin() + pos, cend(),
                                        s.cbegin(), s.cend(), Traits::eq);
        return it == cend() ? npos : std::distance(cbegin(), it);
    }
    size_type find(CharT ch, size_type pos = 0) const noexcept
    { 
        return find(basic_string_view(&ch, 1), pos); 
    }
    size_type find(const CharT* s, size_type pos, size_type n) const noexcept
    { 
        return find(basic_string_view(s, n), pos); 
    }
    size_type find(const CharT* s, size_type pos = 0) const noexcept
    { 
        return find(basic_string_view(s), pos); 
    }

    size_type rfind(basic_string_view s, size_type pos = npos) const noexcept 
    {
        if (length_ < s.length_)
        {
            return npos;
        }
        if (pos > length_ - s.length_)
        {
            pos = length_ - s.length_;
        }
        if (s.length_ == 0) 
        {
            return pos;
        }
        for (const CharT* p = data_ + pos; true; --p) 
        {
            if (Traits::compare(p, s.data_, s.length_) == 0)
            {
                return p - data_;
            }
            if (p == data_)
            {
                return npos;
            }
         };
    }
    size_type rfind(CharT ch, size_type pos = npos) const noexcept
    { 
        return rfind(basic_string_view(&ch, 1), pos); 
    }
    size_type rfind(const CharT* s, size_type pos, size_type n) const noexcept
    { 
        return rfind(basic_string_view(s, n), pos); 
    }
    size_type rfind(const CharT* s, size_type pos = npos) const noexcept
    { 
        return rfind(basic_string_view(s), pos); 
    }

    size_type find_first_of(basic_string_view s, size_type pos = 0) const noexcept 
    {
        if (pos >= length_ || s.length_ == 0)
        {
            return npos;
        }
        const_iterator it = std::find_first_of
            (cbegin() + pos, cend(), s.cbegin(), s.cend(), Traits::eq);
        return it == cend() ? npos : std::distance (cbegin(), it);
    }
    size_type find_first_of(CharT ch, size_type pos = 0) const noexcept
    {
         return find_first_of(basic_string_view(&ch, 1), pos); 
    }
    size_type find_first_of(const CharT* s, size_type pos, size_type n) const noexcept
    { 
        return find_first_of(basic_string_view(s, n), pos); 
    }
    size_type find_first_of(const CharT* s, size_type pos = 0) const noexcept
    { 
        return find_first_of(basic_string_view(s), pos); 
    }

    size_type find_last_of(basic_string_view s, size_type pos = npos) const noexcept 
    {
        if (s.length_ == 0)
        {
            return npos;
        }
        if (pos >= length_)
        {
            pos = 0;
        }
        else
        {
            pos = length_ - (pos+1);
        }
        const_reverse_iterator it = std::find_first_of
            (crbegin() + pos, crend(), s.cbegin(), s.cend(), Traits::eq);
        return it == crend() ? npos : (length_ - 1 - std::distance(crbegin(), it));
    }
    size_type find_last_of(CharT ch, size_type pos = npos) const noexcept
    { 
        return find_last_of(basic_string_view(&ch, 1), pos); 
    }
    size_type find_last_of(const CharT* s, size_type pos, size_type n) const noexcept
    { 
        return find_last_of(basic_string_view(s, n), pos); 
    }
    size_type find_last_of(const CharT* s, size_type pos = npos) const noexcept
    { 
        return find_last_of(basic_string_view(s), pos); 
    }

    size_type find_first_not_of(basic_string_view s, size_type pos = 0) const noexcept 
    {
        if (pos >= length_)
            return npos;
        if (s.length_ == 0)
            return pos;

        const_iterator it = cend();
        for (auto p = cbegin()+pos; p != cend(); ++p)
        {
            if (Traits::find(s.data_, s.length_, *p) == 0)
            {
                it = p;
                break;
            }
        }
        return it == cend() ? npos : std::distance (cbegin(), it);
    }
    size_type find_first_not_of(CharT ch, size_type pos = 0) const noexcept
    { 
        return find_first_not_of(basic_string_view(&ch, 1), pos); 
    }
    size_type find_first_not_of(const CharT* s, size_type pos, size_type n) const noexcept
    { 
        return find_first_not_of(basic_string_view(s, n), pos); 
    }
    size_type find_first_not_of(const CharT* s, size_type pos = 0) const noexcept
    { 
        return find_first_not_of(basic_string_view(s), pos); 
    }

    size_type find_last_not_of(basic_string_view s, size_type pos = npos) const noexcept 
    {
        if (pos >= length_)
        {
            pos = length_ - 1;
        }
        if (s.length_ == 0)
        {
            return pos;
        }
        pos = length_ - (pos+1);

        const_iterator it = crend();
        for (auto p = crbegin()+pos; p != crend(); ++p)
        {
            if (Traits::find(s.data_, s.length_, *p) == 0)
            {
                it = p;
                break;
            }
        }
        return it == crend() ? npos : (length_ - 1 - std::distance(crbegin(), it));
    }
    size_type find_last_not_of(CharT ch, size_type pos = npos) const noexcept
    { 
        return find_last_not_of(basic_string_view(&ch, 1), pos); 
    }
    size_type find_last_not_of(const CharT* s, size_type pos, size_type n) const noexcept
    { 
        return find_last_not_of(basic_string_view(s, n), pos); 
    }
    size_type find_last_not_of(const CharT* s, size_type pos = npos) const noexcept
    { 
        return find_last_not_of(basic_string_view(s), pos); 
    }

    friend std::basic_ostream<CharT>& operator<<(std::basic_ostream<CharT>& os, const basic_string_view& sv)
    {
        os.write(sv.data_,sv.length_);
        return os;
    }
};

// ==
template<class CharT,class Traits>
bool operator==(const basic_string_view<CharT,Traits>& lhs, 
                const basic_string_view<CharT,Traits>& rhs)
{
    return lhs.compare(rhs) == 0;
}
template<class CharT,class Traits,class Allocator>
bool operator==(const basic_string_view<CharT,Traits>& lhs, 
                const std::basic_string<CharT,Traits,Allocator>& rhs)
{
    return lhs.compare(rhs) == 0;
}
template<class CharT,class Traits,class Allocator>
bool operator==(const std::basic_string<CharT,Traits,Allocator>& lhs, 
                const basic_string_view<CharT,Traits>& rhs)
{
    return rhs.compare(lhs) == 0;
}
template<class CharT,class Traits>
bool operator==(const basic_string_view<CharT,Traits>& lhs, 
                const CharT* rhs)
{
    return lhs.compare(rhs) == 0;
}
template<class CharT,class Traits>
bool operator==(const CharT* lhs, 
                const basic_string_view<CharT,Traits>& rhs)
{
    return rhs.compare(lhs) == 0;
}

// !=
template<class CharT,class Traits>
bool operator!=(const basic_string_view<CharT,Traits>& lhs, 
                const basic_string_view<CharT,Traits>& rhs)
{
    return lhs.compare(rhs) != 0;
}
template<class CharT,class Traits,class Allocator>
bool operator!=(const basic_string_view<CharT,Traits>& lhs, 
                const std::basic_string<CharT,Traits,Allocator>& rhs)
{
    return lhs.compare(rhs) != 0;
}
template<class CharT,class Traits,class Allocator>
bool operator!=(const std::basic_string<CharT,Traits,Allocator>& lhs, 
                const basic_string_view<CharT,Traits>& rhs)
{
    return rhs.compare(lhs) != 0;
}
template<class CharT,class Traits>
bool operator!=(const basic_string_view<CharT,Traits>& lhs, 
                const CharT* rhs)
{
    return lhs.compare(rhs) != 0;
}
template<class CharT,class Traits>
bool operator!=(const CharT* lhs, 
                const basic_string_view<CharT,Traits>& rhs)
{
    return rhs.compare(lhs) != 0;
}

// <=
template<class CharT,class Traits>
bool operator<=(const basic_string_view<CharT,Traits>& lhs, 
                const basic_string_view<CharT,Traits>& rhs)
{
    return lhs.compare(rhs) <= 0;
}
template<class CharT,class Traits,class Allocator>
bool operator<=(const basic_string_view<CharT,Traits>& lhs, 
                const std::basic_string<CharT,Traits,Allocator>& rhs)
{
    return lhs.compare(rhs) <= 0;
}
template<class CharT,class Traits,class Allocator>
bool operator<=(const std::basic_string<CharT,Traits,Allocator>& lhs, 
                const basic_string_view<CharT,Traits>& rhs)
{
    return rhs.compare(lhs) >= 0;
}

// <
template<class CharT,class Traits>
bool operator<(const basic_string_view<CharT,Traits>& lhs, 
                const basic_string_view<CharT,Traits>& rhs)
{
    return lhs.compare(rhs) < 0;
}
template<class CharT,class Traits,class Allocator>
bool operator<(const basic_string_view<CharT,Traits>& lhs, 
                const std::basic_string<CharT,Traits,Allocator>& rhs)
{
    return lhs.compare(rhs) < 0;
}
template<class CharT,class Traits,class Allocator>
bool operator<(const std::basic_string<CharT,Traits,Allocator>& lhs, 
                const basic_string_view<CharT,Traits>& rhs)
{
    return rhs.compare(lhs) > 0;
}

// >=
template<class CharT,class Traits>
bool operator>=(const basic_string_view<CharT,Traits>& lhs, 
                const basic_string_view<CharT,Traits>& rhs)
{
    return lhs.compare(rhs) >= 0;
}
template<class CharT,class Traits,class Allocator>
bool operator>=(const basic_string_view<CharT,Traits>& lhs, 
                const std::basic_string<CharT,Traits,Allocator>& rhs)
{
    return lhs.compare(rhs) >= 0;
}
template<class CharT,class Traits,class Allocator>
bool operator>=(const std::basic_string<CharT,Traits,Allocator>& lhs, 
                const basic_string_view<CharT,Traits>& rhs)
{
    return rhs.compare(lhs) <= 0;
}

// >
template<class CharT,class Traits>
bool operator>(const basic_string_view<CharT,Traits>& lhs, 
                const basic_string_view<CharT,Traits>& rhs)
{
    return lhs.compare(rhs) > 0;
}
template<class CharT,class Traits,class Allocator>
bool operator>(const basic_string_view<CharT,Traits>& lhs, 
                const std::basic_string<CharT,Traits,Allocator>& rhs)
{
    return lhs.compare(rhs) > 0;
}
template<class CharT,class Traits,class Allocator>
bool operator>(const std::basic_string<CharT,Traits,Allocator>& lhs, 
                const basic_string_view<CharT,Traits>& rhs)
{
    return rhs.compare(lhs) < 0;
}

}}

#endif
