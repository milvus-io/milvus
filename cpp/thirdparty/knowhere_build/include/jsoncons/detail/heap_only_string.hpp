// Copyright 2013 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_DETAIL_HEAP_ONLY_STRING_HPP
#define JSONCONS_DETAIL_HEAP_ONLY_STRING_HPP

#include <stdexcept>
#include <string>
#include <exception>
#include <ostream>
#include <cstring> // std::memcpy
#include <memory> // std::allocator
#include <jsoncons/config/jsoncons_config.hpp>

namespace jsoncons { namespace detail {

template <class Allocator>
class heap_only_string_base
{
    Allocator allocator_;
public:
    Allocator& get_allocator() 
    {
        return allocator_;
    }

    const Allocator& get_allocator() const
    {
        return allocator_;
    }
protected:
    heap_only_string_base(const Allocator& allocator)
        : allocator_(allocator)
    {
    }

    ~heap_only_string_base() {}
};

template <class CharT,class Allocator>
class heap_only_string_factory;

template <class CharT, class Allocator>
class heap_only_string : public heap_only_string_base<Allocator>
{
    typedef typename std::allocator_traits<Allocator>::template rebind_alloc<CharT> allocator_type;  
    typedef std::allocator_traits<allocator_type> allocator_traits_type;
    typedef typename allocator_traits_type::pointer pointer;

    friend class heap_only_string_factory<CharT, Allocator>;
public:
    typedef CharT char_type;
    typedef heap_only_string<CharT,Allocator> value_type;

    ~heap_only_string() {}

    const char_type* c_str() const { return to_plain_pointer(p_); }
    const char_type* data() const { return to_plain_pointer(p_); }
    size_t length() const { return length_; }

    using heap_only_string_base<Allocator>::get_allocator;


    friend std::basic_ostream<CharT>& operator<<(std::basic_ostream<CharT>& os, const heap_only_string& s)
    {
        os.write(s.data(),s.length());
        return os;
    }
private:
    heap_only_string()
        : heap_only_string_base<Allocator>(Allocator())
    {

    }
    heap_only_string(const Allocator& allocator)
        : heap_only_string_base<Allocator>(allocator)
    {

    }

    pointer p_;
    size_t length_;

    heap_only_string(const heap_only_string&) = delete;
    heap_only_string& operator=(const heap_only_string&) = delete;

};

template <class CharT, class Allocator>
class heap_only_string_factory
{
    typedef CharT char_type;
    typedef typename std::allocator_traits<Allocator>::template rebind_alloc<char> byte_allocator_type;  
    typedef std::allocator_traits<byte_allocator_type> byte_allocator_traits_type;
    typedef typename byte_allocator_traits_type::pointer byte_pointer;
    typedef typename heap_only_string<CharT,Allocator>::pointer pointer;
public:

    typedef typename std::allocator_traits<Allocator>::template rebind_alloc<heap_only_string<CharT,Allocator>> string_allocator_type;  
    typedef std::allocator_traits<string_allocator_type> string_allocator_traits_type;
    typedef typename string_allocator_traits_type::pointer string_pointer;

    typedef heap_only_string<char_type,Allocator>* raw_string_pointer_type;
 
    struct string_storage
    {
        heap_only_string<CharT,Allocator> data;
        char_type c[1];
    };
    typedef typename std::aligned_storage<sizeof(string_storage), alignof(string_storage)>::type storage_type;

    static size_t aligned_size(size_t n)
    {
        return sizeof(storage_type) + n;
    }
public:
    static string_pointer create(const char_type* s, size_t length)
    {
        return create(s, length, Allocator());
    }
    static string_pointer create(const char_type* s, size_t length, const Allocator& allocator)
    {
        size_t mem_size = aligned_size(length*sizeof(char_type));

        byte_allocator_type alloc(allocator);
        byte_pointer ptr = alloc.allocate(mem_size);

        char* storage = to_plain_pointer(ptr);
        raw_string_pointer_type ps = new(storage)heap_only_string<char_type,Allocator>(alloc);
        auto psa = reinterpret_cast<string_storage*>(storage); 

        CharT* p = new(&psa->c)char_type[length + 1];
        std::memcpy(p, s, length*sizeof(char_type));
        p[length] = 0;
        ps->p_ = std::pointer_traits<pointer>::pointer_to(*p);
        ps->length_ = length;
        return std::pointer_traits<string_pointer>::pointer_to(*ps);
    }

    static void destroy(string_pointer ptr)
    {
        raw_string_pointer_type rawp = to_plain_pointer(ptr);
        char* p = reinterpret_cast<char*>(rawp);
        size_t mem_size = aligned_size(ptr->length_*sizeof(char_type));
        byte_allocator_type alloc(ptr->get_allocator());
        alloc.deallocate(p,mem_size);
    }
};


}}

#endif
