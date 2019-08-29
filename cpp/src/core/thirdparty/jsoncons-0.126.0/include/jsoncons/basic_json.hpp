// Copyright 2013 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_BASIC_JSON_HPP
#define JSONCONS_BASIC_JSON_HPP

#include <limits> // std::numeric_limits
#include <string>
#include <vector>
#include <exception>
#include <cstring>
#include <ostream>
#include <memory> // std::allocator
#include <typeinfo>
#include <cstring> // std::memcpy
#include <algorithm> // std::swap
#include <initializer_list> // std::initializer_list
#include <utility> // std::move
#include <type_traits> // std::enable_if
#include <istream> // std::basic_istream
#include <jsoncons/json_fwd.hpp>
#include <jsoncons/config/version.hpp>
#include <jsoncons/json_exception.hpp>
#include <jsoncons/pretty_print.hpp>
#include <jsoncons/json_container_types.hpp>
#include <jsoncons/bignum.hpp>
#include <jsoncons/json_options.hpp>
#include <jsoncons/json_encoder.hpp>
#include <jsoncons/json_decoder.hpp>
#include <jsoncons/json_reader.hpp>
#include <jsoncons/json_type_traits.hpp>
#include <jsoncons/json_error.hpp>
#include <jsoncons/detail/heap_only_string.hpp>

namespace jsoncons {

struct sorted_policy 
{
    static constexpr bool preserve_order = false;

    template <class T,class Allocator>
    using sequence_container_type = std::vector<T,Allocator>;

    template <class CharT, class CharTraits, class Allocator>
    using key_storage = std::basic_string<CharT, CharTraits,Allocator>;

    template <class CharT, class CharTraits, class Allocator>
    using string_storage = std::basic_string<CharT, CharTraits,Allocator>;

    typedef default_parse_error_handler parse_error_handler_type;
};

struct preserve_order_policy : public sorted_policy
{
    static constexpr bool preserve_order = true;
};

template <typename IteratorT>
class range 
{
    IteratorT first_;
    IteratorT last_;
public:
    range(const IteratorT& first, const IteratorT& last)
        : first_(first), last_(last)
    {
    }

public:
    IteratorT begin()
    {
        return first_;
    }
    IteratorT end()
    {
        return last_;
    }
};

enum class storage_type : uint8_t 
{
    null_val = 0x00,
    bool_val = 0x01,
    int64_val = 0x02,
    uint64_val = 0x03,
    double_val = 0x04,
    short_string_val = 0x05,
    long_string_val = 0x06,
    byte_string_val = 0x07,
    array_val = 0x08,
    empty_object_val = 0x09,
    object_val = 0x0a,
    tag_val = 0x0b
};

template <class CharT, class ImplementationPolicy, class Allocator>
class basic_json
{
public:

    typedef Allocator allocator_type;

    typedef ImplementationPolicy implementation_policy;

    typedef typename ImplementationPolicy::parse_error_handler_type parse_error_handler_type;

    typedef CharT char_type;
    typedef typename std::char_traits<char_type> char_traits_type;
    typedef basic_string_view<char_type,char_traits_type> string_view_type;

    typedef typename std::allocator_traits<allocator_type>:: template rebind_alloc<char_type> char_allocator_type;

    typedef std::basic_string<CharT,char_traits_type,char_allocator_type> string_type;

    typedef basic_json<CharT,ImplementationPolicy,Allocator> json_type;

    typedef json_type& reference;
    typedef const json_type& const_reference;
    typedef json_type* pointer;
    typedef const json_type* const_pointer;

    typedef key_value<string_type,json_type> key_value_type;

#if !defined(JSONCONS_NO_DEPRECATED)
    typedef json_type value_type;
    typedef string_type key_type;
    typedef key_value_type kvp_type;
    typedef key_value_type member_type;
    typedef jsoncons::null_type null_type;
#endif

    typedef typename std::allocator_traits<allocator_type>:: template rebind_alloc<uint8_t> byte_allocator_type;
    using byte_string_storage_type = typename implementation_policy::template sequence_container_type<uint8_t, byte_allocator_type>;

    typedef json_array<basic_json> array;

    typedef typename std::allocator_traits<allocator_type>:: template rebind_alloc<key_value_type> key_value_allocator_type;

    typedef json_object<string_type,basic_json> object;

    typedef typename std::allocator_traits<Allocator>:: template rebind_alloc<array> array_allocator;
    typedef typename std::allocator_traits<Allocator>:: template rebind_alloc<object> object_allocator;

    typedef typename object::iterator object_iterator;
    typedef typename object::const_iterator const_object_iterator;
    typedef typename array::iterator array_iterator;
    typedef typename array::const_iterator const_array_iterator;

    struct variant
    {
        class data_base
        {
            static const uint8_t major_type_shift = 0x04;
            static const uint8_t additional_information_mask = (1U << 4) - 1;

            uint8_t type_;
        public:
            data_base(uint8_t type)
                : type_(type)
            {}

            data_base(storage_type data_type, semantic_tag semantic_type)
                : type_((static_cast<uint8_t>(data_type) << major_type_shift) | static_cast<uint8_t>(semantic_type))
            {}

            uint8_t type() const 
            {
                return type_;
            }

            storage_type get_storage_type() const 
            {

                uint8_t value = type_ >> major_type_shift;
                return static_cast<storage_type>(value);
            }

            semantic_tag get_semantic_tag() const 
            {
                uint8_t value = type_ & additional_information_mask;
                return static_cast<semantic_tag>(value);
            }
        };

        class null_data final : public data_base
        {
        public:
            null_data()
                : data_base(storage_type::null_val, semantic_tag::none)
            {
            }
            null_data(semantic_tag tag)
                : data_base(storage_type::null_val, tag)
            {
            }
        };

        class empty_object_data final : public data_base
        {
        public:
            empty_object_data(semantic_tag tag)
                : data_base(storage_type::empty_object_val, tag)
            {
            }
        };

        class bool_data final : public data_base
        {
            bool val_;
        public:
            bool_data(bool val, semantic_tag tag)
                : data_base(storage_type::bool_val, tag),val_(val)
            {
            }

            bool_data(const bool_data& val)
                : data_base(val.type()),val_(val.val_)
            {
            }

            bool value() const
            {
                return val_;
            }

        };

        class int64_data final : public data_base
        {
            int64_t val_;
        public:
            int64_data(int64_t val, 
                       semantic_tag tag = semantic_tag::none)
                : data_base(storage_type::int64_val, tag),val_(val)
            {
            }

            int64_data(const int64_data& val)
                : data_base(val.type()),val_(val.val_)
            {
            }

            int64_t value() const
            {
                return val_;
            }
        };

        class uint64_data final : public data_base
        {
            uint64_t val_;
        public:
            uint64_data(uint64_t val, 
                        semantic_tag tag = semantic_tag::none)
                : data_base(storage_type::uint64_val, tag),val_(val)
            {
            }

            uint64_data(const uint64_data& val)
                : data_base(val.type()),val_(val.val_)
            {
            }

            uint64_t value() const
            {
                return val_;
            }
        };

        class double_data final : public data_base
        {
            double val_;
        public:
            double_data(double val, 
                        semantic_tag tag = semantic_tag::none)
                : data_base(storage_type::double_val, tag), 
                  val_(val)
            {
            }

            double_data(const double_data& val)
                : data_base(val.type()),
                  val_(val.val_)
            {
            }

            double value() const
            {
                return val_;
            }
        };

        class short_string_data final : public data_base
        {
            static const size_t capacity = 14/sizeof(char_type);
            uint8_t length_;
            char_type data_[capacity];
        public:
            static const size_t max_length = (14 / sizeof(char_type)) - 1;

            short_string_data(semantic_tag tag, const char_type* p, uint8_t length)
                : data_base(storage_type::short_string_val, tag), length_(length)
            {
                JSONCONS_ASSERT(length <= max_length);
                std::memcpy(data_,p,length*sizeof(char_type));
                data_[length] = 0;
            }

            short_string_data(const short_string_data& val)
                : data_base(val.type()), length_(val.length_)
            {
                std::memcpy(data_,val.data_,val.length_*sizeof(char_type));
                data_[length_] = 0;
            }

            uint8_t length() const
            {
                return length_;
            }

            const char_type* data() const
            {
                return data_;
            }

            const char_type* c_str() const
            {
                return data_;
            }
        };

        // long_string_data
        class long_string_data final : public data_base
        {
            typedef typename jsoncons::detail::heap_only_string_factory<char_type, Allocator>::string_pointer pointer;

            pointer ptr_;
        public:

            long_string_data(semantic_tag tag, const char_type* data, size_t length, const Allocator& a)
                : data_base(storage_type::long_string_val, tag)
            {
                ptr_ = jsoncons::detail::heap_only_string_factory<char_type,Allocator>::create(data,length,a);
            }

            long_string_data(const long_string_data& val)
                : data_base(val.type())
            {
                ptr_ = jsoncons::detail::heap_only_string_factory<char_type,Allocator>::create(val.data(),val.length(),val.get_allocator());
            }

            long_string_data(long_string_data&& val)
                : data_base(val.type()), ptr_(nullptr)
            {
                std::swap(val.ptr_,ptr_);
            }

            long_string_data(const long_string_data& val, const Allocator& a)
                : data_base(val.type())
            {
                ptr_ = jsoncons::detail::heap_only_string_factory<char_type,Allocator>::create(val.data(),val.length(),a);
            }

            ~long_string_data()
            {
                if (ptr_ != nullptr)
                {
                    jsoncons::detail::heap_only_string_factory<char_type,Allocator>::destroy(ptr_);
                }
            }

            void swap(long_string_data& val)
            {
                std::swap(val.ptr_,ptr_);
            }

            const char_type* data() const
            {
                return ptr_->data();
            }

            const char_type* c_str() const
            {
                return ptr_->c_str();
            }

            size_t length() const
            {
                return ptr_->length();
            }

            allocator_type get_allocator() const
            {
                return ptr_->get_allocator();
            }
        };

        // byte_string_data
        class byte_string_data final : public data_base
        {
            typedef typename std::allocator_traits<Allocator>:: template rebind_alloc<byte_string_storage_type> string_holder_allocator_type;
            typedef typename std::allocator_traits<string_holder_allocator_type>::pointer pointer;

            pointer ptr_;

            template <typename... Args>
            void create(string_holder_allocator_type allocator, Args&& ... args)
            {
                typename std::allocator_traits<Allocator>:: template rebind_alloc<byte_string_storage_type> alloc(allocator);
                ptr_ = alloc.allocate(1);
                try
                {
                    std::allocator_traits<string_holder_allocator_type>:: template rebind_traits<byte_string_storage_type>::construct(alloc, jsoncons::detail::to_plain_pointer(ptr_), std::forward<Args>(args)...);
                }
                catch (...)
                {
                    alloc.deallocate(ptr_,1);
                    throw;
                }
            }
        public:

            byte_string_data(semantic_tag semantic_type, 
                             const uint8_t* data, size_t length, 
                             const Allocator& a)
                : data_base(storage_type::byte_string_val, semantic_type)
            {
                create(string_holder_allocator_type(a), data, data+length, a);
            }

            byte_string_data(const byte_string_data& val)
                : data_base(val.type())
            {
                create(val.ptr_->get_allocator(), *(val.ptr_));
            }

            byte_string_data(byte_string_data&& val)
                : data_base(val.type()), ptr_(nullptr)
            {
                std::swap(val.ptr_,ptr_);
            }

            byte_string_data(const byte_string_data& val, const Allocator& a)
                : data_base(val.type())
            { 
                create(string_holder_allocator_type(a), *(val.ptr_), a);
            }

            ~byte_string_data()
            {
                if (ptr_ != nullptr)
                {
                    typename std::allocator_traits<string_holder_allocator_type>:: template rebind_alloc<byte_string_storage_type> alloc(ptr_->get_allocator());
                    std::allocator_traits<string_holder_allocator_type>:: template rebind_traits<byte_string_storage_type>::destroy(alloc, jsoncons::detail::to_plain_pointer(ptr_));
                    alloc.deallocate(ptr_,1);
                }
            }

            void swap(byte_string_data& val)
            {
                std::swap(val.ptr_,ptr_);
            }

            const uint8_t* data() const
            {
                return ptr_->data();
            }

            size_t length() const
            {
                return ptr_->size();
            }

            const uint8_t* begin() const
            {
                return ptr_->data();
            }

            const uint8_t* end() const
            {
                return ptr_->data() + ptr_->size();
            }

            allocator_type get_allocator() const
            {
                return ptr_->get_allocator();
            }
        };

        // array_data
        class array_data final : public data_base
        {
            typedef typename std::allocator_traits<array_allocator>::pointer pointer;
            pointer ptr_;

            template <typename... Args>
            void create(array_allocator allocator, Args&& ... args)
            {
                typename std::allocator_traits<Allocator>:: template rebind_alloc<array> alloc(allocator);
                ptr_ = alloc.allocate(1);
                try
                {
                    std::allocator_traits<array_allocator>:: template rebind_traits<array>::construct(alloc, jsoncons::detail::to_plain_pointer(ptr_), std::forward<Args>(args)...);
                }
                catch (...)
                {
                    alloc.deallocate(ptr_,1);
                    throw;
                }
            }
        public:
            array_data(const array& val, semantic_tag tag)
                : data_base(storage_type::array_val, tag)
            {
                create(val.get_allocator(), val);
            }

            array_data(const array& val, semantic_tag tag, const Allocator& a)
                : data_base(storage_type::array_val, tag)
            {
                create(array_allocator(a), val, a);
            }

            array_data(const array_data& val)
                : data_base(val.type())
            {
                create(val.ptr_->get_allocator(), *(val.ptr_));
            }

            array_data(array_data&& val)
                : data_base(val.type()), ptr_(nullptr)
            {
                std::swap(val.ptr_, ptr_);
            }

            array_data(const array_data& val, const Allocator& a)
                : data_base(val.type())
            {
                create(array_allocator(a), *(val.ptr_), a);
            }
            ~array_data()
            {
                if (ptr_ != nullptr)
                {
                    typename std::allocator_traits<array_allocator>:: template rebind_alloc<array> alloc(ptr_->get_allocator());
                    std::allocator_traits<array_allocator>:: template rebind_traits<array>::destroy(alloc, jsoncons::detail::to_plain_pointer(ptr_));
                    alloc.deallocate(ptr_,1);
                }
            }

            allocator_type get_allocator() const
            {
                return ptr_->get_allocator();
            }

            void swap(array_data& val)
            {
                std::swap(val.ptr_,ptr_);
            }

            array& value()
            {
                return *ptr_;
            }

            const array& value() const
            {
                return *ptr_;
            }
        };

        // object_data
        class object_data final : public data_base
        {
            typedef typename std::allocator_traits<object_allocator>::pointer pointer;
            pointer ptr_;

            template <typename... Args>
            void create(Allocator allocator, Args&& ... args)
            {
                typename std::allocator_traits<object_allocator>:: template rebind_alloc<object> alloc(allocator);
                ptr_ = alloc.allocate(1);
                try
                {
                    std::allocator_traits<object_allocator>:: template rebind_traits<object>::construct(alloc, jsoncons::detail::to_plain_pointer(ptr_), std::forward<Args>(args)...);
                }
                catch (...)
                {
                    alloc.deallocate(ptr_,1);
                    throw;
                }
            }
        public:
            explicit object_data(const object& val, semantic_tag tag)
                : data_base(storage_type::object_val, tag)
            {
                create(val.get_allocator(), val);
            }

            explicit object_data(const object& val, semantic_tag tag, const Allocator& a)
                : data_base(storage_type::object_val, tag)
            {
                create(object_allocator(a), val, a);
            }

            explicit object_data(const object_data& val)
                : data_base(val.type())
            {
                create(val.ptr_->get_allocator(), *(val.ptr_));
            }

            explicit object_data(object_data&& val)
                : data_base(val.type()), ptr_(nullptr)
            {
                std::swap(val.ptr_,ptr_);
            }

            explicit object_data(const object_data& val, const Allocator& a)
                : data_base(val.type())
            {
                create(object_allocator(a), *(val.ptr_), a);
            }

            ~object_data()
            {
                if (ptr_ != nullptr)
                {
                    typename std::allocator_traits<Allocator>:: template rebind_alloc<object> alloc(ptr_->get_allocator());
                    std::allocator_traits<Allocator>:: template rebind_traits<object>::destroy(alloc, jsoncons::detail::to_plain_pointer(ptr_));
                    alloc.deallocate(ptr_,1);
                }
            }

            void swap(object_data& val)
            {
                std::swap(val.ptr_,ptr_);
            }

            object& value()
            {
                return *ptr_;
            }

            const object& value() const
            {
                return *ptr_;
            }

            allocator_type get_allocator() const
            {
                return ptr_->get_allocator();
            }
        };

    private:
        static const size_t data_size = static_max<sizeof(uint64_data),sizeof(double_data),sizeof(short_string_data), sizeof(long_string_data), sizeof(array_data), sizeof(object_data)>::value;
        static const size_t data_align = static_max<alignof(uint64_data),alignof(double_data),alignof(short_string_data),alignof(long_string_data),alignof(array_data),alignof(object_data)>::value;

        typedef typename std::aligned_storage<data_size,data_align>::type data_t;

        data_t data_;
    public:
        variant(semantic_tag tag)
        {
            new(reinterpret_cast<void*>(&data_))empty_object_data(tag);
        }

        explicit variant(null_type, semantic_tag tag)
        {
            new(reinterpret_cast<void*>(&data_))null_data(tag);
        }

        explicit variant(bool val, semantic_tag tag)
        {
            new(reinterpret_cast<void*>(&data_))bool_data(val,tag);
        }
        explicit variant(int64_t val, semantic_tag tag)
        {
            new(reinterpret_cast<void*>(&data_))int64_data(val, tag);
        }
        explicit variant(uint64_t val, semantic_tag tag)
        {
            new(reinterpret_cast<void*>(&data_))uint64_data(val, tag);
        }

        variant(double val, semantic_tag tag)
        {
            new(reinterpret_cast<void*>(&data_))double_data(val, tag);
        }

        variant(const char_type* s, size_t length, semantic_tag tag)
        {
            if (length <= short_string_data::max_length)
            {
                new(reinterpret_cast<void*>(&data_))short_string_data(tag, s, static_cast<uint8_t>(length));
            }
            else
            {
                new(reinterpret_cast<void*>(&data_))long_string_data(tag, s, length, char_allocator_type());
            }
        }

        variant(const char_type* s, size_t length, semantic_tag tag, const Allocator& alloc)
        {
            if (length <= short_string_data::max_length)
            {
                new(reinterpret_cast<void*>(&data_))short_string_data(tag, s, static_cast<uint8_t>(length));
            }
            else
            {
                new(reinterpret_cast<void*>(&data_))long_string_data(tag, s, length, char_allocator_type(alloc));
            }
        }

        variant(const byte_string_view& bs, semantic_tag tag)
        {
            new(reinterpret_cast<void*>(&data_))byte_string_data(tag, bs.data(), bs.length(), byte_allocator_type());
        }

        variant(const byte_string_view& bs, semantic_tag tag, const Allocator& allocator)
        {
            new(reinterpret_cast<void*>(&data_))byte_string_data(tag, bs.data(), bs.length(), allocator);
        }

        variant(const basic_bignum<byte_allocator_type>& n)
        {
            std::basic_string<CharT> s;
            n.dump(s);

            if (s.length() <= short_string_data::max_length)
            {
                new(reinterpret_cast<void*>(&data_))short_string_data(semantic_tag::bigint, s.data(), static_cast<uint8_t>(s.length()));
            }
            else
            {
                new(reinterpret_cast<void*>(&data_))long_string_data(semantic_tag::bigint, s.data(), s.length(), char_allocator_type());
            }
        }

        variant(const basic_bignum<byte_allocator_type>& n, const Allocator& allocator)
        {
            std::basic_string<CharT> s;
            n.dump(s);

            if (s.length() <= short_string_data::max_length)
            {
                new(reinterpret_cast<void*>(&data_))short_string_data(semantic_tag::bigint, s.data(), static_cast<uint8_t>(s.length()));
            }
            else
            {
                new(reinterpret_cast<void*>(&data_))long_string_data(semantic_tag::bigint, s.data(), s.length(), char_allocator_type(allocator));
            }
        }
        variant(const object& val, semantic_tag tag)
        {
            new(reinterpret_cast<void*>(&data_))object_data(val, tag);
        }
        variant(const object& val, semantic_tag tag, const Allocator& alloc)
        {
            new(reinterpret_cast<void*>(&data_))object_data(val, tag, alloc);
        }
        variant(const array& val, semantic_tag tag)
        {
            new(reinterpret_cast<void*>(&data_))array_data(val, tag);
        }
        variant(const array& val, semantic_tag tag, const Allocator& alloc)
        {
            new(reinterpret_cast<void*>(&data_))array_data(val, tag, alloc);
        }

        variant(const variant& val)
        {
            Init_(val);
        }

        variant(const variant& val, const Allocator& allocator)
        {
            Init_(val,allocator);
        }

        variant(variant&& val) noexcept
        {
            Init_rv_(std::forward<variant>(val));
        }

        variant(variant&& val, const Allocator& allocator) noexcept
        {
            Init_rv_(std::forward<variant>(val), allocator,
                     typename std::allocator_traits<Allocator>::propagate_on_container_move_assignment());
        }

        ~variant()
        {
            Destroy_();
        }

        void Destroy_()
        {
            switch (get_storage_type())
            {
                case storage_type::long_string_val:
                    reinterpret_cast<long_string_data*>(&data_)->~long_string_data();
                    break;
                case storage_type::byte_string_val:
                    reinterpret_cast<byte_string_data*>(&data_)->~byte_string_data();
                    break;
                case storage_type::array_val:
                    reinterpret_cast<array_data*>(&data_)->~array_data();
                    break;
                case storage_type::object_val:
                    reinterpret_cast<object_data*>(&data_)->~object_data();
                    break;
                default:
                    break;
            }
        }

        variant& operator=(const variant& val)
        {
            if (this !=&val)
            {
                Destroy_();
                switch (val.get_storage_type())
                {
                    case storage_type::null_val:
                        new(reinterpret_cast<void*>(&data_))null_data(*(val.null_data_cast()));
                        break;
                    case storage_type::empty_object_val:
                        new(reinterpret_cast<void*>(&data_))empty_object_data(*(val.empty_object_data_cast()));
                        break;
                    case storage_type::bool_val:
                        new(reinterpret_cast<void*>(&data_))bool_data(*(val.bool_data_cast()));
                        break;
                    case storage_type::int64_val:
                        new(reinterpret_cast<void*>(&data_))int64_data(*(val.int64_data_cast()));
                        break;
                    case storage_type::uint64_val:
                        new(reinterpret_cast<void*>(&data_))uint64_data(*(val.uint64_data_cast()));
                        break;
                    case storage_type::double_val:
                        new(reinterpret_cast<void*>(&data_))double_data(*(val.double_data_cast()));
                        break;
                    case storage_type::short_string_val:
                        new(reinterpret_cast<void*>(&data_))short_string_data(*(val.short_string_data_cast()));
                        break;
                    case storage_type::long_string_val:
                        new(reinterpret_cast<void*>(&data_))long_string_data(*(val.string_data_cast()));
                        break;
                    case storage_type::byte_string_val:
                        new(reinterpret_cast<void*>(&data_))byte_string_data(*(val.byte_string_data_cast()));
                        break;
                    case storage_type::array_val:
                        new(reinterpret_cast<void*>(&data_))array_data(*(val.array_data_cast()));
                        break;
                    case storage_type::object_val:
                        new(reinterpret_cast<void*>(&data_))object_data(*(val.object_data_cast()));
                        break;
                    default:
                        JSONCONS_UNREACHABLE();
                        break;
                }
            }
            return *this;
        }

        variant& operator=(variant&& val) noexcept
        {
            if (this !=&val)
            {
                swap(val);
            }
            return *this;
        }

        storage_type get_storage_type() const
        {
            return reinterpret_cast<const data_base*>(&data_)->get_storage_type();
        }

        semantic_tag get_semantic_tag() const
        {
            return reinterpret_cast<const data_base*>(&data_)->get_semantic_tag();
        }

        const null_data* null_data_cast() const
        {
            return reinterpret_cast<const null_data*>(&data_);
        }

        const empty_object_data* empty_object_data_cast() const
        {
            return reinterpret_cast<const empty_object_data*>(&data_);
        }

        const bool_data* bool_data_cast() const
        {
            return reinterpret_cast<const bool_data*>(&data_);
        }

        const int64_data* int64_data_cast() const
        {
            return reinterpret_cast<const int64_data*>(&data_);
        }

        const uint64_data* uint64_data_cast() const
        {
            return reinterpret_cast<const uint64_data*>(&data_);
        }

        const double_data* double_data_cast() const
        {
            return reinterpret_cast<const double_data*>(&data_);
        }

        const short_string_data* short_string_data_cast() const
        {
            return reinterpret_cast<const short_string_data*>(&data_);
        }

        long_string_data* string_data_cast()
        {
            return reinterpret_cast<long_string_data*>(&data_);
        }

        const long_string_data* string_data_cast() const
        {
            return reinterpret_cast<const long_string_data*>(&data_);
        }

        byte_string_data* byte_string_data_cast()
        {
            return reinterpret_cast<byte_string_data*>(&data_);
        }

        const byte_string_data* byte_string_data_cast() const
        {
            return reinterpret_cast<const byte_string_data*>(&data_);
        }

        object_data* object_data_cast()
        {
            return reinterpret_cast<object_data*>(&data_);
        }

        const object_data* object_data_cast() const
        {
            return reinterpret_cast<const object_data*>(&data_);
        }

        array_data* array_data_cast()
        {
            return reinterpret_cast<array_data*>(&data_);
        }

        const array_data* array_data_cast() const
        {
            return reinterpret_cast<const array_data*>(&data_);
        }

        size_t size() const
        {
            switch (get_storage_type())
            {
                case storage_type::array_val:
                    return array_data_cast()->value().size();
                case storage_type::object_val:
                    return object_data_cast()->value().size();
                default:
                    return 0;
            }
        }

        string_view_type as_string_view() const
        {
            switch (get_storage_type())
            {
                case storage_type::short_string_val:
                    return string_view_type(short_string_data_cast()->data(),short_string_data_cast()->length());
                case storage_type::long_string_val:
                    return string_view_type(string_data_cast()->data(),string_data_cast()->length());
                default:
                    JSONCONS_THROW(json_runtime_error<std::runtime_error>("Not a string"));
            }
        }

        template <typename BAllocator=std::allocator<uint8_t>>
        basic_byte_string<BAllocator> as_byte_string() const
        {
            switch (get_storage_type())
            {
                case storage_type::short_string_val:
                case storage_type::long_string_val:
                {
                    switch (get_semantic_tag())
                    {
                        case semantic_tag::base16:
                        {
                            basic_byte_string<BAllocator> bs;
                            auto s = as_string_view();
                            decode_base16(s.begin(), s.end(), bs);
                            return bs;
                        }
                        case semantic_tag::base64:
                        {
                            basic_byte_string<BAllocator> bs;
                            auto s = as_string_view();
                            decode_base64(s.begin(), s.end(), bs);
                            return bs;
                        }
                        case semantic_tag::base64url:
                        {
                            basic_byte_string<BAllocator> bs;
                            auto s = as_string_view();
                            decode_base64url(s.begin(), s.end(), bs);
                            return bs;
                        }
                        default:
                            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Not a byte string"));
                    }
                    break;
                }
                case storage_type::byte_string_val:
                    return basic_byte_string<BAllocator>(byte_string_data_cast()->data(),byte_string_data_cast()->length());
                default:
                    JSONCONS_THROW(json_runtime_error<std::runtime_error>("Not a byte string"));
            }
        }

        byte_string_view as_byte_string_view() const
        {
            switch (get_storage_type())
            {
            case storage_type::byte_string_val:
                return byte_string_view(byte_string_data_cast()->data(),byte_string_data_cast()->length());
            default:
                JSONCONS_THROW(json_runtime_error<std::runtime_error>("Not a byte string"));
            }
        }

        template <class UserAllocator=std::allocator<uint8_t>>
        basic_bignum<UserAllocator> as_bignum() const
        {
            switch (get_storage_type())
            {
                case storage_type::short_string_val:
                case storage_type::long_string_val:
                    if (!jsoncons::detail::is_integer(as_string_view().data(), as_string_view().length()))
                    {
                        JSONCONS_THROW(json_runtime_error<std::runtime_error>("Not an integer"));
                    }
                    return basic_bignum<UserAllocator>(as_string_view().data(), as_string_view().length());
                case storage_type::double_val:
                    return basic_bignum<UserAllocator>(double_data_cast()->value());
                case storage_type::int64_val:
                    return basic_bignum<UserAllocator>(int64_data_cast()->value());
                case storage_type::uint64_val:
                    return basic_bignum<UserAllocator>(uint64_data_cast()->value());
                case storage_type::bool_val:
                    return basic_bignum<UserAllocator>(bool_data_cast()->value() ? 1 : 0);
                default:
                    JSONCONS_THROW(json_runtime_error<std::runtime_error>("Not a bignum"));
            }
        }

        bool operator==(const variant& rhs) const
        {
            if (this ==&rhs)
            {
                return true;
            }
            switch (get_storage_type())
            {
                case storage_type::null_val:
                    switch (rhs.get_storage_type())
                    {
                        case storage_type::null_val:
                            return true;
                        default:
                            return false;
                    }
                    break;
                case storage_type::empty_object_val:
                    switch (rhs.get_storage_type())
                    {
                        case storage_type::empty_object_val:
                            return true;
                        case storage_type::object_val:
                            return rhs.size() == 0;
                        default:
                            return false;
                    }
                    break;
                case storage_type::bool_val:
                    switch (rhs.get_storage_type())
                    {
                        case storage_type::bool_val:
                            return bool_data_cast()->value() == rhs.bool_data_cast()->value();
                        default:
                            return false;
                    }
                    break;
                case storage_type::int64_val:
                    switch (rhs.get_storage_type())
                    {
                        case storage_type::int64_val:
                            return int64_data_cast()->value() == rhs.int64_data_cast()->value();
                        case storage_type::uint64_val:
                            return int64_data_cast()->value() >= 0 ? static_cast<uint64_t>(int64_data_cast()->value()) == rhs.uint64_data_cast()->value() : false;
                        case storage_type::double_val:
                            return static_cast<double>(int64_data_cast()->value()) == rhs.double_data_cast()->value();
                        default:
                            return false;
                    }
                    break;
                case storage_type::uint64_val:
                    switch (rhs.get_storage_type())
                    {
                        case storage_type::int64_val:
                            return rhs.int64_data_cast()->value() >= 0 ? uint64_data_cast()->value() == static_cast<uint64_t>(rhs.int64_data_cast()->value()) : false;
                        case storage_type::uint64_val:
                            return uint64_data_cast()->value() == rhs.uint64_data_cast()->value();
                        case storage_type::double_val:
                            return static_cast<double>(uint64_data_cast()->value()) == rhs.double_data_cast()->value();
                        default:
                            return false;
                    }
                    break;
                case storage_type::double_val:
                    switch (rhs.get_storage_type())
                    {
                        case storage_type::int64_val:
                            return double_data_cast()->value() == static_cast<double>(rhs.int64_data_cast()->value());
                        case storage_type::uint64_val:
                            return double_data_cast()->value() == static_cast<double>(rhs.uint64_data_cast()->value());
                        case storage_type::double_val:
                            return double_data_cast()->value() == rhs.double_data_cast()->value();
                        default:
                            return false;
                    }
                    break;
                case storage_type::short_string_val:
                    switch (rhs.get_storage_type())
                    {
                        case storage_type::short_string_val:
                            return as_string_view() == rhs.as_string_view();
                        case storage_type::long_string_val:
                            return as_string_view() == rhs.as_string_view();
                        default:
                            return false;
                    }
                    break;
                case storage_type::long_string_val:
                    switch (rhs.get_storage_type())
                    {
                        case storage_type::short_string_val:
                            return as_string_view() == rhs.as_string_view();
                        case storage_type::long_string_val:
                            return as_string_view() == rhs.as_string_view();
                        default:
                            return false;
                    }
                    break;
                case storage_type::byte_string_val:
                    switch (rhs.get_storage_type())
                    {
                        case storage_type::byte_string_val:
                        {
                            return as_byte_string_view() == rhs.as_byte_string_view();
                        }
                        default:
                            return false;
                    }
                    break;
                case storage_type::array_val:
                    switch (rhs.get_storage_type())
                    {
                        case storage_type::array_val:
                            return array_data_cast()->value() == rhs.array_data_cast()->value();
                        default:
                            return false;
                    }
                    break;
                case storage_type::object_val:
                    switch (rhs.get_storage_type())
                    {
                        case storage_type::empty_object_val:
                            return size() == 0;
                        case storage_type::object_val:
                            return object_data_cast()->value() == rhs.object_data_cast()->value();
                        default:
                            return false;
                    }
                    break;
                default:
                    JSONCONS_UNREACHABLE();
                    break;
            }
        }

        bool operator!=(const variant& rhs) const
        {
            return !(*this == rhs);
        }

        bool operator<(const variant& rhs) const
        {
            if (this == &rhs)
            {
                return false;
            }
            switch (get_storage_type())
            {
                case storage_type::null_val:
                    return (int)get_storage_type() < (int)rhs.get_storage_type();
                case storage_type::empty_object_val:
                    switch (rhs.get_storage_type())
                    {
                        case storage_type::empty_object_val:
                            return false;
                        case storage_type::object_val:
                            return rhs.size() != 0;
                        default:
                            return (int)get_storage_type() < (int)rhs.get_storage_type();
                    }
                    break;
                case storage_type::bool_val:
                    switch (rhs.get_storage_type())
                    {
                        case storage_type::bool_val:
                            return bool_data_cast()->value() < rhs.bool_data_cast()->value();
                        default:
                            return (int)get_storage_type() < (int)rhs.get_storage_type();
                    }
                    break;
                case storage_type::int64_val:
                    switch (rhs.get_storage_type())
                    {
                        case storage_type::int64_val:
                            return int64_data_cast()->value() < rhs.int64_data_cast()->value();
                        case storage_type::uint64_val:
                            return int64_data_cast()->value() >= 0 ? static_cast<uint64_t>(int64_data_cast()->value()) < rhs.uint64_data_cast()->value() : true;
                        case storage_type::double_val:
                            return static_cast<double>(int64_data_cast()->value()) < rhs.double_data_cast()->value();
                        default:
                            return (int)get_storage_type() < (int)rhs.get_storage_type();
                    }
                    break;
                case storage_type::uint64_val:
                    switch (rhs.get_storage_type())
                    {
                        case storage_type::int64_val:
                            return rhs.int64_data_cast()->value() >= 0 ? uint64_data_cast()->value() < static_cast<uint64_t>(rhs.int64_data_cast()->value()) : true;
                        case storage_type::uint64_val:
                            return uint64_data_cast()->value() < rhs.uint64_data_cast()->value();
                        case storage_type::double_val:
                            return static_cast<double>(uint64_data_cast()->value()) < rhs.double_data_cast()->value();
                        default:
                            return (int)get_storage_type() < (int)rhs.get_storage_type();
                    }
                    break;
                case storage_type::double_val:
                    switch (rhs.get_storage_type())
                    {
                        case storage_type::int64_val:
                            return double_data_cast()->value() < static_cast<double>(rhs.int64_data_cast()->value());
                        case storage_type::uint64_val:
                            return double_data_cast()->value() < static_cast<double>(rhs.uint64_data_cast()->value());
                        case storage_type::double_val:
                            return double_data_cast()->value() < rhs.double_data_cast()->value();
                        default:
                            return (int)get_storage_type() < (int)rhs.get_storage_type();
                    }
                    break;
                case storage_type::short_string_val:
                    switch (rhs.get_storage_type())
                    {
                        case storage_type::short_string_val:
                            return as_string_view() < rhs.as_string_view();
                        case storage_type::long_string_val:
                            return as_string_view() < rhs.as_string_view();
                        default:
                            return (int)get_storage_type() < (int)rhs.get_storage_type();
                    }
                    break;
                case storage_type::long_string_val:
                    switch (rhs.get_storage_type())
                    {
                        case storage_type::short_string_val:
                            return as_string_view() < rhs.as_string_view();
                        case storage_type::long_string_val:
                            return as_string_view() < rhs.as_string_view();
                        default:
                            return (int)get_storage_type() < (int)rhs.get_storage_type();
                    }
                    break;
                case storage_type::byte_string_val:
                    switch (rhs.get_storage_type())
                    {
                        case storage_type::byte_string_val:
                        {
                            return as_byte_string_view() < rhs.as_byte_string_view();
                        }
                        default:
                            return (int)get_storage_type() < (int)rhs.get_storage_type();
                    }
                    break;
                case storage_type::array_val:
                    switch (rhs.get_storage_type())
                    {
                        case storage_type::array_val:
                            return array_data_cast()->value() < rhs.array_data_cast()->value();
                        default:
                            return (int)get_storage_type() < (int)rhs.get_storage_type();
                    }
                    break;
                case storage_type::object_val:
                    switch (rhs.get_storage_type())
                    {
                        case storage_type::empty_object_val:
                            return false;
                        case storage_type::object_val:
                            return object_data_cast()->value() < rhs.object_data_cast()->value();
                        default:
                            return (int)get_storage_type() < (int)rhs.get_storage_type();
                    }
                    break;
                default:
                    JSONCONS_UNREACHABLE();
                    break;
            }
        }

        template <class Alloc = allocator_type>
        typename std::enable_if<std::is_pod<typename std::allocator_traits<Alloc>::pointer>::value,void>::type
        swap(variant& other) noexcept
        {
            if (this ==&other)
            {
                return;
            }

            std::swap(data_,other.data_);
        }

        template <class Alloc = allocator_type>
        typename std::enable_if<!std::is_pod<typename std::allocator_traits<Alloc>::pointer>::value, void>::type
        swap(variant& other) noexcept
        {
            if (this ==&other)
            {
                return;
            }

            variant temp(other);
            switch (get_storage_type())
            {
                case storage_type::null_val:
                    new(reinterpret_cast<void*>(&(other.data_)))null_data(*null_data_cast());
                    break;
                case storage_type::empty_object_val:
                    new(reinterpret_cast<void*>(&(other.data_)))empty_object_data(*empty_object_data_cast());
                    break;
                case storage_type::bool_val:
                    new(reinterpret_cast<void*>(&(other.data_)))bool_data(*bool_data_cast());
                    break;
                case storage_type::int64_val:
                    new(reinterpret_cast<void*>(&(other.data_)))int64_data(*int64_data_cast());
                    break;
                case storage_type::uint64_val:
                    new(reinterpret_cast<void*>(&(other.data_)))uint64_data(*uint64_data_cast());
                    break;
                case storage_type::double_val:
                    new(reinterpret_cast<void*>(&(other.data_)))double_data(*double_data_cast());
                    break;
                case storage_type::short_string_val:
                    new(reinterpret_cast<void*>(&(other.data_)))short_string_data(*short_string_data_cast());
                    break;
                case storage_type::long_string_val:
                    new(reinterpret_cast<void*>(&other.data_))long_string_data(std::move(*string_data_cast()));
                    break;
                case storage_type::byte_string_val:
                    new(reinterpret_cast<void*>(&other.data_))byte_string_data(std::move(*byte_string_data_cast()));
                    break;
                case storage_type::array_val:
                    new(reinterpret_cast<void*>(&(other.data_)))array_data(std::move(*array_data_cast()));
                    break;
                case storage_type::object_val:
                    new(reinterpret_cast<void*>(&(other.data_)))object_data(std::move(*object_data_cast()));
                    break;
                default:
                    JSONCONS_UNREACHABLE();
                    break;
            }
            switch (temp.get_storage_type())
            {
                case storage_type::long_string_val:
                    new(reinterpret_cast<void*>(&data_))long_string_data(std::move(*temp.string_data_cast()));
                    break;
                case storage_type::byte_string_val:
                    new(reinterpret_cast<void*>(&data_))byte_string_data(std::move(*temp.byte_string_data_cast()));
                    break;
                case storage_type::array_val:
                    new(reinterpret_cast<void*>(&(data_)))array_data(std::move(*temp.array_data_cast()));
                    break;
                case storage_type::object_val:
                    new(reinterpret_cast<void*>(&(data_)))object_data(std::move(*temp.object_data_cast()));
                    break;
                default:
                    std::swap(data_,temp.data_);
                    break;
            }
        }
    private:

        void Init_(const variant& val)
        {
            switch (val.get_storage_type())
            {
            case storage_type::null_val:
                new(reinterpret_cast<void*>(&data_))null_data(*(val.null_data_cast()));
                break;
            case storage_type::empty_object_val:
                new(reinterpret_cast<void*>(&data_))empty_object_data(*(val.empty_object_data_cast()));
                break;
            case storage_type::bool_val:
                new(reinterpret_cast<void*>(&data_))bool_data(*(val.bool_data_cast()));
                break;
            case storage_type::int64_val:
                new(reinterpret_cast<void*>(&data_))int64_data(*(val.int64_data_cast()));
                break;
            case storage_type::uint64_val:
                new(reinterpret_cast<void*>(&data_))uint64_data(*(val.uint64_data_cast()));
                break;
            case storage_type::double_val:
                new(reinterpret_cast<void*>(&data_))double_data(*(val.double_data_cast()));
                break;
            case storage_type::short_string_val:
                new(reinterpret_cast<void*>(&data_))short_string_data(*(val.short_string_data_cast()));
                break;
            case storage_type::long_string_val:
                new(reinterpret_cast<void*>(&data_))long_string_data(*(val.string_data_cast()));
                break;
            case storage_type::byte_string_val:
                new(reinterpret_cast<void*>(&data_))byte_string_data(*(val.byte_string_data_cast()));
                break;
            case storage_type::object_val:
                new(reinterpret_cast<void*>(&data_))object_data(*(val.object_data_cast()));
                break;
            case storage_type::array_val:
                new(reinterpret_cast<void*>(&data_))array_data(*(val.array_data_cast()));
                break;
            default:
                break;
            }
        }

        void Init_(const variant& val, const Allocator& a)
        {
            switch (val.get_storage_type())
            {
            case storage_type::null_val:
            case storage_type::empty_object_val:
            case storage_type::bool_val:
            case storage_type::int64_val:
            case storage_type::uint64_val:
            case storage_type::double_val:
            case storage_type::short_string_val:
                Init_(val);
                break;
            case storage_type::long_string_val:
                new(reinterpret_cast<void*>(&data_))long_string_data(*(val.string_data_cast()),a);
                break;
            case storage_type::byte_string_val:
                new(reinterpret_cast<void*>(&data_))byte_string_data(*(val.byte_string_data_cast()),a);
                break;
            case storage_type::array_val:
                new(reinterpret_cast<void*>(&data_))array_data(*(val.array_data_cast()),a);
                break;
            case storage_type::object_val:
                new(reinterpret_cast<void*>(&data_))object_data(*(val.object_data_cast()),a);
                break;
            default:
                break;
            }
        }

        void Init_rv_(variant&& val) noexcept
        {
            switch (val.get_storage_type())
            {
            case storage_type::null_val:
            case storage_type::empty_object_val:
            case storage_type::double_val:
            case storage_type::int64_val:
            case storage_type::uint64_val:
            case storage_type::bool_val:
            case storage_type::short_string_val:
                Init_(val);
                break;
            case storage_type::long_string_val:
                {
                    new(reinterpret_cast<void*>(&data_))long_string_data(std::move(*val.string_data_cast()));
                    new(reinterpret_cast<void*>(&val.data_))null_data();
                }
                break;
            case storage_type::byte_string_val:
                {
                    new(reinterpret_cast<void*>(&data_))byte_string_data(std::move(*val.byte_string_data_cast()));
                    new(reinterpret_cast<void*>(&val.data_))null_data();
                }
                break;
            case storage_type::array_val:
                {
                    new(reinterpret_cast<void*>(&data_))array_data(std::move(*val.array_data_cast()));
                    new(reinterpret_cast<void*>(&val.data_))null_data();
                }
                break;
            case storage_type::object_val:
                {
                    new(reinterpret_cast<void*>(&data_))object_data(std::move(*val.object_data_cast()));
                    new(reinterpret_cast<void*>(&val.data_))null_data();
                }
                break;
            default:
                JSONCONS_UNREACHABLE();
                break;
            }
        }

        void Init_rv_(variant&& val, const Allocator&, std::true_type) noexcept
        {
            Init_rv_(std::forward<variant>(val));
        }

        void Init_rv_(variant&& val, const Allocator& a, std::false_type) noexcept
        {
            switch (val.get_storage_type())
            {
            case storage_type::null_val:
            case storage_type::empty_object_val:
            case storage_type::double_val:
            case storage_type::int64_val:
            case storage_type::uint64_val:
            case storage_type::bool_val:
            case storage_type::short_string_val:
                Init_(std::forward<variant>(val));
                break;
            case storage_type::long_string_val:
                {
                    if (a == val.string_data_cast()->get_allocator())
                    {
                        Init_rv_(std::forward<variant>(val), a, std::true_type());
                    }
                    else
                    {
                        Init_(val,a);
                    }
                }
                break;
            case storage_type::byte_string_val:
                {
                    if (a == val.byte_string_data_cast()->get_allocator())
                    {
                        Init_rv_(std::forward<variant>(val), a, std::true_type());
                    }
                    else
                    {
                        Init_(val,a);
                    }
                }
                break;
            case storage_type::object_val:
                {
                    if (a == val.object_data_cast()->get_allocator())
                    {
                        Init_rv_(std::forward<variant>(val), a, std::true_type());
                    }
                    else
                    {
                        Init_(val,a);
                    }
                }
                break;
            case storage_type::array_val:
                {
                    if (a == val.array_data_cast()->get_allocator())
                    {
                        Init_rv_(std::forward<variant>(val), a, std::true_type());
                    }
                    else
                    {
                        Init_(val,a);
                    }
                }
                break;
            default:
                break;
            }
        }
    };

    template <class ParentT>
    class json_proxy 
    {
    private:

        ParentT& parent_;
        const char_type* data_;
        size_t length_;

        json_proxy() = delete;
        json_proxy& operator = (const json_proxy& other) = delete; 

        json_proxy(ParentT& parent, const char_type* data, size_t length)
            : parent_(parent), data_(data), length_(length)
        {
        }

        basic_json& evaluate() 
        {
            return parent_.evaluate(string_view_type(data_,length_));
        }

        const basic_json& evaluate() const
        {
            return parent_.evaluate(string_view_type(data_,length_));
        }

        basic_json& evaluate_with_default()
        {
            basic_json& val = parent_.evaluate_with_default();
            auto it = val.find(string_view_type(data_,length_));
            if (it == val.object_range().end())
            {
                it = val.insert_or_assign(val.object_range().begin(),string_view_type(data_,length_),object(val.object_value().get_allocator()));            
            }
            return it->value();
        }

        basic_json& evaluate(size_t index)
        {
            return evaluate().at(index);
        }

        const basic_json& evaluate(size_t index) const
        {
            return evaluate().at(index);
        }

        basic_json& evaluate(const string_view_type& index)
        {
            return evaluate().at(index);
        }

        const basic_json& evaluate(const string_view_type& index) const
        {
            return evaluate().at(index);
        }
    public:

        friend class basic_json<CharT,ImplementationPolicy,Allocator>;
        typedef json_proxy<ParentT> proxy_type;

        range<object_iterator> object_range()
        {
            return evaluate().object_range();
        }

        range<const_object_iterator> object_range() const
        {
            return evaluate().object_range();
        }

        range<array_iterator> array_range()
        {
            return evaluate().array_range();
        }

        range<const_array_iterator> array_range() const
        {
            return evaluate().array_range();
        }

        size_t size() const noexcept
        {
            return evaluate().size();
        }

        storage_type get_storage_type() const
        {
            return evaluate().get_storage_type();
        }

        semantic_tag get_semantic_tag() const
        {
            return evaluate().get_semantic_tag();
        }

        size_t count(const string_view_type& name) const
        {
            return evaluate().count(name);
        }

        allocator_type get_allocator() const
        {
            return evaluate().get_allocator();
        }

        bool contains(const string_view_type& name) const
        {
            return evaluate().contains(name);
        }

        bool is_null() const noexcept
        {
            return evaluate().is_null();
        }

        bool empty() const
        {
            return evaluate().empty();
        }

        size_t capacity() const
        {
            return evaluate().capacity();
        }

        void reserve(size_t n)
        {
            evaluate().reserve(n);
        }

        void resize(size_t n)
        {
            evaluate().resize(n);
        }

        template <class T>
        void resize(size_t n, T val)
        {
            evaluate().resize(n,val);
        }

        template<class T, class... Args>
        bool is(Args&&... args) const
        {
            return evaluate().template is<T>(std::forward<Args>(args)...);
        }

        bool is_string() const noexcept
        {
            return evaluate().is_string();
        }

        bool is_string_view() const noexcept
        {
            return evaluate().is_string_view();
        }

        bool is_byte_string() const noexcept
        {
            return evaluate().is_byte_string();
        }

        bool is_byte_string_view() const noexcept
        {
            return evaluate().is_byte_string_view();
        }

        bool is_bignum() const noexcept
        {
            return evaluate().is_bignum();
        }

        bool is_number() const noexcept
        {
            return evaluate().is_number();
        }
        bool is_bool() const noexcept
        {
            return evaluate().is_bool();
        }

        bool is_object() const noexcept
        {
            return evaluate().is_object();
        }

        bool is_array() const noexcept
        {
            return evaluate().is_array();
        }

        bool is_int64() const noexcept
        {
            return evaluate().is_int64();
        }

        bool is_uint64() const noexcept
        {
            return evaluate().is_uint64();
        }

        bool is_double() const noexcept
        {
            return evaluate().is_double();
        }

        string_view_type as_string_view() const 
        {
            return evaluate().as_string_view();
        }

        byte_string_view as_byte_string_view() const 
        {
            return evaluate().as_byte_string_view();
        }

        basic_bignum<byte_allocator_type> as_bignum() const 
        {
            return evaluate().as_bignum();
        }

        template <class SAllocator=std::allocator<CharT>>
        string_type as_string() const 
        {
            return evaluate().as_string();
        }

        template <class SAllocator=std::allocator<CharT>>
        string_type as_string(const SAllocator& allocator) const 
        {
            return evaluate().as_string(allocator);
        }

        template <typename BAllocator=std::allocator<uint8_t>>
        basic_byte_string<BAllocator> as_byte_string() const
        {
            return evaluate().template as_byte_string<BAllocator>();
        }

        template <class SAllocator=std::allocator<CharT>>
        string_type as_string(const basic_json_options<char_type>& options) const
        {
            return evaluate().as_string(options);
        }

        template <class SAllocator=std::allocator<CharT>>
        string_type as_string(const basic_json_options<char_type>& options,
                              const SAllocator& allocator) const
        {
            return evaluate().as_string(options,allocator);
        }

        template<class T, class... Args>
        T as(Args&&... args) const
        {
            return evaluate().template as<T>(std::forward<Args>(args)...);
        }

        template<class T>
        typename std::enable_if<std::is_same<string_type,T>::value,T>::type 
        as(const char_allocator_type& allocator) const
        {
            return evaluate().template as<T>(allocator);
        }
        bool as_bool() const
        {
            return evaluate().as_bool();
        }

        double as_double() const
        {
            return evaluate().as_double();
        }

        template <class T
#if !defined(JSONCONS_NO_DEPRECATED)
             = int64_t
#endif
        >
        T as_integer() const
        {
            return evaluate().template as_integer<T>();
        }

        template <class T>
        json_proxy& operator=(T&& val) 
        {
            parent_.evaluate_with_default().insert_or_assign(string_view_type(data_,length_), std::forward<T>(val));
            return *this;
        }

        bool operator==(const basic_json& rhs) const
        {
            return evaluate() == rhs;
        }

        bool operator!=(const basic_json& rhs) const
        {
            return evaluate() != rhs;
        }

        bool operator<(const basic_json& rhs) const
        {
            return evaluate() < rhs;
        }

        bool operator<=(const basic_json& rhs) const
        {
            return !(rhs < evaluate());
        }

        bool operator>(const basic_json& rhs) const
        {
            return !(evaluate() <= rhs);
        }

        bool operator>=(const basic_json& rhs) const
        {
            return !(evaluate() < rhs);
        }

        basic_json& operator[](size_t i)
        {
            return evaluate_with_default().at(i);
        }

        const basic_json& operator[](size_t i) const
        {
            return evaluate().at(i);
        }

        json_proxy<proxy_type> operator[](const string_view_type& key)
        {
            return json_proxy<proxy_type>(*this,key.data(),key.length());
        }

        const basic_json& operator[](const string_view_type& name) const
        {
            return at(name);
        }

        basic_json& at(const string_view_type& name)
        {
            return evaluate().at(name);
        }

        const basic_json& at(const string_view_type& name) const
        {
            return evaluate().at(name);
        }

        const basic_json& at(size_t index)
        {
            return evaluate().at(index);
        }

        const basic_json& at(size_t index) const
        {
            return evaluate().at(index);
        }

        object_iterator find(const string_view_type& name)
        {
            return evaluate().find(name);
        }

        const_object_iterator find(const string_view_type& name) const
        {
            return evaluate().find(name);
        }

        template <class T>
        T get_with_default(const string_view_type& name, const T& default_val) const
        {
            return evaluate().template get_with_default<T>(name,default_val);
        }

        template <class T = std::basic_string<CharT>>
        T get_with_default(const string_view_type& name, const CharT* default_val) const
        {
            return evaluate().template get_with_default<T>(name,default_val);
        }

        void shrink_to_fit()
        {
            evaluate_with_default().shrink_to_fit();
        }

        void clear()
        {
            evaluate().clear();
        }
        // Remove all elements from an array or object

        void erase(const_object_iterator pos)
        {
            evaluate().erase(pos);
        }
        // Remove a range of elements from an object 

        void erase(const_object_iterator first, const_object_iterator last)
        {
            evaluate().erase(first, last);
        }
        // Remove a range of elements from an object 

        void erase(const string_view_type& name)
        {
            evaluate().erase(name);
        }

        void erase(const_array_iterator pos)
        {
            evaluate().erase(pos);
        }
        // Removes the element at pos 

        void erase(const_array_iterator first, const_array_iterator last)
        {
            evaluate().erase(first, last);
        }
        // Remove a range of elements from an array 

        // merge

        void merge(const basic_json& source)
        {
            return evaluate().merge(source);
        }

        void merge(basic_json&& source)
        {
            return evaluate().merge(std::forward<basic_json>(source));
        }

        void merge(object_iterator hint, const basic_json& source)
        {
            return evaluate().merge(hint, source);
        }

        void merge(object_iterator hint, basic_json&& source)
        {
            return evaluate().merge(hint, std::forward<basic_json>(source));
        }

        // merge_or_update

        void merge_or_update(const basic_json& source)
        {
            return evaluate().merge_or_update(source);
        }

        void merge_or_update(basic_json&& source)
        {
            return evaluate().merge_or_update(std::forward<basic_json>(source));
        }

        void merge_or_update(object_iterator hint, const basic_json& source)
        {
            return evaluate().merge_or_update(hint, source);
        }

        void merge_or_update(object_iterator hint, basic_json&& source)
        {
            return evaluate().merge_or_update(hint, std::forward<basic_json>(source));
        }

        template <class T>
        std::pair<object_iterator,bool> insert_or_assign(const string_view_type& name, T&& val)
        {
            return evaluate().insert_or_assign(name,std::forward<T>(val));
        }

       // emplace

        template <class ... Args>
        std::pair<object_iterator,bool> try_emplace(const string_view_type& name, Args&&... args)
        {
            return evaluate().try_emplace(name,std::forward<Args>(args)...);
        }

        template <class T>
        object_iterator insert_or_assign(object_iterator hint, const string_view_type& name, T&& val)
        {
            return evaluate().insert_or_assign(hint, name, std::forward<T>(val));
        }

        template <class ... Args>
        object_iterator try_emplace(object_iterator hint, const string_view_type& name, Args&&... args)
        {
            return evaluate().try_emplace(hint, name, std::forward<Args>(args)...);
        }

        template <class... Args> 
        array_iterator emplace(const_array_iterator pos, Args&&... args)
        {
            evaluate_with_default().emplace(pos, std::forward<Args>(args)...);
        }

        template <class... Args> 
        basic_json& emplace_back(Args&&... args)
        {
            return evaluate_with_default().emplace_back(std::forward<Args>(args)...);
        }

        template <class T>
        void push_back(T&& val)
        {
            evaluate_with_default().push_back(std::forward<T>(val));
        }

        template <class T>
        array_iterator insert(const_array_iterator pos, T&& val)
        {
            return evaluate_with_default().insert(pos, std::forward<T>(val));
        }

        template <class InputIt>
        array_iterator insert(const_array_iterator pos, InputIt first, InputIt last)
        {
            return evaluate_with_default().insert(pos, first, last);
        }

        template <class InputIt>
        void insert(InputIt first, InputIt last)
        {
            evaluate_with_default().insert(first, last);
        }

        template <class InputIt>
        void insert(sorted_unique_range_tag tag, InputIt first, InputIt last)
        {
            evaluate_with_default().insert(tag, first, last);
        }

        template <class SAllocator>
        void dump(std::basic_string<char_type,char_traits_type,SAllocator>& s) const
        {
            evaluate().dump(s);
        }

        template <class SAllocator>
        void dump(std::basic_string<char_type,char_traits_type,SAllocator>& s,
                  indenting line_indent) const
        {
            evaluate().dump(s, line_indent);
        }

        template <class SAllocator>
        void dump(std::basic_string<char_type,char_traits_type,SAllocator>& s,
                  const basic_json_options<char_type>& options) const
        {
            evaluate().dump(s,options);
        }

        template <class SAllocator>
        void dump(std::basic_string<char_type,char_traits_type,SAllocator>& s,
                  const basic_json_options<char_type>& options,
                  indenting line_indent) const
        {
            evaluate().dump(s,options,line_indent);
        }

        void dump(basic_json_content_handler<char_type>& handler) const
        {
            evaluate().dump(handler);
        }

        void dump(std::basic_ostream<char_type>& os) const
        {
            evaluate().dump(os);
        }

        void dump(std::basic_ostream<char_type>& os, indenting line_indent) const
        {
            evaluate().dump(os, line_indent);
        }

        void dump(std::basic_ostream<char_type>& os, const basic_json_options<char_type>& options) const
        {
            evaluate().dump(os,options);
        }

        void dump(std::basic_ostream<char_type>& os, const basic_json_options<char_type>& options, indenting line_indent) const
        {
            evaluate().dump(os,options,line_indent);
        }
#if !defined(JSONCONS_NO_DEPRECATED)

        bool is_datetime() const noexcept
        {
            return evaluate().is_datetime();
        }

        bool is_epoch_time() const noexcept
        {
            return evaluate().is_epoch_time();
        }

        template <class T>
        void add(T&& val)
        {
            evaluate_with_default().add(std::forward<T>(val));
        }

        template <class T>
        array_iterator add(const_array_iterator pos, T&& val)
        {
            return evaluate_with_default().add(pos, std::forward<T>(val));
        }

       // set

        template <class T>
        std::pair<object_iterator,bool> set(const string_view_type& name, T&& val)
        {
            return evaluate().set(name,std::forward<T>(val));
        }

        template <class T>
        object_iterator set(object_iterator hint, const string_view_type& name, T&& val)
        {
            return evaluate().set(hint, name, std::forward<T>(val));
        }

        bool has_key(const string_view_type& name) const
        {
            return evaluate().has_key(name);
        }

        bool is_integer() const noexcept
        {
            return evaluate().is_int64();
        }

        bool is_uinteger() const noexcept
        {
            return evaluate().is_uint64();
        }

        unsigned long long as_ulonglong() const
        {
            return evaluate().as_ulonglong();
        }

        uint64_t as_uinteger() const
        {
            return evaluate().as_uinteger();
        }

        void dump(std::basic_ostream<char_type>& os, const basic_json_options<char_type>& options, bool pprint) const
        {
            evaluate().dump(os,options,pprint);
        }

        void dump(std::basic_ostream<char_type>& os, bool pprint) const
        {
            evaluate().dump(os, pprint);
        }

        string_type to_string(const char_allocator_type& allocator = char_allocator_type()) const noexcept
        {
            return evaluate().to_string(allocator);
        }
        void write(basic_json_content_handler<char_type>& handler) const
        {
            evaluate().write(handler);
        }

        void write(std::basic_ostream<char_type>& os) const
        {
            evaluate().write(os);
        }

        void write(std::basic_ostream<char_type>& os, const basic_json_options<char_type>& options) const
        {
            evaluate().write(os,options);
        }

        void write(std::basic_ostream<char_type>& os, const basic_json_options<char_type>& options, bool pprint) const
        {
            evaluate().write(os,options,pprint);
        }

        string_type to_string(const basic_json_options<char_type>& options, char_allocator_type& allocator = char_allocator_type()) const
        {
            return evaluate().to_string(options,allocator);
        }

        range<object_iterator> members()
        {
            return evaluate().members();
        }

        range<const_object_iterator> members() const
        {
            return evaluate().members();
        }

        range<array_iterator> elements()
        {
            return evaluate().elements();
        }

        range<const_array_iterator> elements() const
        {
            return evaluate().elements();
        }
        void to_stream(basic_json_content_handler<char_type>& handler) const
        {
            evaluate().to_stream(handler);
        }

        void to_stream(std::basic_ostream<char_type>& os) const
        {
            evaluate().to_stream(os);
        }

        void to_stream(std::basic_ostream<char_type>& os, const basic_json_options<char_type>& options) const
        {
            evaluate().to_stream(os,options);
        }

        void to_stream(std::basic_ostream<char_type>& os, const basic_json_options<char_type>& options, bool pprint) const
        {
            evaluate().to_stream(os,options,pprint);
        }
#endif
        void swap(basic_json& val)
        {
            evaluate_with_default().swap(val);
        }

        friend std::basic_ostream<char_type>& operator<<(std::basic_ostream<char_type>& os, const json_proxy& o)
        {
            o.dump(os);
            return os;
        }

#if !defined(JSONCONS_NO_DEPRECATED)

        void resize_array(size_t n)
        {
            evaluate().resize_array(n);
        }

        template <class T>
        void resize_array(size_t n, T val)
        {
            evaluate().resize_array(n,val);
        }

        object_iterator begin_members()
        {
            return evaluate().begin_members();
        }

        const_object_iterator begin_members() const
        {
            return evaluate().begin_members();
        }

        object_iterator end_members()
        {
            return evaluate().end_members();
        }

        const_object_iterator end_members() const
        {
            return evaluate().end_members();
        }

        array_iterator begin_elements()
        {
            return evaluate().begin_elements();
        }

        const_array_iterator begin_elements() const
        {
            return evaluate().begin_elements();
        }

        array_iterator end_elements()
        {
            return evaluate().end_elements();
        }

        const_array_iterator end_elements() const
        {
            return evaluate().end_elements();
        }

        template <class T>
        basic_json get(const string_view_type& name, T&& default_val) const
        {
            return evaluate().get(name,std::forward<T>(default_val));
        }

        const basic_json& get(const string_view_type& name) const
        {
            return evaluate().get(name);
        }

        bool is_ulonglong() const noexcept
        {
            return evaluate().is_ulonglong();
        }

        bool is_longlong() const noexcept
        {
            return evaluate().is_longlong();
        }

        int as_int() const
        {
            return evaluate().as_int();
        }

        unsigned int as_uint() const
        {
            return evaluate().as_uint();
        }

        long as_long() const
        {
            return evaluate().as_long();
        }

        unsigned long as_ulong() const
        {
            return evaluate().as_ulong();
        }

        long long as_longlong() const
        {
            return evaluate().as_longlong();
        }

        bool has_member(const string_type& name) const
        {
            return evaluate().has_member(name);
        }

        // Remove a range of elements from an array 
        void remove_range(size_t from_index, size_t to_index)
        {
            evaluate().remove_range(from_index, to_index);
        }
        // Remove a range of elements from an array 
        void remove(const string_view_type& name)
        {
            evaluate().remove(name);
        }
        void remove_member(const string_view_type& name)
        {
            evaluate().remove(name);
        }
        bool is_empty() const noexcept
        {
            return empty();
        }
        bool is_numeric() const noexcept
        {
            return is_number();
        }
#endif
    };

    static basic_json parse(std::basic_istream<char_type>& is)
    {
        parse_error_handler_type err_handler;
        return parse(is,err_handler);
    }

    static basic_json parse(std::basic_istream<char_type>& is, parse_error_handler& err_handler)
    {
        json_decoder<basic_json<CharT,ImplementationPolicy,Allocator>> handler;
        basic_json_reader<char_type,stream_source<char_type>> reader(is, handler, err_handler);
        reader.read_next();
        reader.check_done();
        if (!handler.is_valid())
        {
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Failed to parse json stream"));
        }
        return handler.get_result();
    }

    static basic_json parse(const string_view_type& s)
    {
        parse_error_handler_type err_handler;
        return parse(s,err_handler);
    }

#if !defined(JSONCONS_NO_DEPRECATED)

    void add(size_t index, const basic_json& value)
    {
        evaluate_with_default().add(index, value);
    }

    void add(size_t index, basic_json&& value)
    {
        evaluate_with_default().add(index, std::forward<basic_json>(value));
    }

    static basic_json parse(const char_type* s, size_t length)
    {
        parse_error_handler_type err_handler;
        return parse(s,length,err_handler);
    }

    static basic_json parse(const char_type* s, size_t length, parse_error_handler& err_handler)
    {
        return parse(string_view_type(s,length),err_handler);
    }
#endif

    static basic_json parse(const string_view_type& s, parse_error_handler& err_handler)
    {
        json_decoder<basic_json> decoder;
        basic_json_parser<char_type> parser(err_handler);

        auto result = unicons::skip_bom(s.begin(), s.end());
        if (result.ec != unicons::encoding_errc())
        {
            throw ser_error(result.ec);
        }
        size_t offset = result.it - s.begin();
        parser.update(s.data()+offset,s.size()-offset);
        parser.parse_some(decoder);
        parser.finish_parse(decoder);
        parser.check_done();
        if (!decoder.is_valid())
        {
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Failed to parse json string"));
        }
        return decoder.get_result();
    }

    static basic_json parse(std::basic_istream<char_type>& is, const basic_json_options<CharT>& options)
    {
        parse_error_handler_type err_handler;
        return parse(is,options,err_handler);
    }

    static basic_json parse(std::basic_istream<char_type>& is, const basic_json_options<CharT>& options, parse_error_handler& err_handler)
    {
        json_decoder<basic_json<CharT,ImplementationPolicy,Allocator>> handler;
        basic_json_reader<char_type,stream_source<char_type>> reader(is, handler, options, err_handler);
        reader.read_next();
        reader.check_done();
        if (!handler.is_valid())
        {
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Failed to parse json stream"));
        }
        return handler.get_result();
    }

    static basic_json parse(const string_view_type& s, const basic_json_options<CharT>& options)
    {
        parse_error_handler_type err_handler;
        return parse(s,options,err_handler);
    }

    static basic_json parse(const string_view_type& s, const basic_json_options<CharT>& options, parse_error_handler& err_handler)
    {
        json_decoder<basic_json> decoder;
        basic_json_parser<char_type> parser(options,err_handler);

        auto result = unicons::skip_bom(s.begin(), s.end());
        if (result.ec != unicons::encoding_errc())
        {
            throw ser_error(result.ec);
        }
        size_t offset = result.it - s.begin();
        parser.update(s.data()+offset,s.size()-offset);
        parser.parse_some(decoder);
        parser.finish_parse(decoder);
        parser.check_done();
        if (!decoder.is_valid())
        {
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Failed to parse json string"));
        }
        return decoder.get_result();
    }

    static basic_json make_array()
    {
        return basic_json(array());
    }

    static basic_json make_array(const array& a)
    {
        return basic_json(a);
    }

    static basic_json make_array(const array& a, allocator_type allocator)
    {
        return basic_json(variant(a, semantic_tag::none, allocator));
    }

    static basic_json make_array(std::initializer_list<basic_json> init, const Allocator& allocator = Allocator())
    {
        return array(std::move(init),allocator);
    }

    static basic_json make_array(size_t n, const Allocator& allocator = Allocator())
    {
        return array(n,allocator);
    }

    template <class T>
    static basic_json make_array(size_t n, const T& val, const Allocator& allocator = Allocator())
    {
        return basic_json::array(n, val,allocator);
    }

    template <size_t dim>
    static typename std::enable_if<dim==1,basic_json>::type make_array(size_t n)
    {
        return array(n);
    }

    template <size_t dim, class T>
    static typename std::enable_if<dim==1,basic_json>::type make_array(size_t n, const T& val, const Allocator& allocator = Allocator())
    {
        return array(n,val,allocator);
    }

    template <size_t dim, typename... Args>
    static typename std::enable_if<(dim>1),basic_json>::type make_array(size_t n, Args... args)
    {
        const size_t dim1 = dim - 1;

        basic_json val = make_array<dim1>(std::forward<Args>(args)...);
        val.resize(n);
        for (size_t i = 0; i < n; ++i)
        {
            val[i] = make_array<dim1>(std::forward<Args>(args)...);
        }
        return val;
    }

    static const basic_json& null()
    {
        static basic_json a_null = basic_json(null_type(), semantic_tag::none);
        return a_null;
    }

    variant var_;

    basic_json(semantic_tag tag = semantic_tag::none) 
        : var_(tag)
    {
    }

    explicit basic_json(const Allocator& allocator, semantic_tag tag = semantic_tag::none) 
        : var_(object(allocator),tag)
    {
    }

    basic_json(const basic_json& val)
        : var_(val.var_)
    {
    }

    basic_json(const basic_json& val, const Allocator& allocator)
        : var_(val.var_,allocator)
    {
    }

    basic_json(basic_json&& other) noexcept
        : var_(std::move(other.var_))
    {
    }

    basic_json(basic_json&& other, const Allocator&) noexcept
        : var_(std::move(other.var_) /*,allocator*/ )
    {
    }

    basic_json(const variant& val)
        : var_(val)
    {
    }

    basic_json(variant&& other)
        : var_(std::forward<variant>(other))
    {
    }

    basic_json(const array& val, semantic_tag tag = semantic_tag::none)
        : var_(val, tag)
    {
    }

    basic_json(array&& other, semantic_tag tag = semantic_tag::none)
        : var_(std::forward<array>(other), tag)
    {
    }

    basic_json(const object& other, semantic_tag tag = semantic_tag::none)
        : var_(other, tag)
    {
    }

    basic_json(object&& other, semantic_tag tag = semantic_tag::none)
        : var_(std::forward<object>(other), tag)
    {
    }

    template <class ParentT>
    basic_json(const json_proxy<ParentT>& proxy)
        : var_(proxy.evaluate().var_)
    {
    }

    template <class ParentT>
    basic_json(const json_proxy<ParentT>& proxy, const Allocator& allocator)
        : var_(proxy.evaluate().var_,allocator)
    {
    }

    template <class T>
    basic_json(const T& val)
        : var_(json_type_traits<basic_json,T>::to_json(val).var_)
    {
    }

    template <class T>
    basic_json(const T& val, const Allocator& allocator)
        : var_(json_type_traits<basic_json,T>::to_json(val,allocator).var_)
    {
    }

    basic_json(const char_type* s, semantic_tag tag = semantic_tag::none)
        : var_(s, char_traits_type::length(s), tag)
    {
    }

    basic_json(const char_type* s, const Allocator& allocator)
        : var_(s, char_traits_type::length(s), semantic_tag::none, allocator)
    {
    }

#if !defined(JSONCONS_NO_DEPRECATED)
    basic_json(double val, uint8_t)
        : var_(val, semantic_tag::none)
    {
    }
    basic_json(double val, 
               const floating_point_options&,
               semantic_tag tag = semantic_tag::none)
        : var_(val, tag)
    {
    }
#endif

    basic_json(double val, semantic_tag tag)
        : var_(val, tag)
    {
    }

    template <class T>
    basic_json(T val, semantic_tag tag, typename std::enable_if<std::is_integral<T>::value && std::is_signed<T>::value>::type* = 0)
        : var_(static_cast<int64_t>(val), tag)
    {
    }

    template <class T>
    basic_json(T val, semantic_tag tag, typename std::enable_if<std::is_integral<T>::value && !std::is_signed<T>::value>::type* = 0)
        : var_(static_cast<uint64_t>(val), tag)
    {
    }

    basic_json(const char_type *s, size_t length, semantic_tag tag = semantic_tag::none)
        : var_(s, length, tag)
    {
    }

    basic_json(const string_view_type& sv, semantic_tag tag)
        : var_(sv.data(), sv.length(), tag)
    {
    }

    basic_json(null_type val, semantic_tag tag)
        : var_(val, tag)
    {
    }

    basic_json(bool val, semantic_tag tag)
        : var_(val, tag)
    {
    }

    basic_json(const string_view_type& sv, semantic_tag tag, const Allocator& allocator)
        : var_(sv.data(), sv.length(), tag, allocator)
    {
    }

    basic_json(const char_type *s, size_t length, 
               semantic_tag tag, const Allocator& allocator)
        : var_(s, length, tag, allocator)
    {
    }

#if !defined(JSONCONS_NO_DEPRECATED)

    basic_json(const byte_string_view& bs, 
               byte_string_chars_format encoding_hint,
               semantic_tag tag = semantic_tag::none)
        : var_(bs, tag)
    {
        switch (encoding_hint)
        {
            {
                case byte_string_chars_format::base16:
                    var_ = variant(bs, semantic_tag::base16);
                    break;
                case byte_string_chars_format::base64:
                    var_ = variant(bs, semantic_tag::base64);
                    break;
                case byte_string_chars_format::base64url:
                    var_ = variant(bs, semantic_tag::base64url);
                    break;
                default:
                    break;
            }
        }
    }
#endif

    explicit basic_json(const byte_string_view& bs, 
                        semantic_tag tag = semantic_tag::none)
        : var_(bs, tag)
    {
    }

    basic_json(const byte_string_view& bs, 
               semantic_tag tag, 
               const Allocator& allocator)
        : var_(bs, tag, allocator)
    {
    }

    explicit basic_json(const basic_bignum<byte_allocator_type>& bs)
        : var_(bs)
    {
    }

    explicit basic_json(const basic_bignum<byte_allocator_type>& bs, const Allocator& allocator)
    : var_(bs, byte_allocator_type(allocator))
    {
    }

#if !defined(JSONCONS_NO_DEPRECATED)

    template<class InputIterator>
    basic_json(InputIterator first, InputIterator last, const Allocator& allocator = Allocator())
        : var_(first,last,allocator)
    {
    }
#endif

    ~basic_json()
    {
    }

    basic_json& operator=(const basic_json& rhs)
    {
        if (this != &rhs)
        {
            var_ = rhs.var_;
        }
        return *this;
    }

    basic_json& operator=(basic_json&& rhs) noexcept
    {
        if (this !=&rhs)
        {
            var_ = std::move(rhs.var_);
        }
        return *this;
    }

    template <class T>
    basic_json& operator=(const T& val)
    {
        var_ = json_type_traits<basic_json,T>::to_json(val).var_;
        return *this;
    }

    basic_json& operator=(const char_type* s)
    {
        var_ = variant(s, char_traits_type::length(s), semantic_tag::none);
        return *this;
    }

    friend bool operator==(const basic_json& lhs, const basic_json& rhs)
    {
        return lhs.var_ == rhs.var_;
    }

    friend bool operator!=(const basic_json& lhs, const basic_json& rhs)
    {
        return !(lhs == rhs);
    }

    friend bool operator<(const basic_json& lhs, const basic_json& rhs) 
    {
        return lhs.var_ < rhs.var_;
    }

    friend bool operator<=(const basic_json& lhs, const basic_json& rhs)
    {
        return !(rhs < lhs);
    }

    friend bool operator>(const basic_json& lhs, const basic_json& rhs) 
    {
        return !(lhs <= rhs);
    }

    friend bool operator>=(const basic_json& lhs, const basic_json& rhs)
    {
        return !(lhs < rhs);
    }

    size_t size() const noexcept
    {
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
            return 0;
        case storage_type::object_val:
            return object_value().size();
        case storage_type::array_val:
            return array_value().size();
        default:
            return 0;
        }
    }

    basic_json& operator[](size_t i)
    {
        return at(i);
    }

    const basic_json& operator[](size_t i) const
    {
        return at(i);
    }

    json_proxy<basic_json> operator[](const string_view_type& name)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val: 
            create_object_implicitly();
            JSONCONS_FALLTHROUGH;
        case storage_type::object_val:
            return json_proxy<basic_json>(*this, name.data(),name.length());
            break;
        default:
            JSONCONS_THROW(not_an_object(name.data(),name.length()));
            break;
        }
    }

    const basic_json& operator[](const string_view_type& name) const
    {
        return at(name);
    }

    template <class SAllocator>
    void dump(std::basic_string<char_type,char_traits_type,SAllocator>& s) const
    {
        typedef std::basic_string<char_type,char_traits_type,SAllocator> string_type;
        basic_json_compressed_encoder<char_type,jsoncons::string_result<string_type>> encoder(s);
        dump(encoder);
    }

    template <class SAllocator>
    void dump(std::basic_string<char_type,char_traits_type,SAllocator>& s, indenting line_indent) const
    {
        typedef std::basic_string<char_type,char_traits_type,SAllocator> string_type;
        if (line_indent == indenting::indent)
        {
            basic_json_encoder<char_type,jsoncons::string_result<string_type>> encoder(s);
            dump(encoder);
        }
        else
        {
            basic_json_compressed_encoder<char_type,jsoncons::string_result<string_type>> encoder(s);
            dump(encoder);
        }
    }

    template <class SAllocator>
    void dump(std::basic_string<char_type,char_traits_type,SAllocator>& s,
              const basic_json_options<char_type>& options) const
    {
        typedef std::basic_string<char_type,char_traits_type,SAllocator> string_type;
        basic_json_compressed_encoder<char_type,jsoncons::string_result<string_type>> encoder(s, options);
        dump(encoder);
    }

    template <class SAllocator>
    void dump(std::basic_string<char_type,char_traits_type,SAllocator>& s,
              const basic_json_options<char_type>& options, 
              indenting line_indent) const
    {
        typedef std::basic_string<char_type,char_traits_type,SAllocator> string_type;
        if (line_indent == indenting::indent)
        {
            basic_json_encoder<char_type,jsoncons::string_result<string_type>> encoder(s, options);
            dump(encoder);
        }
        else
        {
            basic_json_compressed_encoder<char_type,jsoncons::string_result<string_type>> encoder(s, options);
            dump(encoder);
        }
    }

    void dump(basic_json_content_handler<char_type>& handler) const
    {
        dump_noflush(handler);
        handler.flush();
    }

    void dump(std::basic_ostream<char_type>& os) const
    {
        basic_json_compressed_encoder<char_type> encoder(os);
        dump(encoder);
    }

    void dump(std::basic_ostream<char_type>& os, indenting line_indent) const
    {
        if (line_indent == indenting::indent)
        {
            basic_json_encoder<char_type> encoder(os);
            dump(encoder);
        }
        else
        {
            basic_json_compressed_encoder<char_type> encoder(os);
            dump(encoder);
        }
    }

    void dump(std::basic_ostream<char_type>& os, const basic_json_options<char_type>& options) const
    {
        basic_json_compressed_encoder<char_type> encoder(os, options);
        dump(encoder);
    }

    void dump(std::basic_ostream<char_type>& os, const basic_json_options<char_type>& options, indenting line_indent) const
    {
        if (line_indent == indenting::indent)
        {
            basic_json_encoder<char_type> encoder(os, options);
            dump(encoder);
        }
        else
        {
            basic_json_compressed_encoder<char_type> encoder(os, options);
            dump(encoder);
        }
    }

    string_type to_string(const char_allocator_type& allocator=char_allocator_type()) const noexcept
    {
        string_type s(allocator);
        basic_json_compressed_encoder<char_type,jsoncons::string_result<string_type>> encoder(s);
        dump(encoder);
        return s;
    }

    string_type to_string(const basic_json_options<char_type>& options,
                          const char_allocator_type& allocator=char_allocator_type()) const
    {
        string_type s(allocator);
        basic_json_compressed_encoder<char_type,jsoncons::string_result<string_type>> encoder(s,options);
        dump(encoder);
        return s;
    }

#if !defined(JSONCONS_NO_DEPRECATED)
    void dump_fragment(basic_json_content_handler<char_type>& handler) const
    {
        dump(handler);
    }

    void dump_body(basic_json_content_handler<char_type>& handler) const
    {
        dump(handler);
    }

    void dump(std::basic_ostream<char_type>& os, bool pprint) const
    {
        if (pprint)
        {
            basic_json_encoder<char_type> encoder(os);
            dump(encoder);
        }
        else
        {
            basic_json_compressed_encoder<char_type> encoder(os);
            dump(encoder);
        }
    }

    void dump(std::basic_ostream<char_type>& os, const basic_json_options<char_type>& options, bool pprint) const
    {
        if (pprint)
        {
            basic_json_encoder<char_type> encoder(os, options);
            dump(encoder);
        }
        else
        {
            basic_json_compressed_encoder<char_type> encoder(os, options);
            dump(encoder);
        }
    }

    void write_body(basic_json_content_handler<char_type>& handler) const
    {
        dump(handler);
    }
    void write(basic_json_content_handler<char_type>& handler) const
    {
        dump(handler);
    }

    void write(std::basic_ostream<char_type>& os) const
    {
        dump(os);
    }

    void write(std::basic_ostream<char_type>& os, const basic_json_options<char_type>& options) const
    {
        dump(os,options);
    }

    void write(std::basic_ostream<char_type>& os, const basic_json_options<char_type>& options, bool pprint) const
    {
        dump(os,options,pprint);
    }

    void to_stream(basic_json_content_handler<char_type>& handler) const
    {
        dump(handler);
    }

    void to_stream(std::basic_ostream<char_type>& os) const
    {
        dump(os);
    }

    void to_stream(std::basic_ostream<char_type>& os, const basic_json_options<char_type>& options) const
    {
        dump(os,options);
    }

    void to_stream(std::basic_ostream<char_type>& os, const basic_json_options<char_type>& options, bool pprint) const
    {
        dump(os,options,pprint ? indenting::indent : indenting::no_indent);
    }
#endif
    bool is_null() const noexcept
    {
        return var_.get_storage_type() == storage_type::null_val;
    }

    allocator_type get_allocator() const
    {
        switch (var_.get_storage_type())
        {
            case storage_type::long_string_val:
            {
                return var_.string_data_cast()->get_allocator();
            }
            case storage_type::byte_string_val:
            {
                return var_.byte_string_data_cast()->get_allocator();
            }
            case storage_type::array_val:
            {
                return var_.array_data_cast()->get_allocator();
            }
            case storage_type::object_val:
            {
                return var_.object_data_cast()->get_allocator();
            }
            default:
                return allocator_type();
        }
    }

    bool contains(const string_view_type& name) const
    {
        switch (var_.get_storage_type())
        {
        case storage_type::object_val:
            {
                const_object_iterator it = object_value().find(name);
                return it != object_range().end();
            }
            break;
        default:
            return false;
        }
    }

    size_t count(const string_view_type& name) const
    {
        switch (var_.get_storage_type())
        {
        case storage_type::object_val:
            {
                auto it = object_value().find(name);
                if (it == object_range().end())
                {
                    return 0;
                }
                size_t count = 0;
                while (it != object_range().end()&& it->key() == name)
                {
                    ++count;
                    ++it;
                }
                return count;
            }
            break;
        default:
            return 0;
        }
    }

    template<class T, class... Args>
    bool is(Args&&... args) const
    {
        return json_type_traits<basic_json,T>::is(*this,std::forward<Args>(args)...);
    }

    bool is_string() const noexcept
    {
        return (var_.get_storage_type() == storage_type::long_string_val) || (var_.get_storage_type() == storage_type::short_string_val);
    }

    bool is_string_view() const noexcept
    {
        return is_string();
    }

    bool is_byte_string() const noexcept
    {
        return var_.get_storage_type() == storage_type::byte_string_val;
    }

    bool is_byte_string_view() const noexcept
    {
        return is_byte_string();
    }

    bool is_bignum() const
    {
        switch (get_storage_type())
        {
            case storage_type::short_string_val:
            case storage_type::long_string_val:
                return jsoncons::detail::is_integer(as_string_view().data(), as_string_view().length());
            case storage_type::int64_val:
            case storage_type::uint64_val:
                return true;
            default:
                return false;
        }
    }

    bool is_bool() const noexcept
    {
        return var_.get_storage_type() == storage_type::bool_val;
    }

    bool is_object() const noexcept
    {
        return var_.get_storage_type() == storage_type::object_val || var_.get_storage_type() == storage_type::empty_object_val;
    }

    bool is_array() const noexcept
    {
        return var_.get_storage_type() == storage_type::array_val;
    }

    bool is_int64() const noexcept
    {
        return var_.get_storage_type() == storage_type::int64_val || (var_.get_storage_type() == storage_type::uint64_val&& (as_integer<uint64_t>() <= static_cast<uint64_t>((std::numeric_limits<int64_t>::max)())));
    }

    bool is_uint64() const noexcept
    {
        return var_.get_storage_type() == storage_type::uint64_val || (var_.get_storage_type() == storage_type::int64_val&& as_integer<int64_t>() >= 0);
    }

    bool is_double() const noexcept
    {
        return var_.get_storage_type() == storage_type::double_val;
    }

    bool is_number() const noexcept
    {
        switch (var_.get_storage_type())
        {
            case storage_type::int64_val:
            case storage_type::uint64_val:
            case storage_type::double_val:
                return true;
            case storage_type::short_string_val:
            case storage_type::long_string_val:
                return var_.get_semantic_tag() == semantic_tag::bigint ||
                       var_.get_semantic_tag() == semantic_tag::bigdec ||
                       var_.get_semantic_tag() == semantic_tag::bigfloat;
#if !defined(JSONCONS_NO_DEPRECATED)
            case storage_type::array_val:
                return var_.get_semantic_tag() == semantic_tag::bigfloat;
#endif
            default:
                return false;
        }
    }

    bool empty() const noexcept
    {
        switch (var_.get_storage_type())
        {
            case storage_type::byte_string_val:
                return var_.byte_string_data_cast()->length() == 0;
                break;
            case storage_type::short_string_val:
                return var_.short_string_data_cast()->length() == 0;
            case storage_type::long_string_val:
                return var_.string_data_cast()->length() == 0;
            case storage_type::array_val:
                return array_value().size() == 0;
            case storage_type::empty_object_val:
                return true;
            case storage_type::object_val:
                return object_value().size() == 0;
            default:
                return false;
        }
    }

    size_t capacity() const
    {
        switch (var_.get_storage_type())
        {
        case storage_type::array_val:
            return array_value().capacity();
        case storage_type::object_val:
            return object_value().capacity();
        default:
            return 0;
        }
    }

    template<class U=Allocator>
    void create_object_implicitly()
    {
        static_assert(is_stateless<U>::value, "Cannot create object implicitly - allocator is stateful.");
        var_ = variant(object(Allocator()), semantic_tag::none);
    }

    void reserve(size_t n)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::array_val:
            array_value().reserve(n);
            break;
        case storage_type::empty_object_val:
        {
            create_object_implicitly();
            object_value().reserve(n);
        }
        break;
        case storage_type::object_val:
        {
            object_value().reserve(n);
        }
            break;
        default:
            break;
        }
    }

    void resize(size_t n)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::array_val:
            array_value().resize(n);
            break;
        default:
            break;
        }
    }

    template <class T>
    void resize(size_t n, T val)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::array_val:
            array_value().resize(n, val);
            break;
        default:
            break;
        }
    }

    template<class T, class... Args>
    T as(Args&&... args) const
    {
        return json_type_traits<basic_json,T>::as(*this,std::forward<Args>(args)...);
    }

    template<class T>
    typename std::enable_if<std::is_same<string_type,T>::value,T>::type 
    as(const char_allocator_type& allocator) const
    {
        return json_type_traits<basic_json,T>::as(*this,allocator);
    }

    bool as_bool() const 
    {
        switch (var_.get_storage_type())
        {
        case storage_type::short_string_val:
        case storage_type::long_string_val:
            if (var_.get_semantic_tag() == semantic_tag::bigint)
            {
                return static_cast<bool>(var_.as_bignum());
            }

            try
            {
                basic_json j = basic_json::parse(as_string_view());
                return j.as_bool();
            }
            catch (...)
            {
                JSONCONS_THROW(json_runtime_error<std::runtime_error>("Not a bool"));
            }
            break;
        case storage_type::bool_val:
            return var_.bool_data_cast()->value();
        case storage_type::double_val:
            return var_.double_data_cast()->value() != 0.0;
        case storage_type::int64_val:
            return var_.int64_data_cast()->value() != 0;
        case storage_type::uint64_val:
            return var_.uint64_data_cast()->value() != 0;
        default:
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Not a bool"));
        }
    }

    template <class T
#if !defined(JSONCONS_NO_DEPRECATED)
         = int64_t
#endif
    >
    T as_integer() const
    {
        switch (var_.get_storage_type())
        {
        case storage_type::short_string_val:
        case storage_type::long_string_val:
            {
                auto result = jsoncons::detail::to_integer<T>(as_string_view().data(), as_string_view().length());
                if (result.ec != jsoncons::detail::to_integer_errc())
                {
                    JSONCONS_THROW(json_runtime_error<std::runtime_error>(make_error_code(result.ec).message()));
                }
                return result.value;
            }
        case storage_type::double_val:
            return static_cast<T>(var_.double_data_cast()->value());
        case storage_type::int64_val:
            return static_cast<T>(var_.int64_data_cast()->value());
        case storage_type::uint64_val:
            return static_cast<T>(var_.uint64_data_cast()->value());
        case storage_type::bool_val:
            return static_cast<T>(var_.bool_data_cast()->value() ? 1 : 0);
        default:
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Not an integer"));
        }
    }

#if !defined(JSONCONS_NO_DEPRECATED)
    size_t precision() const
    {
        switch (var_.get_storage_type())
        {
        case storage_type::double_val:
            return 0;
        default:
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Not a double"));
        }
    }

    size_t decimal_places() const
    {
        switch (var_.get_storage_type())
        {
        case storage_type::double_val:
            return 0;
        default:
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Not a double"));
        }
    }
#endif

    double as_double() const
    {
        switch (var_.get_storage_type())
        {
            case storage_type::short_string_val:
            case storage_type::long_string_val:
            {
                jsoncons::detail::string_to_double to_double;
                // to_double() throws std::invalid_argument if conversion fails
                return to_double(as_cstring(), as_string_view().length());
            }
            case storage_type::double_val:
                return var_.double_data_cast()->value();
            case storage_type::int64_val:
                return static_cast<double>(var_.int64_data_cast()->value());
            case storage_type::uint64_val:
                return static_cast<double>(var_.uint64_data_cast()->value());
#if !defined(JSONCONS_NO_DEPRECATED)
            case storage_type::array_val:
                if (get_semantic_tag() == semantic_tag::bigfloat)
                {
                    jsoncons::detail::string_to_double to_double;
                    string_type s = as_string();
                    return to_double(s.c_str(), s.length());
                }
                else
                {
                    JSONCONS_THROW(json_runtime_error<std::invalid_argument>("Not a double"));
                }
#endif
            default:
                JSONCONS_THROW(json_runtime_error<std::invalid_argument>("Not a double"));
        }
    }

    string_view_type as_string_view() const
    {
        return var_.as_string_view();
    }

    byte_string_view as_byte_string_view() const
    {
        return var_.as_byte_string_view();
    }

    template <typename BAllocator=std::allocator<uint8_t>>
    basic_byte_string<BAllocator> as_byte_string() const
    {
        return var_.template as_byte_string<BAllocator>();
    }

    basic_bignum<byte_allocator_type> as_bignum() const
    {
        return var_.as_bignum();
    }

    template <class SAllocator=std::allocator<CharT>>
    string_type as_string() const 
    {
        return as_string(basic_json_options<char_type>(),SAllocator());
    }

    template <class SAllocator=std::allocator<CharT>>
    string_type as_string(const SAllocator& allocator) const 
    {
        return as_string(basic_json_options<char_type>(),allocator);
    }

    template <class SAllocator=std::allocator<CharT>>
    string_type as_string(const basic_json_options<char_type>& options) const 
    {
        return as_string(options,SAllocator());
    }

    template <class SAllocator=std::allocator<CharT>>
    string_type as_string(const basic_json_options<char_type>& options,
                          const SAllocator& allocator) const 
    {
        switch (var_.get_storage_type())
        {
            case storage_type::short_string_val:
            case storage_type::long_string_val:
            {
                return string_type(as_string_view().data(),as_string_view().length(),allocator);
            }
            case storage_type::byte_string_val:
            {
                string_type s(allocator);
                byte_string_chars_format format = jsoncons::detail::resolve_byte_string_chars_format(options.byte_string_format(), 
                                                                                           byte_string_chars_format::none, 
                                                                                           byte_string_chars_format::base64url);
                switch (format)
                {
                    case byte_string_chars_format::base64:
                        encode_base64(var_.byte_string_data_cast()->begin(), 
                                      var_.byte_string_data_cast()->end(),
                                      s);
                        break;
                    case byte_string_chars_format::base16:
                        encode_base16(var_.byte_string_data_cast()->begin(), 
                                      var_.byte_string_data_cast()->end(),
                                      s);
                        break;
                    default:
                        encode_base64url(var_.byte_string_data_cast()->begin(), 
                                         var_.byte_string_data_cast()->end(),
                                         s);
                        break;
                }
                return s;
            }
            case storage_type::array_val:
            {
                string_type s(allocator);
#if !defined(JSONCONS_NO_DEPRECATED)
                if (get_semantic_tag() == semantic_tag::bigfloat)
                {
                    JSONCONS_ASSERT(size() == 2);
                    int64_t exp = at(0).template as_integer<int64_t>();
                    string_type mantissa = at(1).as_string();
                    bignum n(mantissa);
                    int64_t new_exp = 0;
                    bignum five(5);
                    if (exp > 0)
                    {
                        new_exp = static_cast<int64_t>(std::floor(exp*std::log(2)/std::log(10)));
                        bignum five_power = power(five,(unsigned)new_exp);
                        uint64_t binShift = exp - new_exp;
                        n = ((n) << (unsigned)binShift)/five_power;
                    }
                    else
                    {
                        new_exp = static_cast<int64_t>(std::ceil(-exp*std::log(2)/std::log(10)));
                        bignum five_power = power(five,(unsigned)new_exp);
                        uint64_t binShift = -exp - new_exp;
                        n = (n*five_power) >> (unsigned)binShift;
                    }

                    std::string str;
                    n.dump(str);
                    if (str[0] == '-')
                    {
                        s.push_back('-');
                        jsoncons::detail::prettify_string(str.c_str()+1, str.size()-1, -(int)new_exp, -4, 17, s);
                    }
                    else
                    {
                        jsoncons::detail::prettify_string(str.c_str(), str.size(), -(int)new_exp, -4, 17, s);
                    }
                }
                else
#endif
                {
                    basic_json_compressed_encoder<char_type,jsoncons::string_result<string_type>> encoder(s,options);
                    dump(encoder);
                }
                return s;
            }
            default:
            {
                string_type s(allocator);
                basic_json_compressed_encoder<char_type,jsoncons::string_result<string_type>> encoder(s,options);
                dump(encoder);
                return s;
            }
        }
    }

    const char_type* as_cstring() const
    {
        switch (var_.get_storage_type())
        {
        case storage_type::short_string_val:
            return var_.short_string_data_cast()->c_str();
        case storage_type::long_string_val:
            return var_.string_data_cast()->c_str();
        default:
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Not a cstring"));
        }
    }

#if !defined(JSONCONS_NO_DEPRECATED)

    bool is_datetime() const noexcept
    {
        return var_.get_semantic_tag() == semantic_tag::datetime;
    }

    bool is_epoch_time() const noexcept
    {
        return var_.get_semantic_tag() == semantic_tag::timestamp;
    }

    bool has_key(const string_view_type& name) const
    {
        return contains(name);
    }

    bool is_integer() const noexcept
    {
        return var_.get_storage_type() == storage_type::int64_val || (var_.get_storage_type() == storage_type::uint64_val&& (as_integer<uint64_t>() <= static_cast<uint64_t>((std::numeric_limits<int64_t>::max)())));
    }

    bool is_uinteger() const noexcept
    {
        return var_.get_storage_type() == storage_type::uint64_val || (var_.get_storage_type() == storage_type::int64_val&& as_integer<int64_t>() >= 0);
    }

    int64_t as_uinteger() const
    {
        return as_integer<uint64_t>();
    }

    size_t double_precision() const
    {
        switch (var_.get_storage_type())
        {
        case storage_type::double_val:
            return 0;
        default:
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Not a double"));
        }
    }
#endif

    basic_json& at(const string_view_type& name)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
            JSONCONS_THROW(key_not_found(name.data(),name.length()));
        case storage_type::object_val:
            {
                auto it = object_value().find(name);
                if (it == object_range().end())
                {
                    JSONCONS_THROW(key_not_found(name.data(),name.length()));
                }
                return it->value();
            }
            break;
        default:
            {
                JSONCONS_THROW(not_an_object(name.data(),name.length()));
            }
        }
    }

    basic_json& evaluate() 
    {
        return *this;
    }

    basic_json& evaluate_with_default() 
    {
        return *this;
    }

    const basic_json& evaluate() const
    {
        return *this;
    }
    basic_json& evaluate(const string_view_type& name) 
    {
        return at(name);
    }

    const basic_json& evaluate(const string_view_type& name) const
    {
        return at(name);
    }

    const basic_json& at(const string_view_type& name) const
    {
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
            JSONCONS_THROW(key_not_found(name.data(),name.length()));
        case storage_type::object_val:
            {
                auto it = object_value().find(name);
                if (it == object_range().end())
                {
                    JSONCONS_THROW(key_not_found(name.data(),name.length()));
                }
                return it->value();
            }
            break;
        default:
            {
                JSONCONS_THROW(not_an_object(name.data(),name.length()));
            }
        }
    }

    basic_json& at(size_t i)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::array_val:
            if (i >= array_value().size())
            {
                JSONCONS_THROW(json_runtime_error<std::out_of_range>("Invalid array subscript"));
            }
            return array_value().operator[](i);
        case storage_type::object_val:
            return object_value().at(i);
        default:
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Index on non-array value not supported"));
        }
    }

    const basic_json& at(size_t i) const
    {
        switch (var_.get_storage_type())
        {
        case storage_type::array_val:
            if (i >= array_value().size())
            {
                JSONCONS_THROW(json_runtime_error<std::out_of_range>("Invalid array subscript"));
            }
            return array_value().operator[](i);
        case storage_type::object_val:
            return object_value().at(i);
        default:
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Index on non-array value not supported"));
        }
    }

    object_iterator find(const string_view_type& name)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
            return object_range().end();
        case storage_type::object_val:
            return object_value().find(name);
        default:
            {
                JSONCONS_THROW(not_an_object(name.data(),name.length()));
            }
        }
    }

    const_object_iterator find(const string_view_type& name) const
    {
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
            return object_range().end();
        case storage_type::object_val:
            return object_value().find(name);
        default:
            {
                JSONCONS_THROW(not_an_object(name.data(),name.length()));
            }
        }
    }

    template<class T>
    T get_with_default(const string_view_type& name, const T& default_val) const
    {
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
            {
                return default_val;
            }
        case storage_type::object_val:
            {
                const_object_iterator it = object_value().find(name);
                if (it != object_range().end())
                {
                    return it->value().template as<T>();
                }
                else
                {
                    return default_val;
                }
            }
        default:
            {
                JSONCONS_THROW(not_an_object(name.data(),name.length()));
            }
        }
    }

    template<class T = std::basic_string<CharT>>
    T get_with_default(const string_view_type& name, const CharT* default_val) const
    {
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
            {
                return T(default_val);
            }
        case storage_type::object_val:
            {
                const_object_iterator it = object_value().find(name);
                if (it != object_range().end())
                {
                    return it->value().template as<T>();
                }
                else
                {
                    return T(default_val);
                }
            }
        default:
            {
                JSONCONS_THROW(not_an_object(name.data(),name.length()));
            }
        }
    }

    // Modifiers

    void shrink_to_fit()
    {
        switch (var_.get_storage_type())
        {
        case storage_type::array_val:
            array_value().shrink_to_fit();
            break;
        case storage_type::object_val:
            object_value().shrink_to_fit();
            break;
        default:
            break;
        }
    }

    void clear()
    {
        switch (var_.get_storage_type())
        {
        case storage_type::array_val:
            array_value().clear();
            break;
        case storage_type::object_val:
            object_value().clear();
            break;
        default:
            break;
        }
    }

    void erase(const_object_iterator pos)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
            break;
        case storage_type::object_val:
            object_value().erase(pos);
            break;
        default:
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Not an object"));
            break;
        }
    }

    void erase(const_object_iterator first, const_object_iterator last)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
            break;
        case storage_type::object_val:
            object_value().erase(first, last);
            break;
        default:
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Not an object"));
            break;
        }
    }

    void erase(const_array_iterator pos)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::array_val:
            array_value().erase(pos);
            break;
        default:
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Not an array"));
            break;
        }
    }

    void erase(const_array_iterator first, const_array_iterator last)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::array_val:
            array_value().erase(first, last);
            break;
        default:
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Not an array"));
            break;
        }
    }

    // Removes all elements from an array value whose index is between from_index, inclusive, and to_index, exclusive.

    void erase(const string_view_type& name)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
            break;
        case storage_type::object_val:
            object_value().erase(name);
            break;
        default:
            JSONCONS_THROW(not_an_object(name.data(),name.length()));
            break;
        }
    }

    template <class T>
    std::pair<object_iterator,bool> insert_or_assign(const string_view_type& name, T&& val)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
            create_object_implicitly();
            JSONCONS_FALLTHROUGH;
        case storage_type::object_val:
            return object_value().insert_or_assign(name, std::forward<T>(val));
        default:
            {
                JSONCONS_THROW(not_an_object(name.data(),name.length()));
            }
        }
    }

    template <class ... Args>
    std::pair<object_iterator,bool> try_emplace(const string_view_type& name, Args&&... args)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
            create_object_implicitly();
            JSONCONS_FALLTHROUGH;
        case storage_type::object_val:
            return object_value().try_emplace(name, std::forward<Args>(args)...);
        default:
            {
                JSONCONS_THROW(not_an_object(name.data(),name.length()));
            }
        }
    }

    // merge

    void merge(const basic_json& source)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
            create_object_implicitly();
            JSONCONS_FALLTHROUGH;
        case storage_type::object_val:
            return object_value().merge(source.object_value());
        default:
            {
                JSONCONS_THROW(json_runtime_error<std::runtime_error>("Attempting to merge a value that is not an object"));
            }
        }
    }

    void merge(basic_json&& source)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
            create_object_implicitly();
            JSONCONS_FALLTHROUGH;
        case storage_type::object_val:
            return object_value().merge(std::move(source.object_value()));
        default:
            {
                JSONCONS_THROW(json_runtime_error<std::runtime_error>("Attempting to merge a value that is not an object"));
            }
        }
    }

    void merge(object_iterator hint, const basic_json& source)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
            create_object_implicitly();
            JSONCONS_FALLTHROUGH;
        case storage_type::object_val:
            return object_value().merge(hint, source.object_value());
        default:
            {
                JSONCONS_THROW(json_runtime_error<std::runtime_error>("Attempting to merge a value that is not an object"));
            }
        }
    }

    void merge(object_iterator hint, basic_json&& source)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
            create_object_implicitly();
            JSONCONS_FALLTHROUGH;
        case storage_type::object_val:
            return object_value().merge(hint, std::move(source.object_value()));
        default:
            {
                JSONCONS_THROW(json_runtime_error<std::runtime_error>("Attempting to merge a value that is not an object"));
            }
        }
    }

    // merge_or_update

    void merge_or_update(const basic_json& source)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
            create_object_implicitly();
            JSONCONS_FALLTHROUGH;
        case storage_type::object_val:
            return object_value().merge_or_update(source.object_value());
        default:
            {
                JSONCONS_THROW(json_runtime_error<std::runtime_error>("Attempting to merge or update a value that is not an object"));
            }
        }
    }

    void merge_or_update(basic_json&& source)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
            create_object_implicitly();
            JSONCONS_FALLTHROUGH;
        case storage_type::object_val:
            return object_value().merge_or_update(std::move(source.object_value()));
        default:
            {
                JSONCONS_THROW(json_runtime_error<std::runtime_error>("Attempting to merge or update a value that is not an object"));
            }
        }
    }

    void merge_or_update(object_iterator hint, const basic_json& source)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
            create_object_implicitly();
            JSONCONS_FALLTHROUGH;
        case storage_type::object_val:
            return object_value().merge_or_update(hint, source.object_value());
        default:
            {
                JSONCONS_THROW(json_runtime_error<std::runtime_error>("Attempting to merge or update a value that is not an object"));
            }
        }
    }

    void merge_or_update(object_iterator hint, basic_json&& source)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
            create_object_implicitly();
            JSONCONS_FALLTHROUGH;
        case storage_type::object_val:
            return object_value().merge_or_update(hint, std::move(source.object_value()));
        default:
            {
                JSONCONS_THROW(json_runtime_error<std::runtime_error>("Attempting to merge or update a value that is not an object"));
            }
        }
    }

    template <class T>
    object_iterator insert_or_assign(object_iterator hint, const string_view_type& name, T&& val)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
            create_object_implicitly();
            JSONCONS_FALLTHROUGH;
        case storage_type::object_val:
            return object_value().insert_or_assign(hint, name, std::forward<T>(val));
        default:
            {
                JSONCONS_THROW(not_an_object(name.data(),name.length()));
            }
        }
    }

    template <class ... Args>
    object_iterator try_emplace(object_iterator hint, const string_view_type& name, Args&&... args)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
            create_object_implicitly();
            JSONCONS_FALLTHROUGH;
        case storage_type::object_val:
            return object_value().try_emplace(hint, name, std::forward<Args>(args)...);
        default:
            {
                JSONCONS_THROW(not_an_object(name.data(),name.length()));
            }
        }
    }

    template <class T>
    array_iterator insert(const_array_iterator pos, T&& val)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::array_val:
            return array_value().insert(pos, std::forward<T>(val));
            break;
        default:
            {
                JSONCONS_THROW(json_runtime_error<std::runtime_error>("Attempting to insert into a value that is not an array"));
            }
        }
    }

    template <class InputIt>
    array_iterator insert(const_array_iterator pos, InputIt first, InputIt last)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::array_val:
            return array_value().insert(pos, first, last);
            break;
        default:
            {
                JSONCONS_THROW(json_runtime_error<std::runtime_error>("Attempting to insert into a value that is not an array"));
            }
        }
    }

    template <class InputIt>
    void insert(InputIt first, InputIt last)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
        case storage_type::object_val:
            return object_value().insert(first, last, get_key_value<string_type,json_type>());
            break;
        default:
            {
                JSONCONS_THROW(json_runtime_error<std::runtime_error>("Attempting to insert into a value that is not an object"));
            }
        }
    }

    template <class InputIt>
    void insert(sorted_unique_range_tag tag, InputIt first, InputIt last)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
        case storage_type::object_val:
            return object_value().insert(tag, first, last, get_key_value<string_type,json_type>());
            break;
        default:
            {
                JSONCONS_THROW(json_runtime_error<std::runtime_error>("Attempting to insert into a value that is not an object"));
            }
        }
    }

    template <class... Args> 
    array_iterator emplace(const_array_iterator pos, Args&&... args)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::array_val:
            return array_value().emplace(pos, std::forward<Args>(args)...);
            break;
        default:
            {
                JSONCONS_THROW(json_runtime_error<std::runtime_error>("Attempting to insert into a value that is not an array"));
            }
        }
    }

    template <class... Args> 
    basic_json& emplace_back(Args&&... args)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::array_val:
            return array_value().emplace_back(std::forward<Args>(args)...);
        default:
            {
                JSONCONS_THROW(json_runtime_error<std::runtime_error>("Attempting to insert into a value that is not an array"));
            }
        }
    }

    storage_type get_storage_type() const
    {
        return var_.get_storage_type();
    }

    semantic_tag get_semantic_tag() const
    {
        return var_.get_semantic_tag();
    }

    void swap(basic_json& b)
    {
        var_.swap(b.var_);
    }

    friend void swap(basic_json& a, basic_json& b)
    {
        a.swap(b);
    }

    template <class T>
    void push_back(T&& val)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::array_val:
            array_value().push_back(std::forward<T>(val));
            break;
        default:
            {
                JSONCONS_THROW(json_runtime_error<std::runtime_error>("Attempting to insert into a value that is not an array"));
            }
        }
    }

#if !defined(JSONCONS_NO_DEPRECATED)

    template <class T>
    void add(T&& val)
    {
        push_back(std::forward<T>(val));
    }

    template <class T>
    array_iterator add(const_array_iterator pos, T&& val)
    {
        return insert(pos, std::forward<T>(val));
    }

    template <class T>
    std::pair<object_iterator,bool> set(const string_view_type& name, T&& val)
    {
        return insert_or_assign(name, std::forward<T>(val));
    }

    // set

    template <class T>
    object_iterator set(object_iterator hint, const string_view_type& name, T&& val)
    {
        return insert_or_assign(hint, name, std::forward<T>(val));
    }

    static basic_json parse_file(const std::basic_string<char_type,char_traits_type>& filename)
    {
        parse_error_handler_type err_handler;
        return parse_file(filename,err_handler);
    }

    static basic_json parse_file(const std::basic_string<char_type,char_traits_type>& filename,
                                 parse_error_handler& err_handler)
    {
        std::basic_ifstream<CharT> is(filename);
        return parse(is,err_handler);
    }

    static basic_json parse_stream(std::basic_istream<char_type>& is)
    {
        return parse(is);
    }
    static basic_json parse_stream(std::basic_istream<char_type>& is, parse_error_handler& err_handler)
    {
        return parse(is,err_handler);
    }

    static basic_json parse_string(const string_type& s)
    {
        return parse(s);
    }

    static basic_json parse_string(const string_type& s, parse_error_handler& err_handler)
    {
        return parse(s,err_handler);
    }

    void resize_array(size_t n)
    {
        resize(n);
    }

    template <class T>
    void resize_array(size_t n, T val)
    {
        resize(n,val);
    }

    object_iterator begin_members()
    {
        return object_range().begin();
    }

    const_object_iterator begin_members() const
    {
        return object_range().begin();
    }

    object_iterator end_members()
    {
        return object_range().end();
    }

    const_object_iterator end_members() const
    {
        return object_range().end();
    }

    array_iterator begin_elements()
    {
        return array_range().begin();
    }

    const_array_iterator begin_elements() const
    {
        return array_range().begin();
    }

    array_iterator end_elements()
    {
        return array_range().end();
    }

    const_array_iterator end_elements() const
    {
        return array_range().end();
    }

    template<class T>
    basic_json get(const string_view_type& name, T&& default_val) const
    {
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
            {
                return basic_json(std::forward<T>(default_val));
            }
        case storage_type::object_val:
            {
                const_object_iterator it = object_value().find(name);
                if (it != object_range().end())
                {
                    return it->value();
                }
                else
                {
                    return basic_json(std::forward<T>(default_val));
                }
            }
        default:
            {
                JSONCONS_THROW(not_an_object(name.data(),name.length()));
            }
        }
    }

    const basic_json& get(const string_view_type& name) const
    {
        static const basic_json a_null = null_type();

        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
            return a_null;
        case storage_type::object_val:
            {
                const_object_iterator it = object_value().find(name);
                return it != object_range().end() ? it->value() : a_null;
            }
        default:
            {
                JSONCONS_THROW(not_an_object(name.data(),name.length()));
            }
        }
    }

    bool is_longlong() const noexcept
    {
        return var_.get_storage_type() == storage_type::int64_val;
    }

    bool is_ulonglong() const noexcept
    {
        return var_.get_storage_type() == storage_type::uint64_val;
    }

    long long as_longlong() const
    {
        return as_integer<int64_t>();
    }

    unsigned long long as_ulonglong() const
    {
        return as_integer<uint64_t>();
    }

    int as_int() const
    {
        switch (var_.get_storage_type())
        {
        case storage_type::double_val:
            return static_cast<int>(var_.double_data_cast()->value());
        case storage_type::int64_val:
            return static_cast<int>(var_.int64_data_cast()->value());
        case storage_type::uint64_val:
            return static_cast<int>(var_.uint64_data_cast()->value());
        case storage_type::bool_val:
            return var_.bool_data_cast()->value() ? 1 : 0;
        default:
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Not an int"));
        }
    }

    unsigned int as_uint() const
    {
        switch (var_.get_storage_type())
        {
        case storage_type::double_val:
            return static_cast<unsigned int>(var_.double_data_cast()->value());
        case storage_type::int64_val:
            return static_cast<unsigned int>(var_.int64_data_cast()->value());
        case storage_type::uint64_val:
            return static_cast<unsigned int>(var_.uint64_data_cast()->value());
        case storage_type::bool_val:
            return var_.bool_data_cast()->value() ? 1 : 0;
        default:
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Not an unsigned int"));
        }
    }

    long as_long() const
    {
        switch (var_.get_storage_type())
        {
        case storage_type::double_val:
            return static_cast<long>(var_.double_data_cast()->value());
        case storage_type::int64_val:
            return static_cast<long>(var_.int64_data_cast()->value());
        case storage_type::uint64_val:
            return static_cast<long>(var_.uint64_data_cast()->value());
        case storage_type::bool_val:
            return var_.bool_data_cast()->value() ? 1 : 0;
        default:
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Not a long"));
        }
    }

    unsigned long as_ulong() const
    {
        switch (var_.get_storage_type())
        {
        case storage_type::double_val:
            return static_cast<unsigned long>(var_.double_data_cast()->value());
        case storage_type::int64_val:
            return static_cast<unsigned long>(var_.int64_data_cast()->value());
        case storage_type::uint64_val:
            return static_cast<unsigned long>(var_.uint64_data_cast()->value());
        case storage_type::bool_val:
            return var_.bool_data_cast()->value() ? 1 : 0;
        default:
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Not an unsigned long"));
        }
    }

    bool has_member(const string_type& name) const
    {
        switch (var_.get_storage_type())
        {
        case storage_type::object_val:
            {
                const_object_iterator it = object_value().find(name);
                return it != object_range().end();
            }
            break;
        default:
            return false;
        }
    }

    void remove_range(size_t from_index, size_t to_index)
    {
        switch (var_.get_storage_type())
        {
        case storage_type::array_val:
            array_value().remove_range(from_index, to_index);
            break;
        default:
            break;
        }
    }
    // Removes all elements from an array value whose index is between from_index, inclusive, and to_index, exclusive.

    void remove(const string_view_type& name)
    {
        erase(name);
    }
    void remove_member(const string_view_type& name)
    {
        erase(name);
    }
    // Removes a member from an object value

    bool is_empty() const noexcept
    {
        return empty();
    }
    bool is_numeric() const noexcept
    {
        return is_number();
    }

    template<int size>
    static typename std::enable_if<size==1,basic_json>::type make_multi_array()
    {
        return make_array();
    }
    template<size_t size>
    static typename std::enable_if<size==1,basic_json>::type make_multi_array(size_t n)
    {
        return make_array(n);
    }
    template<size_t size,typename T>
    static typename std::enable_if<size==1,basic_json>::type make_multi_array(size_t n, T val)
    {
        return make_array(n,val);
    }
    template<size_t size>
    static typename std::enable_if<size==2,basic_json>::type make_multi_array(size_t m, size_t n)
    {
        return make_array<2>(m, n);
    }
    template<size_t size,typename T>
    static typename std::enable_if<size==2,basic_json>::type make_multi_array(size_t m, size_t n, T val)
    {
        return make_array<2>(m, n, val);
    }
    template<size_t size>
    static typename std::enable_if<size==3,basic_json>::type make_multi_array(size_t m, size_t n, size_t k)
    {
        return make_array<3>(m, n, k);
    }
    template<size_t size,typename T>
    static typename std::enable_if<size==3,basic_json>::type make_multi_array(size_t m, size_t n, size_t k, T val)
    {
        return make_array<3>(m, n, k, val);
    }
    range<object_iterator> members()
    {
        return object_range();
    }

    range<const_object_iterator> members() const
    {
        return object_range();
    }

    range<array_iterator> elements()
    {
        return array_range();
    }

    range<const_array_iterator> elements() const
    {
        return array_range();
    }
#endif

    range<object_iterator> object_range()
    {
        static basic_json empty_object = object();
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
            return range<object_iterator>(empty_object.object_range().begin(), empty_object.object_range().end());
        case storage_type::object_val:
            return range<object_iterator>(object_value().begin(),object_value().end());
        default:
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Not an object"));
        }
    }

    range<const_object_iterator> object_range() const
    {
        static const basic_json empty_object = object();
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
            return range<const_object_iterator>(empty_object.object_range().begin(), empty_object.object_range().end());
        case storage_type::object_val:
            return range<const_object_iterator>(object_value().begin(),object_value().end());
        default:
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Not an object"));
        }
    }

    range<array_iterator> array_range()
    {
        switch (var_.get_storage_type())
        {
        case storage_type::array_val:
            return range<array_iterator>(array_value().begin(),array_value().end());
        default:
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Not an array"));
        }
    }

    range<const_array_iterator> array_range() const
    {
        switch (var_.get_storage_type())
        {
        case storage_type::array_val:
            return range<const_array_iterator>(array_value().begin(),array_value().end());
        default:
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Not an array"));
        }
    }

    array& array_value() 
    {
        switch (var_.get_storage_type())
        {
        case storage_type::array_val:
            return var_.array_data_cast()->value();
        default:
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Bad array cast"));
            break;
        }
    }

    const array& array_value() const
    {
        switch (var_.get_storage_type())
        {
        case storage_type::array_val:
            return var_.array_data_cast()->value();
        default:
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Bad array cast"));
            break;
        }
    }

    object& object_value()
    {
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
            create_object_implicitly();
            JSONCONS_FALLTHROUGH;
        case storage_type::object_val:
            return var_.object_data_cast()->value();
        default:
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Bad object cast"));
            break;
        }
    }

    const object& object_value() const
    {
        switch (var_.get_storage_type())
        {
        case storage_type::empty_object_val:
            const_cast<basic_json*>(this)->create_object_implicitly(); // HERE
            JSONCONS_FALLTHROUGH;
        case storage_type::object_val:
            return var_.object_data_cast()->value();
        default:
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Bad object cast"));
            break;
        }
    }

private:

    void dump_noflush(basic_json_content_handler<char_type>& handler) const
    {
        switch (var_.get_storage_type())
        {
            case storage_type::short_string_val:
            case storage_type::long_string_val:
                handler.string_value(as_string_view(), var_.get_semantic_tag());
                break;
            case storage_type::byte_string_val:
                handler.byte_string_value(var_.byte_string_data_cast()->data(), var_.byte_string_data_cast()->length(), 
                                          var_.get_semantic_tag());
                break;
            case storage_type::double_val:
                handler.double_value(var_.double_data_cast()->value(), 
                                     var_.get_semantic_tag());
                break;
            case storage_type::int64_val:
                handler.int64_value(var_.int64_data_cast()->value(), var_.get_semantic_tag());
                break;
            case storage_type::uint64_val:
                handler.uint64_value(var_.uint64_data_cast()->value(), var_.get_semantic_tag());
                break;
            case storage_type::bool_val:
                handler.bool_value(var_.bool_data_cast()->value(), var_.get_semantic_tag());
                break;
            case storage_type::null_val:
                handler.null_value(var_.get_semantic_tag());
                break;
            case storage_type::empty_object_val:
                handler.begin_object(0, var_.get_semantic_tag());
                handler.end_object();
                break;
            case storage_type::object_val:
                {
                    handler.begin_object(size(), var_.get_semantic_tag());
                    const object& o = object_value();
                    for (const_object_iterator it = o.begin(); it != o.end(); ++it)
                    {
                        handler.name(string_view_type((it->key()).data(),it->key().length()));
                        it->value().dump_noflush(handler);
                    }
                    handler.end_object();
                }
                break;
            case storage_type::array_val:
                {
                    handler.begin_array(size(), var_.get_semantic_tag());
                    const array& o = array_value();
                    for (const_array_iterator it = o.begin(); it != o.end(); ++it)
                    {
                        it->dump_noflush(handler);
                    }
                    handler.end_array();
                }
                break;
            default:
                break;
        }
    }

    friend std::basic_ostream<char_type>& operator<<(std::basic_ostream<char_type>& os, const basic_json& o)
    {
        o.dump(os);
        return os;
    }

    friend std::basic_istream<char_type>& operator<<(std::basic_istream<char_type>& is, basic_json& o)
    {
        json_decoder<basic_json> handler;
        basic_json_reader<char_type,stream_source<char_type>> reader(is, handler);
        reader.read_next();
        reader.check_done();
        if (!handler.is_valid())
        {
            JSONCONS_THROW(json_runtime_error<std::runtime_error>("Failed to parse json stream"));
        }
        o = handler.get_result();
        return is;
    }
};

template <class Json>
void swap(typename Json::key_value_type& a, typename Json::key_value_type& b)
{
    a.swap(b);
}

template <class Json>
std::basic_istream<typename Json::char_type>& operator>>(std::basic_istream<typename Json::char_type>& is, Json& o)
{
    typedef typename Json::char_type char_type;

    json_decoder<Json> handler;
    basic_json_reader<char_type,stream_source<char_type>> reader(is, handler);
    reader.read_next();
    reader.check_done();
    if (!handler.is_valid())
    {
        JSONCONS_THROW(json_runtime_error<std::runtime_error>("Failed to parse json stream"));
    }
    o = handler.get_result();
    return is;
}

typedef basic_json<char,sorted_policy,std::allocator<char>> json;
typedef basic_json<wchar_t,sorted_policy,std::allocator<wchar_t>> wjson;
typedef basic_json<char, preserve_order_policy, std::allocator<char>> ojson;
typedef basic_json<wchar_t, preserve_order_policy, std::allocator<wchar_t>> wojson;

#if !defined(JSONCONS_NO_DEPRECATED)
typedef basic_json<wchar_t, preserve_order_policy, std::allocator<wchar_t>> owjson;
typedef json_decoder<json> json_deserializer;
typedef json_decoder<wjson> wjson_deserializer;
typedef json_decoder<ojson> ojson_deserializer;
typedef json_decoder<wojson> wojson_deserializer;
#endif

inline namespace literals {

inline 
jsoncons::json operator "" _json(const char* s, std::size_t n)
{
    return jsoncons::json::parse(jsoncons::json::string_view_type(s, n));
}

inline 
jsoncons::wjson operator "" _json(const wchar_t* s, std::size_t n)
{
    return jsoncons::wjson::parse(jsoncons::wjson::string_view_type(s, n));
}

inline
jsoncons::ojson operator "" _ojson(const char* s, std::size_t n)
{
    return jsoncons::ojson::parse(jsoncons::ojson::string_view_type(s, n));
}

inline
jsoncons::wojson operator "" _ojson(const wchar_t* s, std::size_t n)
{
    return jsoncons::wojson::parse(jsoncons::wojson::string_view_type(s, n));
}

}

}

#endif
