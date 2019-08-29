// Copyright 2013 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_JSON_CONTAINER_TYPES_HPP
#define JSONCONS_JSON_CONTAINER_TYPES_HPP

#include <string>
#include <vector>
#include <deque>
#include <exception>
#include <cstring>
#include <algorithm> // std::sort, std::stable_sort, std::lower_bound, std::unique
#include <utility>
#include <initializer_list>
#include <iterator> // std::iterator_traits
#include <memory> // std::allocator
#include <utility> // std::move
#include <type_traits> // std::enable_if
#include <jsoncons/json_exception.hpp>
#include <jsoncons/json_type_traits.hpp>

namespace jsoncons {

// key_value

template <class KeyT, class ValueT>
class key_value
{
public:
    typedef KeyT key_type;
    typedef ValueT value_type;
    typedef typename KeyT::allocator_type allocator_type;
    typedef typename value_type::string_view_type string_view_type;
private:
    key_type key_;
    value_type value_;
public:

    key_value()
    {
    }

    key_value(const key_type& name, const value_type& val)
        : key_(name), value_(val)
    {
    }

    key_value(const string_view_type& name)
        : key_(name)
    {
    }

    template <class T>
    key_value(key_type&& name, T&& val)
        : key_(std::forward<key_type>(name)), 
          value_(std::forward<T>(val))
    {
    }

    template <class T>
    key_value(key_type&& name, 
                   T&& val, 
                   const allocator_type& allocator)
        : key_(std::forward<key_type>(name)), value_(std::forward<T>(val), allocator)
    {
    }

    key_value(const key_value& member)
        : key_(member.key_), value_(member.value_)
    {
    }

    key_value(key_value&& member)
        : key_(std::move(member.key_)), value_(std::move(member.value_))
    {
    }

    string_view_type key() const
    {
        return string_view_type(key_.data(),key_.size());
    }

    value_type& value()
    {
        return value_;
    }

    const value_type& value() const
    {
        return value_;
    }

    template <class T>
    void value(T&& value)
    {
        value_ = std::forward<T>(value);
    }

    void swap(key_value& member)
    {
        key_.swap(member.key_);
        value_.swap(member.value_);
    }

    key_value& operator=(const key_value& member)
    {
        if (this != & member)
        {
            key_ = member.key_;
            value_ = member.value_;
        }
        return *this;
    }

    key_value& operator=(key_value&& member)
    {
        if (this != &member)
        {
            key_.swap(member.key_);
            value_.swap(member.value_);
        }
        return *this;
    }

    void shrink_to_fit() 
    {
        key_.shrink_to_fit();
        value_.shrink_to_fit();
    }
#if !defined(JSONCONS_NO_DEPRECATED)
    const key_type& name() const
    {
        return key_;
    }
#endif

    friend bool operator==(const key_value& lhs, const key_value& rhs)
    {
        return lhs.key_ == rhs.key_ && lhs.value_ == rhs.value_;
    }

    friend bool operator!=(const key_value& lhs, const key_value& rhs)
    {
        return !(lhs == rhs);
    }

    friend bool operator<(const key_value& lhs, const key_value& rhs)
    {
        if (lhs.key_ < rhs.key_)
        {
            return true;
        }
        if (lhs.key_ == rhs.key_ && lhs.value_ < rhs.value_)
        {
            return true;
        }
        return false;
    }

    friend bool operator<=(const key_value& lhs, const key_value& rhs)
    {
        return !(rhs < lhs);
    }

    friend bool operator>(const key_value& lhs, const key_value& rhs) 
    {
        return !(lhs <= rhs);
    }

    friend bool operator>=(const key_value& lhs, const key_value& rhs)
    {
        return !(lhs < rhs);
    }
};

template <class KeyT, class ValueT>
struct get_key_value
{
    typedef key_value<KeyT,ValueT> key_value_type;

    template <class T1,class T2>
    key_value_type operator()(const std::pair<T1,T2>& p)
    {
        return key_value_type(p.first,p.second);
    }
    template <class T1,class T2>
    key_value_type operator()(std::pair<T1,T2>&& p)
    {
        return key_value_type(std::forward<T1>(p.first),std::forward<T2>(p.second));
    }
    template <class T1,class T2>
    const key_value_type& operator()(const key_value<T1,T2>& p)
    {
        return p;
    }
    template <class T1,class T2>
    key_value_type operator()(key_value<T1,T2>&& p)
    {
        return std::move(p);
    }
};

// json_array

template <class Allocator>
class container_base
{
public:
    typedef Allocator allocator_type;
private:
    allocator_type allocator_;
public:
    container_base()
        : allocator_()
    {
    }
    container_base(const allocator_type& allocator)
        : allocator_(allocator)
    {
    }

    allocator_type get_allocator() const
    {
        return allocator_;
    }
};

// json_array

template <class Json>
class json_array : public container_base<typename Json::allocator_type>
{
public:
    typedef typename Json::allocator_type allocator_type;
    typedef Json value_type;
private:
    typedef typename Json::implementation_policy implementation_policy;
    typedef typename std::allocator_traits<allocator_type>:: template rebind_alloc<value_type> value_allocator_type;
    using value_container_type = typename implementation_policy::template sequence_container_type<value_type,value_allocator_type>;
    value_container_type elements_;
public:
    typedef typename value_container_type::iterator iterator;
    typedef typename value_container_type::const_iterator const_iterator;
    typedef typename std::iterator_traits<iterator>::reference reference;
    typedef typename std::iterator_traits<const_iterator>::reference const_reference;

    using container_base<allocator_type>::get_allocator;

    json_array()
    {
    }

    explicit json_array(const allocator_type& allocator)
        : container_base<allocator_type>(allocator), 
          elements_(value_allocator_type(allocator))
    {
    }

    explicit json_array(size_t n, 
                        const allocator_type& allocator = allocator_type())
        : container_base<allocator_type>(allocator), 
          elements_(n,Json(),value_allocator_type(allocator))
    {
    }

    explicit json_array(size_t n, 
                        const Json& value, 
                        const allocator_type& allocator = allocator_type())
        : container_base<allocator_type>(allocator), 
          elements_(n,value,value_allocator_type(allocator))
    {
    }

    template <class InputIterator>
    json_array(InputIterator begin, InputIterator end, const allocator_type& allocator = allocator_type())
        : container_base<allocator_type>(allocator), 
          elements_(begin,end,value_allocator_type(allocator))
    {
    }
    json_array(const json_array& val)
        : container_base<allocator_type>(val.get_allocator()),
          elements_(val.elements_)
    {
    }
    json_array(const json_array& val, const allocator_type& allocator)
        : container_base<allocator_type>(allocator), 
          elements_(val.elements_,value_allocator_type(allocator))
    {
    }

    json_array(json_array&& val) noexcept
        : container_base<allocator_type>(val.get_allocator()), 
          elements_(std::move(val.elements_))
    {
    }
    json_array(json_array&& val, const allocator_type& allocator)
        : container_base<allocator_type>(allocator), 
          elements_(std::move(val.elements_),value_allocator_type(allocator))
    {
    }

    json_array(std::initializer_list<Json> init)
        : container_base<allocator_type>(), 
          elements_(std::move(init))
    {
    }

    json_array(std::initializer_list<Json> init, 
               const allocator_type& allocator)
        : container_base<allocator_type>(allocator), 
          elements_(std::move(init),value_allocator_type(allocator))
    {
    }
    ~json_array()
    {
    }

    void swap(json_array<Json>& val)
    {
        elements_.swap(val.elements_);
    }

    size_t size() const {return elements_.size();}

    size_t capacity() const {return elements_.capacity();}

    void clear() {elements_.clear();}

    void shrink_to_fit() 
    {
        for (size_t i = 0; i < elements_.size(); ++i)
        {
            elements_[i].shrink_to_fit();
        }
        elements_.shrink_to_fit();
    }

    void reserve(size_t n) {elements_.reserve(n);}

    void resize(size_t n) {elements_.resize(n);}

    void resize(size_t n, const Json& val) {elements_.resize(n,val);}

#if !defined(JSONCONS_NO_DEPRECATED)
    void remove_range(size_t from_index, size_t to_index) 
    {
        JSONCONS_ASSERT(from_index <= to_index);
        JSONCONS_ASSERT(to_index <= elements_.size());
        elements_.erase(elements_.cbegin()+from_index,elements_.cbegin()+to_index);
    }
#endif
    void erase(const_iterator pos) 
    {
#if defined(JSONCONS_NO_ERASE_TAKING_CONST_ITERATOR)
        iterator it = elements_.begin() + (pos - elements_.begin());
        elements_.erase(it);
#else
        elements_.erase(pos);
#endif
    }

    void erase(const_iterator first, const_iterator last) 
    {
#if defined(JSONCONS_NO_ERASE_TAKING_CONST_ITERATOR)
        iterator it1 = elements_.begin() + (first - elements_.begin());
        iterator it2 = elements_.begin() + (last - elements_.begin());
        elements_.erase(it1,it2);
#else
        elements_.erase(first,last);
#endif
    }

    Json& operator[](size_t i) {return elements_[i];}

    const Json& operator[](size_t i) const {return elements_[i];}

    // push_back

    template <class T, class A=allocator_type>
    typename std::enable_if<is_stateless<A>::value,void>::type 
    push_back(T&& value)
    {
        elements_.emplace_back(std::forward<T>(value));
    }

    template <class T, class A=allocator_type>
    typename std::enable_if<!is_stateless<A>::value,void>::type 
    push_back(T&& value)
    {
        elements_.emplace_back(std::forward<T>(value),get_allocator());
    }

    template <class T, class A=allocator_type>
    typename std::enable_if<is_stateless<A>::value,iterator>::type 
    insert(const_iterator pos, T&& value)
    {
#if defined(JSONCONS_NO_ERASE_TAKING_CONST_ITERATOR)
        iterator it = elements_.begin() + (pos - elements_.begin());
        return elements_.emplace(it, std::forward<T>(value));
#else
        return elements_.emplace(pos, std::forward<T>(value));
#endif
    }
    template <class T, class A=allocator_type>
    typename std::enable_if<!is_stateless<A>::value,iterator>::type 
    insert(const_iterator pos, T&& value)
    {
#if defined(JSONCONS_NO_ERASE_TAKING_CONST_ITERATOR)
        iterator it = elements_.begin() + (pos - elements_.begin());
        return elements_.emplace(it, std::forward<T>(value), get_allocator());
#else
        return elements_.emplace(pos, std::forward<T>(value), get_allocator());
#endif
    }

    template <class InputIt>
    iterator insert(const_iterator pos, InputIt first, InputIt last)
    {
#if defined(JSONCONS_NO_ERASE_TAKING_CONST_ITERATOR)
        iterator it = elements_.begin() + (pos - elements_.begin());
        elements_.insert(it, first, last);
        return first == last ? it : it + 1;
#else
        return elements_.insert(pos, first, last);
#endif
    }

    template <class A=allocator_type, class... Args>
    typename std::enable_if<is_stateless<A>::value,iterator>::type 
    emplace(const_iterator pos, Args&&... args)
    {
#if defined(JSONCONS_NO_ERASE_TAKING_CONST_ITERATOR)
        iterator it = elements_.begin() + (pos - elements_.begin());
        return elements_.emplace(it, std::forward<Args>(args)...);
#else
        return elements_.emplace(pos, std::forward<Args>(args)...);
#endif
    }

    template <class... Args>
    Json& emplace_back(Args&&... args)
    {
        elements_.emplace_back(std::forward<Args>(args)...);
        return elements_.back();
    }

    iterator begin() {return elements_.begin();}

    iterator end() {return elements_.end();}

    const_iterator begin() const {return elements_.begin();}

    const_iterator end() const {return elements_.end();}

    bool operator==(const json_array<Json>& rhs) const
    {
        return elements_ == rhs.elements_;
    }

    bool operator<(const json_array<Json>& rhs) const
    {
        return elements_ < rhs.elements_;
    }
private:

    json_array& operator=(const json_array<Json>&) = delete;
};

struct sorted_unique_range_tag
{
};

// json_object

template <class KeyT,class Json,class Enable = void>
class json_object
{
};

// Sort keys
template <class KeyT,class Json>
class json_object<KeyT,Json,typename std::enable_if<!Json::implementation_policy::preserve_order>::type> : 
    public container_base<typename Json::allocator_type>
{
public:
    typedef typename Json::allocator_type allocator_type;
    typedef KeyT key_type;
    typedef key_value<KeyT,Json> key_value_type;
    typedef typename Json::char_type char_type;
    typedef typename Json::string_view_type string_view_type;
private:
    typedef typename Json::implementation_policy implementation_policy;
    typedef typename std::allocator_traits<allocator_type>:: template rebind_alloc<key_value_type> key_value_allocator_type;
    using key_value_container_type = typename implementation_policy::template sequence_container_type<key_value_type,key_value_allocator_type>;

    key_value_container_type members_;
public:
    typedef typename key_value_container_type::iterator iterator;
    typedef typename key_value_container_type::const_iterator const_iterator;

    using container_base<allocator_type>::get_allocator;

    json_object()
    {
    }

    explicit json_object(const allocator_type& allocator)
        : container_base<allocator_type>(allocator), 
          members_(key_value_allocator_type(allocator))
    {
    }

    json_object(const json_object& val)
        : container_base<allocator_type>(val.get_allocator()),
          members_(val.members_)
    {
    }

    json_object(json_object&& val)
        : container_base<allocator_type>(val.get_allocator()), 
          members_(std::move(val.members_))
    {
    }

    json_object(const json_object& val, const allocator_type& allocator) 
        : container_base<allocator_type>(allocator), 
          members_(val.members_,key_value_allocator_type(allocator))
    {
    }

    json_object(json_object&& val,const allocator_type& allocator) 
        : container_base<allocator_type>(allocator), members_(std::move(val.members_),key_value_allocator_type(allocator))
    {
    }

    template<class InputIt>
    json_object(InputIt first, InputIt last)
    {
        size_t count = std::distance(first,last);
        members_.reserve(count);
        for (auto s = first; s != last; ++s)
        {
            members_.emplace_back(get_key_value<KeyT,Json>()(*s));
        }
        std::stable_sort(members_.begin(),members_.end(),
                         [](const key_value_type& a, const key_value_type& b){return a.key().compare(b.key()) < 0;});
        auto it = std::unique(members_.begin(), members_.end(),
                              [](const key_value_type& a, const key_value_type& b){ return !(a.key().compare(b.key()));});
        members_.erase(it, members_.end());
    }

    template<class InputIt, class Key, class Value>
    json_object(InputIt first, InputIt last, 
                const allocator_type& allocator)
        : container_base<allocator_type>(allocator), 
          members_(key_value_allocator_type(allocator))
    {
        size_t count = std::distance(first,last);
        members_.reserve(count);
        for (auto s = first; s != last; ++s)
        {
            members_.emplace_back(get_key_value<KeyT,Json>()(*s));
        }
        std::stable_sort(members_.begin(),members_.end(),
                         [](const key_value_type& a, const key_value_type& b){return a.key().compare(b.key()) < 0;});
        auto it = std::unique(members_.begin(), members_.end(),
                              [](const key_value_type& a, const key_value_type& b){ return !(a.key().compare(b.key()));});
        members_.erase(it, members_.end());
    }

    json_object(std::initializer_list<typename Json::array> init)
    {
        for (const auto& element : init)
        {
            if (element.size() != 2 || !element[0].is_string())
            {
                JSONCONS_THROW(json_runtime_error<std::runtime_error>("Cannot create object from initializer list"));
                break;
            }
        }
        for (auto& element : init)
        {
            insert_or_assign(element[0].as_string_view(), std::move(element[1]));
        }
    }

    json_object(std::initializer_list<typename Json::array> init, 
                const allocator_type& allocator)
        : container_base<allocator_type>(allocator), 
          members_(key_value_allocator_type(allocator))
    {
        for (const auto& element : init)
        {
            if (element.size() != 2 || !element[0].is_string())
            {
                JSONCONS_THROW(json_runtime_error<std::runtime_error>("Cannot create object from initializer list"));
                break;
            }
        }
        for (auto& element : init)
        {
            insert_or_assign(element[0].as_string_view(), std::move(element[1]));
        }
    }

    void swap(json_object& val)
    {
        members_.swap(val.members_);
    }

    iterator begin()
    {
        return members_.begin();
    }

    iterator end()
    {
        return members_.end();
    }

    const_iterator begin() const
    {
        return members_.begin();
    }

    const_iterator end() const
    {
        return members_.end();
    }

    size_t size() const {return members_.size();}

    size_t capacity() const {return members_.capacity();}

    void clear() {members_.clear();}

    void shrink_to_fit() 
    {
        for (size_t i = 0; i < members_.size(); ++i)
        {
            members_[i].shrink_to_fit();
        }
        members_.shrink_to_fit();
    }

    void reserve(size_t n) {members_.reserve(n);}

    Json& at(size_t i) 
    {
        if (i >= members_.size())
        {
            JSONCONS_THROW(json_runtime_error<std::out_of_range>("Invalid array subscript"));
        }
        return members_[i].value();
    }

    const Json& at(size_t i) const 
    {
        if (i >= members_.size())
        {
            JSONCONS_THROW(json_runtime_error<std::out_of_range>("Invalid array subscript"));
        }
        return members_[i].value();
    }

    iterator find(const string_view_type& name)
    {
        auto it = std::lower_bound(members_.begin(),members_.end(), name, 
                                   [](const key_value_type& a, const string_view_type& k){return a.key().compare(k) < 0;});        
        auto result = (it != members_.end() && it->key() == name) ? it : members_.end();
        return result;
    }

    const_iterator find(const string_view_type& name) const
    {
        auto it = std::lower_bound(members_.begin(),members_.end(), 
                                   name, 
                                   [](const key_value_type& a, const string_view_type& k){return a.key().compare(k) < 0;});
        auto result = (it != members_.end() && it->key() == name) ? it : members_.end();
        return result;
    }

    void erase(const_iterator pos) 
    {
#if defined(JSONCONS_NO_ERASE_TAKING_CONST_ITERATOR)
        iterator it = members_.begin() + (pos - members_.begin());
        members_.erase(it);
#else
        members_.erase(pos);
#endif
    }

    void erase(const_iterator first, const_iterator last) 
    {
#if defined(JSONCONS_NO_ERASE_TAKING_CONST_ITERATOR)
        iterator it1 = members_.begin() + (first - members_.begin());
        iterator it2 = members_.begin() + (last - members_.begin());
        members_.erase(it1,it2);
#else
        members_.erase(first,last);
#endif
    }

    void erase(const string_view_type& name) 
    {
        auto it = std::lower_bound(members_.begin(),members_.end(), name, 
                                   [](const key_value_type& a, const string_view_type& k){return a.key().compare(k) < 0;});        
        if (it != members_.end() && it->key() == name)
        {
            members_.erase(it);
        }
    }

    template<class InputIt, class Convert>
    void insert(InputIt first, InputIt last, Convert convert)
    {
        size_t count = std::distance(first,last);
        members_.reserve(members_.size() + count);
        for (auto s = first; s != last; ++s)
        {
            members_.emplace_back(convert(*s));
        }
        std::stable_sort(members_.begin(),members_.end(),
                         [](const key_value_type& a, const key_value_type& b){return a.key().compare(b.key()) < 0;});
        auto it = std::unique(members_.begin(), members_.end(),
                              [](const key_value_type& a, const key_value_type& b){ return !(a.key().compare(b.key()));});
        members_.erase(it, members_.end());
    }

    template<class InputIt, class Convert>
    void insert(sorted_unique_range_tag, InputIt first, InputIt last, Convert convert)
    {
        if (first != last)
        {
            size_t count = std::distance(first,last);
            members_.reserve(members_.size() + count);

            auto it = find(convert(*first).key());
            if (it != members_.end())
            {
                for (auto s = first; s != last; ++s)
                {
                    it = members_.emplace(it, convert(*s));
                }
            }
            else
            {
                for (auto s = first; s != last; ++s)
                {
                    members_.emplace_back(convert(*s));
                }
            }
        }
    }

    // insert_or_assign

    template <class T, class A=allocator_type>
    typename std::enable_if<is_stateless<A>::value,std::pair<iterator,bool>>::type
    insert_or_assign(const string_view_type& name, T&& value)
    {
        bool inserted;
        auto it = std::lower_bound(members_.begin(),members_.end(), name, 
                                   [](const key_value_type& a, const string_view_type& k){return a.key().compare(k) < 0;});        
        if (it == members_.end())
        {
            members_.emplace_back(key_type(name.begin(),name.end()), 
                                        std::forward<T>(value));
            inserted = true;
            it = members_.begin() + members_.size() - 1;
        }
        else if (it->key() == name)
        {
            it->value(Json(std::forward<T>(value)));
            inserted = false; // assigned
        }
        else
        {
            it = members_.emplace(it,
                                        key_type(name.begin(),name.end()),
                                        std::forward<T>(value));
            inserted = true;
        }
        return std::make_pair(it,inserted);
    }

    template <class T, class A=allocator_type>
    typename std::enable_if<!is_stateless<A>::value,std::pair<iterator,bool>>::type
    insert_or_assign(const string_view_type& name, T&& value)
    {
        bool inserted;
        auto it = std::lower_bound(members_.begin(),members_.end(), name, 
                                   [](const key_value_type& a, const string_view_type& k){return a.key().compare(k) < 0;});        
        if (it == members_.end())
        {
            members_.emplace_back(key_type(name.begin(),name.end(), get_allocator()), 
                                        std::forward<T>(value),get_allocator());
            inserted = true;
            it = members_.begin() + members_.size() - 1;
        }
        else if (it->key() == name)
        {
            it->value(Json(std::forward<T>(value), get_allocator()));
            inserted = false; // assigned
        }
        else
        {
            it = members_.emplace(it,
                                        key_type(name.begin(),name.end(), get_allocator()),
                                        std::forward<T>(value),get_allocator());
            inserted = true;
        }
        return std::make_pair(it,inserted);
    }

    // try_emplace

    template <class A=allocator_type, class... Args>
    typename std::enable_if<is_stateless<A>::value,std::pair<iterator,bool>>::type
    try_emplace(const string_view_type& name, Args&&... args)
    {
        bool inserted;
        auto it = std::lower_bound(members_.begin(),members_.end(), name, 
                                   [](const key_value_type& a, const string_view_type& k){return a.key().compare(k) < 0;});        
        if (it == members_.end())
        {
            members_.emplace_back(key_type(name.begin(),name.end()), 
                                        std::forward<Args>(args)...);
            it = members_.begin() + members_.size() - 1;
            inserted = true;
        }
        else if (it->key() == name)
        {
            inserted = false;
        }
        else
        {
            it = members_.emplace(it,
                                        key_type(name.begin(),name.end()),
                                        std::forward<Args>(args)...);
            inserted = true;
        }
        return std::make_pair(it,inserted);
    }

    template <class A=allocator_type, class... Args>
    typename std::enable_if<!is_stateless<A>::value,std::pair<iterator,bool>>::type
    try_emplace(const string_view_type& name, Args&&... args)
    {
        bool inserted;
        auto it = std::lower_bound(members_.begin(),members_.end(), name, 
                                   [](const key_value_type& a, const string_view_type& k){return a.key().compare(k) < 0;});        
        if (it == members_.end())
        {
            members_.emplace_back(key_type(name.begin(),name.end(), get_allocator()), 
                                        std::forward<Args>(args)...);
            it = members_.begin() + members_.size() - 1;
            inserted = true;
        }
        else if (it->key() == name)
        {
            inserted = false;
        }
        else
        {
            it = members_.emplace(it,
                                        key_type(name.begin(),name.end(), get_allocator()),
                                        std::forward<Args>(args)...);
            inserted = true;
        }
        return std::make_pair(it,inserted);
    }

    template <class A=allocator_type, class ... Args>
    typename std::enable_if<is_stateless<A>::value,iterator>::type 
    try_emplace(iterator hint, const string_view_type& name, Args&&... args)
    {
        iterator it = hint;

        if (hint != members_.end() && hint->key() <= name)
        {
            it = std::lower_bound(hint,members_.end(), name, 
                                  [](const key_value_type& a, const string_view_type& k){return a.key().compare(k) < 0;});        
        }
        else
        {
            it = std::lower_bound(members_.begin(),members_.end(), name, 
                                  [](const key_value_type& a, const string_view_type& k){return a.key().compare(k) < 0;});        
        }

        if (it == members_.end())
        {
            members_.emplace_back(key_type(name.begin(),name.end()), 
                                        std::forward<Args>(args)...);
            it = members_.begin() + (members_.size() - 1);
        }
        else if (it->key() == name)
        {
        }
        else
        {
            it = members_.emplace(it,
                                        key_type(name.begin(),name.end()),
                                        std::forward<Args>(args)...);
        }

        return it;
    }

    template <class A=allocator_type, class ... Args>
    typename std::enable_if<!is_stateless<A>::value,iterator>::type 
    try_emplace(iterator hint, const string_view_type& name, Args&&... args)
    {
        iterator it = hint;
        if (hint != members_.end() && hint->key() <= name)
        {
            it = std::lower_bound(hint,members_.end(), name, 
                                  [](const key_value_type& a, const string_view_type& k){return a.key().compare(k) < 0;});        
        }
        else
        {
            it = std::lower_bound(members_.begin(),members_.end(), name, 
                                  [](const key_value_type& a, const string_view_type& k){return a.key().compare(k) < 0;});        
        }

        if (it == members_.end())
        {
            members_.emplace_back(key_type(name.begin(),name.end(), get_allocator()), 
                                        std::forward<Args>(args)...);
            it = members_.begin() + (members_.size() - 1);
        }
        else if (it->key() == name)
        {
        }
        else
        {
            it = members_.emplace(it,
                                        key_type(name.begin(),name.end(), get_allocator()),
                                        std::forward<Args>(args)...);
        }
        return it;
    }

    // insert_or_assign

    template <class T, class A=allocator_type>
    typename std::enable_if<is_stateless<A>::value,iterator>::type 
    insert_or_assign(iterator hint, const string_view_type& name, T&& value)
    {
        iterator it;
        if (hint != members_.end() && hint->key() <= name)
        {
            it = std::lower_bound(hint,members_.end(), name, 
                                  [](const key_value_type& a, const string_view_type& k){return a.key().compare(k) < 0;});        
        }
        else
        {
            it = std::lower_bound(members_.begin(),members_.end(), name, 
                                  [](const key_value_type& a, const string_view_type& k){return a.key().compare(k) < 0;});        
        }

        if (it == members_.end())
        {
            members_.emplace_back(key_type(name.begin(),name.end()), 
                                        std::forward<T>(value));
            it = members_.begin() + (members_.size() - 1);
        }
        else if (it->key() == name)
        {
            it->value(Json(std::forward<T>(value)));
        }
        else
        {
            it = members_.emplace(it,
                                        key_type(name.begin(),name.end()),
                                        std::forward<T>(value));
        }
        return it;
    }

    template <class T, class A=allocator_type>
    typename std::enable_if<!is_stateless<A>::value,iterator>::type 
    insert_or_assign(iterator hint, const string_view_type& name, T&& value)
    {
        iterator it;
        if (hint != members_.end() && hint->key() <= name)
        {
            it = std::lower_bound(hint,members_.end(), name, 
                                  [](const key_value_type& a, const string_view_type& k){return a.key().compare(k) < 0;});        
        }
        else
        {
            it = std::lower_bound(members_.begin(),members_.end(), name, 
                                  [](const key_value_type& a, const string_view_type& k){return a.key().compare(k) < 0;});        
        }

        if (it == members_.end())
        {
            members_.emplace_back(key_type(name.begin(),name.end(), get_allocator()), 
                                        std::forward<T>(value),get_allocator());
            it = members_.begin() + (members_.size() - 1);
        }
        else if (it->key() == name)
        {
            it->value(Json(std::forward<T>(value),get_allocator()));
        }
        else
        {
            it = members_.emplace(it,
                                        key_type(name.begin(),name.end(), get_allocator()),
                                        std::forward<T>(value),get_allocator());
        }
        return it;
    }

    // merge

    void merge(const json_object& source)
    {
        for (auto it = source.begin(); it != source.end(); ++it)
        {
            try_emplace(it->key(),it->value());
        }
    }

    void merge(json_object&& source)
    {
        auto it = std::make_move_iterator(source.begin());
        auto end = std::make_move_iterator(source.end());
        for (; it != end; ++it)
        {
            auto pos = std::lower_bound(members_.begin(),members_.end(), it->key(), 
                                        [](const key_value_type& a, const string_view_type& k){return a.key().compare(k) < 0;});   
            if (pos == members_.end() )
            {
                members_.emplace_back(*it);
            }
            else if (it->key() != pos->key())
            {
                members_.emplace(pos,*it);
            }
        }
    }

    void merge(iterator hint, const json_object& source)
    {
        for (auto it = source.begin(); it != source.end(); ++it)
        {
            hint = try_emplace(hint, it->key(),it->value());
        }
    }

    void merge(iterator hint, json_object&& source)
    {
        auto it = std::make_move_iterator(source.begin());
        auto end = std::make_move_iterator(source.end());
        for (; it != end; ++it)
        {
            iterator pos;
            if (hint != members_.end() && hint->key() <= it->key())
            {
                pos = std::lower_bound(hint,members_.end(), it->key(), 
                                      [](const key_value_type& a, const string_view_type& k){return a.key().compare(k) < 0;});        
            }
            else
            {
                pos = std::lower_bound(members_.begin(),members_.end(), it->key(), 
                                      [](const key_value_type& a, const string_view_type& k){return a.key().compare(k) < 0;});        
            }
            if (pos == members_.end() )
            {
                members_.emplace_back(*it);
                hint = members_.begin() + (members_.size() - 1);
            }
            else if (it->key() != pos->key())
            {
                hint = members_.emplace(pos,*it);
            }
        }
    }

    // merge_or_update

    void merge_or_update(const json_object& source)
    {
        for (auto it = source.begin(); it != source.end(); ++it)
        {
            insert_or_assign(it->key(),it->value());
        }
    }

    void merge_or_update(json_object&& source)
    {
        auto it = std::make_move_iterator(source.begin());
        auto end = std::make_move_iterator(source.end());
        for (; it != end; ++it)
        {
            auto pos = std::lower_bound(members_.begin(),members_.end(), it->key(), 
                                        [](const key_value_type& a, const string_view_type& k){return a.key().compare(k) < 0;});   
            if (pos == members_.end() )
            {
                members_.emplace_back(*it);
            }
            else 
            {
                pos->value(it->value());
            }
        }
    }

    void merge_or_update(iterator hint, const json_object& source)
    {
        for (auto it = source.begin(); it != source.end(); ++it)
        {
            hint = insert_or_assign(hint, it->key(),it->value());
        }
    }

    void merge_or_update(iterator hint, json_object&& source)
    {
        auto it = std::make_move_iterator(source.begin());
        auto end = std::make_move_iterator(source.end());
        for (; it != end; ++it)
        {
            iterator pos;
            if (hint != members_.end() && hint->key() <= it->key())
            {
                pos = std::lower_bound(hint,members_.end(), it->key(), 
                                      [](const key_value_type& a, const string_view_type& k){return a.key().compare(k) < 0;});        
            }
            else
            {
                pos = std::lower_bound(members_.begin(),members_.end(), it->key(), 
                                      [](const key_value_type& a, const string_view_type& k){return a.key().compare(k) < 0;});        
            }
            if (pos == members_.end() )
            {
                members_.emplace_back(*it);
                hint = members_.begin() + (members_.size() - 1);
            }
            else 
            {
                pos->value(it->value());
                hint = pos;
            }
        }
    }

    bool operator==(const json_object& rhs) const
    {
        return members_ == rhs.members_;
    }

    bool operator<(const json_object& rhs) const
    {
        return members_ < rhs.members_;
    }
private:
    json_object& operator=(const json_object&) = delete;
};

// Preserve order
template <class KeyT,class Json>
class json_object<KeyT,Json,typename std::enable_if<Json::implementation_policy::preserve_order>::type> :
    public container_base<typename Json::allocator_type>
{
public:
    typedef typename Json::allocator_type allocator_type;
    typedef typename Json::char_type char_type;
    typedef KeyT key_type;
    typedef typename Json::string_view_type string_view_type;
    typedef key_value<KeyT,Json> key_value_type;
private:
    typedef typename Json::implementation_policy implementation_policy;
    typedef typename std::allocator_traits<allocator_type>:: template rebind_alloc<key_value_type> key_value_allocator_type;
    using key_value_container_type = typename implementation_policy::template sequence_container_type<key_value_type,key_value_allocator_type>;
    typedef typename std::allocator_traits<allocator_type>:: template rebind_alloc<size_t> index_allocator_type;
    using index_container_type = typename implementation_policy::template sequence_container_type<size_t,index_allocator_type>;

    key_value_container_type members_;
    index_container_type index_;
public:
    typedef typename key_value_container_type::iterator iterator;
    typedef typename key_value_container_type::const_iterator const_iterator;

    using container_base<allocator_type>::get_allocator;

    json_object()
    {
    }
    json_object(const allocator_type& allocator)
        : container_base<allocator_type>(allocator), 
          members_(key_value_allocator_type(allocator)), 
          index_(index_allocator_type(allocator))
    {
    }

    json_object(const json_object& val)
        : container_base<allocator_type>(val.get_allocator()), 
          members_(val.members_),
          index_(val.index_)
    {
    }

    json_object(json_object&& val)
        : container_base<allocator_type>(val.get_allocator()), 
          members_(std::move(val.members_)),
          index_(std::move(val.index_))
    {
    }

    json_object(const json_object& val, const allocator_type& allocator) 
        : container_base<allocator_type>(allocator), 
          members_(val.members_,key_value_allocator_type(allocator)),
          index_(val.index_,index_allocator_type(allocator))
    {
    }

    json_object(json_object&& val,const allocator_type& allocator) 
        : container_base<allocator_type>(allocator), 
          members_(std::move(val.members_),key_value_allocator_type(allocator)),
          index_(std::move(val.index_),index_allocator_type(allocator))
    {
    }

    template<class InputIt>
    json_object(InputIt first, InputIt last)
    {
        size_t count = std::distance(first,last);
        members_.reserve(count);
        for (auto s = first; s != last; ++s)
        {
            members_.emplace_back(get_key_value<KeyT,Json>()(*s));
        }

        build_index();
        auto last_unique = std::unique(index_.begin(), index_.end(),
            [&](size_t a, size_t b) { return !(members_.at(a).key().compare(members_.at(b).key())); });

        if (last_unique != index_.end())
        {
            index_.erase(last_unique, index_.end());
            std::sort(index_.begin(), index_.end());

            auto result = index_.rbegin();
            if (*result != members_.size())
            {
                members_.erase(members_.begin() + (*result + 1), members_.end());
            }
            for (auto it = index_.rbegin() + 1; it != index_.rend(); ++it, ++result)
            {
                if (*result - *it > 1)
                {
                    members_.erase(members_.begin() + (*it + 1), members_.begin() + *result);
                }
            }
        }
        build_index();
    }

    template<class InputIt>
    json_object(InputIt first, InputIt last, 
                const allocator_type& allocator)
        : container_base<allocator_type>(allocator), 
          members_(key_value_allocator_type(allocator)), 
          index_(index_allocator_type(allocator))
    {
        size_t count = std::distance(first,last);
        members_.reserve(count);
        for (auto s = first; s != last; ++s)
        {
            members_.emplace_back(get_key_value<KeyT,Json>()(*s));
        }

        build_index();
        auto last_unique = std::unique(index_.begin(), index_.end(),
            [&](size_t a, size_t b) { return !(members_.at(a).key().compare(members_.at(b).key())); });

        if (last_unique != index_.end())
        {
            index_.erase(last_unique, index_.end());
            std::sort(index_.begin(), index_.end());

            auto result = index_.rbegin();
            if (*result != members_.size())
            {
                members_.erase(members_.begin() + (*result + 1), members_.end());
            }
            for (auto it = index_.rbegin() + 1; it != index_.rend(); ++it, ++result)
            {
                if (*result - *it > 1)
                {
                    members_.erase(members_.begin() + (*it + 1), members_.begin() + *result);
                }
            }
        }
        build_index();
    }

    json_object(std::initializer_list<typename Json::array> init)
    {
        for (const auto& element : init)
        {
            if (element.size() != 2 || !element[0].is_string())
            {
                JSONCONS_THROW(json_runtime_error<std::runtime_error>("Cannot create object from initializer list"));
                break;
            }
        }
        for (auto& element : init)
        {
            insert_or_assign(element[0].as_string_view(), std::move(element[1]));
        }
    }

    json_object(std::initializer_list<typename Json::array> init, 
                const allocator_type& allocator)
        : container_base<allocator_type>(allocator), 
          members_(key_value_allocator_type(allocator)), 
          index_(index_allocator_type(allocator))
    {
        for (const auto& element : init)
        {
            if (element.size() != 2 || !element[0].is_string())
            {
                JSONCONS_THROW(json_runtime_error<std::runtime_error>("Cannot create object from initializer list"));
                break;
            }
        }
        for (auto& element : init)
        {
            insert_or_assign(element[0].as_string_view(), std::move(element[1]));
        }
    }

    void swap(json_object& val)
    {
        members_.swap(val.members_);
    }

    iterator begin()
    {
        return members_.begin();
    }

    iterator end()
    {
        return members_.end();
    }

    const_iterator begin() const
    {
        return members_.begin();
    }

    const_iterator end() const
    {
        return members_.end();
    }

    size_t size() const {return members_.size();}

    size_t capacity() const {return members_.capacity();}

    void clear() 
    {
        members_.clear();
        index_.clear();
    }

    void shrink_to_fit() 
    {
        for (size_t i = 0; i < members_.size(); ++i)
        {
            members_[i].shrink_to_fit();
        }
        members_.shrink_to_fit();
        index_.shrink_to_fit();
    }

    void reserve(size_t n) {members_.reserve(n);}

    Json& at(size_t i) 
    {
        if (i >= members_.size())
        {
            JSONCONS_THROW(json_runtime_error<std::out_of_range>("Invalid array subscript"));
        }
        return members_[i].value();
    }

    const Json& at(size_t i) const 
    {
        if (i >= members_.size())
        {
            JSONCONS_THROW(json_runtime_error<std::out_of_range>("Invalid array subscript"));
        }
        return members_[i].value();
    }

    iterator find(const string_view_type& name)
    {
        auto it = std::lower_bound(index_.begin(),index_.end(), name, 
                                    [&](size_t i, const string_view_type& k){return members_.at(i).key().compare(k) < 0;});        
        if (it != index_.end() && members_.at(*it).key() == name)
        {
            return members_.begin() + *it;
        }
        else
        {
            return members_.end();
        }
    }

    const_iterator find(const string_view_type& name) const
    {
        auto it = std::lower_bound(index_.begin(),index_.end(), name, 
                                    [&](size_t i, const string_view_type& k){return members_.at(i).key().compare(k) < 0;});        
        if (it != index_.end() && members_.at(*it).key() == name)
        {
            return members_.begin() + *it;
        }
        else
        {
            return members_.end();
        }
    }

    void erase(const_iterator first, const_iterator last) 
    {
        size_t pos1 = first == members_.end() ? members_.size() : first - members_.begin();
        size_t pos2 = last == members_.end() ? members_.size() : last - members_.begin();

        if (pos1 < members_.size() && pos2 <= members_.size())
        {
            erase_index_entries(pos1,pos2);

#if defined(JSONCONS_NO_ERASE_TAKING_CONST_ITERATOR)
            iterator it1 = members_.begin() + (first - members_.begin());
            iterator it2 = members_.begin() + (last - members_.begin());
            members_.erase(it1,it2);
#else
            members_.erase(first,last);
#endif
        }
    }

    void erase(const string_view_type& name) 
    {
        auto pos = find(name);
        if (pos != members_.end())
        {
            erase_index_entry(name);
#if defined(JSONCONS_NO_ERASE_TAKING_CONST_ITERATOR)
            iterator it = members_.begin() + (pos - members_.begin());
            members_.erase(it);
#else
            members_.erase(pos);
#endif
        }
    }

    template<class InputIt, class Convert>
    void insert(InputIt first, InputIt last, Convert convert)
    {
        size_t count = std::distance(first,last);
        members_.reserve(members_.size() + count);
        for (auto s = first; s != last; ++s)
        {
            members_.emplace_back(convert(*s));
        }

        build_index();
        auto last_unique = std::unique(index_.begin(), index_.end(),
            [&](size_t a, size_t b) { return !(members_.at(a).key().compare(members_.at(b).key())); });

        if (last_unique != index_.end())
        {
            index_.erase(last_unique, index_.end());
            std::sort(index_.begin(), index_.end());

            auto result = index_.rbegin();
            if (*result != members_.size())
            {
                members_.erase(members_.begin() + (*result + 1), members_.end());
            }
            for (auto it = index_.rbegin() + 1; it != index_.rend(); ++it, ++result)
            {
                if (*result - *it > 1)
                {
                    members_.erase(members_.begin() + (*it + 1), members_.begin() + *result);
                }
            }
        }
        build_index();
    }

    template<class InputIt, class Convert>
    void insert(sorted_unique_range_tag, InputIt first, InputIt last, Convert convert)
    {
        size_t count = std::distance(first,last);

        members_.reserve(members_.size() + count);
        for (auto s = first; s != last; ++s)
        {
            members_.emplace_back(convert(*s));
        }

        build_index();
    }

    template <class T, class A=allocator_type>
    typename std::enable_if<is_stateless<A>::value,std::pair<iterator,bool>>::type
    insert_or_assign(const string_view_type& name, T&& value)
    {
        auto result = insert_index_entry(name,members_.size());
        if (result.second)
        {
            members_.emplace_back(key_type(name.begin(), name.end()), std::forward<T>(value));
            auto it = members_.begin() + result.first;
            return std::make_pair(it,true);
        }
        else
        {
            auto it = members_.begin() + result.first;
            it->value(Json(std::forward<T>(value)));
            return std::make_pair(it,false);
        }
    }

    template <class T, class A=allocator_type>
    typename std::enable_if<!is_stateless<A>::value,std::pair<iterator,bool>>::type
    insert_or_assign(const string_view_type& name, T&& value)
    {
        auto result = insert_index_entry(name,members_.size());
        if (result.second)
        {
            members_.emplace_back(key_type(name.begin(),name.end(),get_allocator()), 
                                  std::forward<T>(value),get_allocator());
            auto it = members_.begin() + result.first;
            return std::make_pair(it,true);
        }
        else
        {
            auto it = members_.begin() + result.first;
            it->value(Json(std::forward<T>(value),get_allocator()));
            return std::make_pair(it,false);
        }
    }

    template <class A=allocator_type, class T>
    typename std::enable_if<is_stateless<A>::value,iterator>::type 
    insert_or_assign(iterator hint, const string_view_type& key, T&& value)
    {
        if (hint == members_.end())
        {
            auto result = insert_or_assign(key, std::forward<T>(value));
            return result.first;
        }
        else
        {
            size_t pos = hint - members_.begin();
            auto result = insert_index_entry(key,pos);

            if (result.second)
            {
                auto it = members_.emplace(hint, key_type(key.begin(), key.end()), std::forward<T>(value));
                return it;
            }
            else
            {
                auto it = members_.begin() + result.first;
                it->value(Json(std::forward<T>(value)));
                return it;
            }
        }
    }

    template <class A=allocator_type, class T>
    typename std::enable_if<!is_stateless<A>::value,iterator>::type 
    insert_or_assign(iterator hint, const string_view_type& key, T&& value)
    {
        if (hint == members_.end())
        {
            auto result = insert_or_assign(key, std::forward<T>(value));
            return result.first;
        }
        else
        {
            size_t pos = hint - members_.begin();
            auto result = insert_index_entry(key,pos);

            if (result.second)
            {
                auto it = members_.emplace(hint, 
                                           key_type(key.begin(),key.end(),get_allocator()), 
                                           std::forward<T>(value),get_allocator());
                return it;
            }
            else
            {
                auto it = members_.begin() + result.first;
                it->value(Json(std::forward<T>(value),get_allocator()));
                return it;
            }
        }
    }

    // merge

    void merge(const json_object& source)
    {
        for (auto it = source.begin(); it != source.end(); ++it)
        {
            try_emplace(it->key(),it->value());
        }
    }

    void merge(json_object&& source)
    {
        auto it = std::make_move_iterator(source.begin());
        auto end = std::make_move_iterator(source.end());
        for (; it != end; ++it)
        {
            auto pos = find(it->key());
            if (pos == members_.end() )
            {
                try_emplace(it->key(),std::move(it->value()));
            }
        }
    }

    void merge(iterator hint, const json_object& source)
    {
        size_t pos = hint - members_.begin();
        for (auto it = source.begin(); it != source.end(); ++it)
        {
            hint = try_emplace(hint, it->key(),it->value());
            size_t newpos = hint - members_.begin();
            if (newpos == pos)
            {
                ++hint;
                pos = hint - members_.begin();
            }
            else
            {
                hint = members_.begin() + pos;
            }
        }
    }

    void merge(iterator hint, json_object&& source)
    {
        size_t pos = hint - members_.begin();

        auto it = std::make_move_iterator(source.begin());
        auto end = std::make_move_iterator(source.end());
        for (; it != end; ++it)
        {
            hint = try_emplace(hint, it->key(), std::move(it->value()));
            size_t newpos = hint - members_.begin();
            if (newpos == pos)
            {
                ++hint;
                pos = hint - members_.begin();
            }
            else
            {
                hint = members_.begin() + pos;
            }
        }
    }

    // merge_or_update

    void merge_or_update(const json_object& source)
    {
        for (auto it = source.begin(); it != source.end(); ++it)
        {
            insert_or_assign(it->key(),it->value());
        }
    }

    void merge_or_update(json_object&& source)
    {
        auto it = std::make_move_iterator(source.begin());
        auto end = std::make_move_iterator(source.end());
        for (; it != end; ++it)
        {
            auto pos = find(it->key());
            if (pos == members_.end() )
            {
                insert_or_assign(it->key(),std::move(it->value()));
            }
            else
            {
                pos->value(std::move(it->value()));
            }
        }
    }

    void merge_or_update(iterator hint, const json_object& source)
    {
        size_t pos = hint - members_.begin();
        for (auto it = source.begin(); it != source.end(); ++it)
        {
            hint = insert_or_assign(hint, it->key(),it->value());
            size_t newpos = hint - members_.begin();
            if (newpos == pos)
            {
                ++hint;
                pos = hint - members_.begin();
            }
            else
            {
                hint = members_.begin() + pos;
            }
        }
    }

    void merge_or_update(iterator hint, json_object&& source)
    {
/*
        auto it = std::make_move_iterator(source.begin());
        auto end = std::make_move_iterator(source.end());
        for (; it != end; ++it)
        {
            auto pos = find(it->key());
            if (pos == members_.end() )
            {
                hint = try_emplace(hint,it->key(),std::move(it->value()));
            }
            else
            {
                pos->value(std::move(it->value()));
                hint = pos;
            }
        }
*/
        size_t pos = hint - members_.begin();
        auto it = std::make_move_iterator(source.begin());
        auto end = std::make_move_iterator(source.end());
        for (; it != end; ++it)
        {
            hint = insert_or_assign(hint, it->key(),std::move(it->value()));
            size_t newpos = hint - members_.begin();
            if (newpos == pos)
            {
                ++hint;
                pos = hint - members_.begin();
            }
            else
            {
                hint = members_.begin() + pos;
            }
        }
    }

    // try_emplace

    template <class A=allocator_type, class... Args>
    typename std::enable_if<is_stateless<A>::value,std::pair<iterator,bool>>::type
    try_emplace(const string_view_type& name, Args&&... args)
    {
        auto result = insert_index_entry(name,members_.size());
        if (result.second)
        {
            members_.emplace_back(key_type(name.begin(), name.end()), std::forward<Args>(args)...);
            auto it = members_.begin() + result.first;
            return std::make_pair(it,true);
        }
        else
        {
            auto it = members_.begin() + result.first;
            return std::make_pair(it,false);
        }
    }

    template <class A=allocator_type, class... Args>
    typename std::enable_if<!is_stateless<A>::value,std::pair<iterator,bool>>::type
    try_emplace(const string_view_type& key, Args&&... args)
    {
        auto result = insert_index_entry(key,members_.size());
        if (result.second)
        {
            members_.emplace_back(key_type(key.begin(),key.end(), get_allocator()), 
                                  std::forward<Args>(args)...);
            auto it = members_.begin() + result.first;
            return std::make_pair(it,true);
        }
        else
        {
            auto it = members_.begin() + result.first;
            return std::make_pair(it,false);
        }
    }
 
    template <class A=allocator_type, class ... Args>
    typename std::enable_if<is_stateless<A>::value,iterator>::type
    try_emplace(iterator hint, const string_view_type& key, Args&&... args)
    {
        if (hint == members_.end())
        {
            auto result = try_emplace(key, std::forward<Args>(args)...);
            return result.first;
        }
        else
        {
            size_t pos = hint - members_.begin();
            auto result = insert_index_entry(key, pos);

            if (result.second)
            {
                auto it = members_.emplace(hint, key_type(key.begin(), key.end()), std::forward<Args>(args)...);
                return it;
            }
            else
            {
                auto it = members_.begin() + result.first;
                return it;
            }
        }
    }

    template <class A=allocator_type, class ... Args>
    typename std::enable_if<!is_stateless<A>::value,iterator>::type
    try_emplace(iterator hint, const string_view_type& key, Args&&... args)
    {
        if (hint == members_.end())
        {
            auto result = try_emplace(key, std::forward<Args>(args)...);
            return result.first;
        }
        else
        {
            size_t pos = hint - members_.begin();
            auto result = insert_index_entry(key, pos);

            if (result.second)
            {
                auto it = members_.emplace(hint, 
                                           key_type(key.begin(),key.end(), get_allocator()), 
                                           std::forward<Args>(args)...);
                return it;
            }
            else
            {
                auto it = members_.begin() + result.first;
                return it;
            }
        }
    }

    bool operator==(const json_object& rhs) const
    {
        return members_ == rhs.members_;
    }

    bool operator<(const json_object& rhs) const
    {
        return members_ < rhs.members_;
    }
private:

    std::pair<size_t,bool> insert_index_entry(const string_view_type& key, size_t pos)
    {
        JSONCONS_ASSERT(pos <= index_.size());

        auto it = std::lower_bound(index_.begin(),index_.end(), key, 
                                    [&](size_t i, const string_view_type& k){return members_.at(i).key().compare(k) < 0;});        

        if (it == index_.end())
        {
            size_t count = index_.size() - pos;
            for (size_t i = 0; count > 0 && i < index_.size(); ++i)
            {
                if (index_[i] >= pos)
                {
                    ++index_[i];
                    --count;
                }
            }
            index_.push_back(pos);
            return std::make_pair(pos,true);
        }
        else if (members_.at(*it).key() != key)
        {
            size_t count = index_.size() - pos;
            for (size_t i = 0; count > 0 && i < index_.size(); ++i)
            {
                if (index_[i] >= pos)
                {
                    ++index_[i];
                    --count;
                }
            }
            auto it2 = index_.insert(it, pos);
            return std::make_pair(*it2,true);
        }
        else
        {
            return std::make_pair(*it,false);
        }
    }

    void erase_index_entry(const string_view_type& key)
    {
        auto it = std::lower_bound(index_.begin(),index_.end(), key, 
                                   [&](size_t i, const string_view_type& k){return members_.at(i).key().compare(k) < 0;});        

        if (it != index_.end() && members_.at(*it).key() != key)
        {
            size_t pos = *it;
            size_t count = index_.size() - pos;
            for (size_t i = 0; i < index_.size() && count > 0; ++i)
            {
                if (index_[i] > pos)
                {
                    --index_[i];
                    --count;
                }
            }
            index_.erase(it);
        }
    }

    void erase_index_entries(size_t pos1, size_t pos2)
    {
        for (size_t i = 0; i < index_.size(); ++i)
        {
            if (index_[i] >= pos1 && index_[i] < pos2)
            {
                index_.erase(index_.begin()+index_[i]);
            }
            else if (index_[i] > pos2)
            {
                --index_[i];
            }
        }
    }

    void build_index()
    {
        index_.clear();
        index_.reserve(members_.size());
        for (size_t i = 0; i < members_.size(); ++i)
        {
            index_.push_back(i);
        }
        std::stable_sort(index_.begin(),index_.end(),
                         [&](size_t a, size_t b){return members_.at(a).key().compare(members_.at(b).key()) < 0;});
    }

    json_object& operator=(const json_object&) = delete;
};

}

#endif
