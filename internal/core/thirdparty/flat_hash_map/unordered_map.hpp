//          Copyright Malte Skarupke 2017.
// Distributed under the Boost Software License, Version 1.0.
//    (See http://www.boost.org/LICENSE_1_0.txt)

#pragma once

#include <cstdint>
#include <cstddef>
#include <cmath>
#include <array>
#include <algorithm>
#include <iterator>
#include <utility>
#include <type_traits>
#include "flat_hash_map.hpp"

namespace ska
{

namespace detailv10
{
template<typename T, typename Allocator>
struct sherwood_v10_entry
{
    sherwood_v10_entry()
    {
    }
    ~sherwood_v10_entry()
    {
    }

    using EntryPointer = typename std::allocator_traits<typename std::allocator_traits<Allocator>::template rebind_alloc<sherwood_v10_entry>>::pointer;

    EntryPointer next = nullptr;
    union
    {
        T value;
    };

    static EntryPointer * empty_pointer()
    {
        static EntryPointer result[3] = { EntryPointer(nullptr) + ptrdiff_t(1), nullptr, nullptr };
        return result + 1;
    }
};

using ska::detailv3::functor_storage;
using ska::detailv3::KeyOrValueHasher;
using ska::detailv3::KeyOrValueEquality;
using ska::detailv3::AssignIfTrue;
using ska::detailv3::HashPolicySelector;

template<typename T, typename FindKey, typename ArgumentHash, typename Hasher, typename ArgumentEqual, typename Equal, typename ArgumentAlloc, typename EntryAlloc, typename BucketAllocator>
class sherwood_v10_table : private EntryAlloc, private Hasher, private Equal, private BucketAllocator
{
    using Entry = detailv10::sherwood_v10_entry<T, ArgumentAlloc>;
    using AllocatorTraits = std::allocator_traits<EntryAlloc>;
    using BucketAllocatorTraits = std::allocator_traits<BucketAllocator>;
    using EntryPointer = typename AllocatorTraits::pointer;
    struct convertible_to_iterator;

public:

    using value_type = T;
    using size_type = size_t;
    using difference_type = std::ptrdiff_t;
    using hasher = ArgumentHash;
    using key_equal = ArgumentEqual;
    using allocator_type = EntryAlloc;
    using reference = value_type &;
    using const_reference = const value_type &;
    using pointer = value_type *;
    using const_pointer = const value_type *;

    sherwood_v10_table()
    {
    }
    explicit sherwood_v10_table(size_type bucket_count, const ArgumentHash & hash = ArgumentHash(), const ArgumentEqual & equal = ArgumentEqual(), const ArgumentAlloc & alloc = ArgumentAlloc())
        : EntryAlloc(alloc), Hasher(hash), Equal(equal), BucketAllocator(alloc)
    {
        rehash(bucket_count);
    }
    sherwood_v10_table(size_type bucket_count, const ArgumentAlloc & alloc)
        : sherwood_v10_table(bucket_count, ArgumentHash(), ArgumentEqual(), alloc)
    {
    }
    sherwood_v10_table(size_type bucket_count, const ArgumentHash & hash, const ArgumentAlloc & alloc)
        : sherwood_v10_table(bucket_count, hash, ArgumentEqual(), alloc)
    {
    }
    explicit sherwood_v10_table(const ArgumentAlloc & alloc)
        : EntryAlloc(alloc), BucketAllocator(alloc)
    {
    }
    template<typename It>
    sherwood_v10_table(It first, It last, size_type bucket_count = 0, const ArgumentHash & hash = ArgumentHash(), const ArgumentEqual & equal = ArgumentEqual(), const ArgumentAlloc & alloc = ArgumentAlloc())
        : sherwood_v10_table(bucket_count, hash, equal, alloc)
    {
        insert(first, last);
    }
    template<typename It>
    sherwood_v10_table(It first, It last, size_type bucket_count, const ArgumentAlloc & alloc)
        : sherwood_v10_table(first, last, bucket_count, ArgumentHash(), ArgumentEqual(), alloc)
    {
    }
    template<typename It>
    sherwood_v10_table(It first, It last, size_type bucket_count, const ArgumentHash & hash, const ArgumentAlloc & alloc)
        : sherwood_v10_table(first, last, bucket_count, hash, ArgumentEqual(), alloc)
    {
    }
    sherwood_v10_table(std::initializer_list<T> il, size_type bucket_count = 0, const ArgumentHash & hash = ArgumentHash(), const ArgumentEqual & equal = ArgumentEqual(), const ArgumentAlloc & alloc = ArgumentAlloc())
        : sherwood_v10_table(bucket_count, hash, equal, alloc)
    {
        if (bucket_count == 0)
            reserve(il.size());
        insert(il.begin(), il.end());
    }
    sherwood_v10_table(std::initializer_list<T> il, size_type bucket_count, const ArgumentAlloc & alloc)
        : sherwood_v10_table(il, bucket_count, ArgumentHash(), ArgumentEqual(), alloc)
    {
    }
    sherwood_v10_table(std::initializer_list<T> il, size_type bucket_count, const ArgumentHash & hash, const ArgumentAlloc & alloc)
        : sherwood_v10_table(il, bucket_count, hash, ArgumentEqual(), alloc)
    {
    }
    sherwood_v10_table(const sherwood_v10_table & other)
        : sherwood_v10_table(other, AllocatorTraits::select_on_container_copy_construction(other.get_allocator()))
    {
    }
    sherwood_v10_table(const sherwood_v10_table & other, const ArgumentAlloc & alloc)
        : EntryAlloc(alloc), Hasher(other), Equal(other), BucketAllocator(alloc), _max_load_factor(other._max_load_factor)
    {
        try
        {
            rehash_for_other_container(other);
            insert(other.begin(), other.end());
        }
        catch(...)
        {
            clear();
            deallocate_data();
            throw;
        }
    }
    sherwood_v10_table(sherwood_v10_table && other) noexcept
        : EntryAlloc(std::move(other)), Hasher(std::move(other)), Equal(std::move(other)), BucketAllocator(std::move(other)), _max_load_factor(other._max_load_factor)
    {
        swap_pointers(other);
    }
    sherwood_v10_table(sherwood_v10_table && other, const ArgumentAlloc & alloc) noexcept
        : EntryAlloc(alloc), Hasher(std::move(other)), Equal(std::move(other)), BucketAllocator(alloc), _max_load_factor(other._max_load_factor)
    {
        swap_pointers(other);
    }
    sherwood_v10_table & operator=(const sherwood_v10_table & other)
    {
        if (this == std::addressof(other))
            return *this;

        clear();
        static_assert(AllocatorTraits::propagate_on_container_copy_assignment::value == BucketAllocatorTraits::propagate_on_container_copy_assignment::value, "The allocators have to behave the same way");
        if (AllocatorTraits::propagate_on_container_copy_assignment::value)
        {
            if (static_cast<EntryAlloc &>(*this) != static_cast<const EntryAlloc &>(other) || static_cast<BucketAllocator &>(*this) != static_cast<const BucketAllocator &>(other))
            {
                reset_to_empty_state();
            }
            AssignIfTrue<EntryAlloc, AllocatorTraits::propagate_on_container_copy_assignment::value>()(*this, other);
            AssignIfTrue<BucketAllocator, BucketAllocatorTraits::propagate_on_container_copy_assignment::value>()(*this, other);
        }
        _max_load_factor = other._max_load_factor;
        static_cast<Hasher &>(*this) = other;
        static_cast<Equal &>(*this) = other;
        rehash_for_other_container(other);
        insert(other.begin(), other.end());
        return *this;
    }
    sherwood_v10_table & operator=(sherwood_v10_table && other) noexcept
    {
        static_assert(AllocatorTraits::propagate_on_container_move_assignment::value == BucketAllocatorTraits::propagate_on_container_move_assignment::value, "The allocators have to behave the same way");
        if (this == std::addressof(other))
            return *this;
        else if (AllocatorTraits::propagate_on_container_move_assignment::value)
        {
            clear();
            reset_to_empty_state();
            AssignIfTrue<EntryAlloc, AllocatorTraits::propagate_on_container_move_assignment::value>()(*this, std::move(other));
            AssignIfTrue<BucketAllocator, BucketAllocatorTraits::propagate_on_container_move_assignment::value>()(*this, std::move(other));
            swap_pointers(other);
        }
        else if (static_cast<EntryAlloc &>(*this) == static_cast<EntryAlloc &>(other) && static_cast<BucketAllocator &>(*this) == static_cast<BucketAllocator &>(other))
        {
            swap_pointers(other);
        }
        else
        {
            clear();
            _max_load_factor = other._max_load_factor;
            rehash_for_other_container(other);
            for (T & elem : other)
                emplace(std::move(elem));
            other.clear();
        }
        static_cast<Hasher &>(*this) = std::move(other);
        static_cast<Equal &>(*this) = std::move(other);
        return *this;
    }
    ~sherwood_v10_table()
    {
        clear();
        deallocate_data();
    }

    const allocator_type & get_allocator() const
    {
        return static_cast<const allocator_type &>(*this);
    }
    const ArgumentEqual & key_eq() const
    {
        return static_cast<const ArgumentEqual &>(*this);
    }
    const ArgumentHash & hash_function() const
    {
        return static_cast<const ArgumentHash &>(*this);
    }

    template<typename ValueType>
    struct templated_iterator
    {
        templated_iterator()
        {
        }
        templated_iterator(EntryPointer element, EntryPointer * bucket)
            : current_element(element), current_bucket(bucket)
        {
        }

        EntryPointer current_element = nullptr;
        EntryPointer * current_bucket = nullptr;

        using iterator_category = std::forward_iterator_tag;
        using value_type = ValueType;
        using difference_type = ptrdiff_t;
        using pointer = ValueType *;
        using reference = ValueType &;

        friend bool operator==(const templated_iterator & lhs, const templated_iterator & rhs)
        {
            return lhs.current_element == rhs.current_element;
        }
        friend bool operator!=(const templated_iterator & lhs, const templated_iterator & rhs)
        {
            return !(lhs == rhs);
        }

        templated_iterator & operator++()
        {
            if (!current_element->next)
            {
                do
                {
                    --current_bucket;
                }
                while (!*current_bucket);
                current_element = *current_bucket;
            }
            else
                current_element = current_element->next;
            return *this;
        }
        templated_iterator operator++(int)
        {
            templated_iterator copy(*this);
            ++*this;
            return copy;
        }

        ValueType & operator*() const
        {
            return current_element->value;
        }
        ValueType * operator->() const
        {
            return std::addressof(current_element->value);
        }

        operator templated_iterator<const value_type>() const
        {
            return { current_element, current_bucket };
        }
    };
    using iterator = templated_iterator<value_type>;
    using const_iterator = templated_iterator<const value_type>;

    iterator begin()
    {
        EntryPointer * end = entries - 1;
        for (EntryPointer * it = entries + num_slots_minus_one; it != end; --it)
        {
            if (*it)
                return { *it, it };
        }
        return { *end, end };
    }
    const_iterator begin() const
    {
        return const_cast<sherwood_v10_table *>(this)->begin();
    }
    const_iterator cbegin() const
    {
        return begin();
    }
    iterator end()
    {
        EntryPointer * end = entries - 1;
        return { *end, end };
    }
    const_iterator end() const
    {
        EntryPointer * end = entries - 1;
        return { *end, end };
    }
    const_iterator cend() const
    {
        return end();
    }

    iterator find(const FindKey & key)
    {
        size_t index = hash_policy.index_for_hash(hash_object(key), num_slots_minus_one);
        EntryPointer * bucket = entries + ptrdiff_t(index);
        for (EntryPointer it = *bucket; it; it = it->next)
        {
            if (compares_equal(key, it->value))
                return { it, bucket };
        }
        return end();
    }
    const_iterator find(const FindKey & key) const
    {
        return const_cast<sherwood_v10_table *>(this)->find(key);
    }
    size_t count(const FindKey & key) const
    {
        return find(key) == end() ? 0 : 1;
    }
    std::pair<iterator, iterator> equal_range(const FindKey & key)
    {
        iterator found = find(key);
        if (found == end())
            return { found, found };
        else
            return { found, std::next(found) };
    }
    std::pair<const_iterator, const_iterator> equal_range(const FindKey & key) const
    {
        const_iterator found = find(key);
        if (found == end())
            return { found, found };
        else
            return { found, std::next(found) };
    }

    template<typename Key, typename... Args>
    std::pair<iterator, bool> emplace(Key && key, Args &&... args)
    {
        size_t index = hash_policy.index_for_hash(hash_object(key), num_slots_minus_one);
        EntryPointer * bucket = entries + ptrdiff_t(index);
        for (EntryPointer it = *bucket; it; it = it->next)
        {
            if (compares_equal(key, it->value))
                return { { it, bucket }, false };
        }
        return emplace_new_key(bucket, std::forward<Key>(key), std::forward<Args>(args)...);
    }

    std::pair<iterator, bool> insert(const value_type & value)
    {
        return emplace(value);
    }
    std::pair<iterator, bool> insert(value_type && value)
    {
        return emplace(std::move(value));
    }
    template<typename... Args>
    iterator emplace_hint(const_iterator, Args &&... args)
    {
        return emplace(std::forward<Args>(args)...).first;
    }
    iterator insert(const_iterator, const value_type & value)
    {
        return emplace(value).first;
    }
    iterator insert(const_iterator, value_type && value)
    {
        return emplace(std::move(value)).first;
    }

    template<typename It>
    void insert(It begin, It end)
    {
        for (; begin != end; ++begin)
        {
            emplace(*begin);
        }
    }
    void insert(std::initializer_list<value_type> il)
    {
        insert(il.begin(), il.end());
    }

    void rehash(size_t num_buckets)
    {
        num_buckets = std::max(num_buckets, static_cast<size_t>(std::ceil(num_elements / static_cast<double>(_max_load_factor))));
        if (num_buckets == 0)
        {
            reset_to_empty_state();
            return;
        }
        auto new_prime_index = hash_policy.next_size_over(num_buckets);
        if (num_buckets == bucket_count())
            return;
        EntryPointer * new_buckets(&*BucketAllocatorTraits::allocate(*this, num_buckets + 1));
        EntryPointer * end_it = new_buckets + static_cast<ptrdiff_t>(num_buckets + 1);
        *new_buckets = EntryPointer(nullptr) + ptrdiff_t(1);
        ++new_buckets;
        std::fill(new_buckets, end_it, nullptr);
        std::swap(entries, new_buckets);
        std::swap(num_slots_minus_one, num_buckets);
        --num_slots_minus_one;
        hash_policy.commit(new_prime_index);
        if (!num_buckets)
            return;

        for (EntryPointer * it = new_buckets, * end = it + static_cast<ptrdiff_t>(num_buckets + 1); it != end; ++it)
        {
            for (EntryPointer e = *it; e;)
            {
                EntryPointer next = e->next;
                size_t index = hash_policy.index_for_hash(hash_object(e->value), num_slots_minus_one);
                EntryPointer & new_slot = entries[index];
                e->next = new_slot;
                new_slot = e;
                e = next;
            }
        }
        BucketAllocatorTraits::deallocate(*this, new_buckets - 1, num_buckets + 2);
    }

    void reserve(size_t num_elements)
    {
        if (!num_elements)
            return;
        num_elements = static_cast<size_t>(std::ceil(num_elements / static_cast<double>(_max_load_factor)));
        if (num_elements > bucket_count())
            rehash(num_elements);
    }

    // the return value is a type that can be converted to an iterator
    // the reason for doing this is that it's not free to find the
    // iterator pointing at the next element. if you care about the
    // next iterator, turn the return value into an iterator
    convertible_to_iterator erase(const_iterator to_erase)
    {
        --num_elements;
        AllocatorTraits::destroy(*this, std::addressof(to_erase.current_element->value));
        EntryPointer * previous = to_erase.current_bucket;
        while (*previous != to_erase.current_element)
        {
            previous = &(*previous)->next;
        }
        *previous = to_erase.current_element->next;
        AllocatorTraits::deallocate(*this, to_erase.current_element, 1);
        return { *previous, to_erase.current_bucket };
    }

    convertible_to_iterator erase(const_iterator begin_it, const_iterator end_it)
    {
        while (begin_it.current_bucket != end_it.current_bucket)
        {
            begin_it = erase(begin_it);
        }
        EntryPointer * bucket = begin_it.current_bucket;
        EntryPointer * previous = bucket;
        while (*previous != begin_it.current_element)
            previous = &(*previous)->next;
        while (*previous != end_it.current_element)
        {
            --num_elements;
            EntryPointer entry = *previous;
            AllocatorTraits::destroy(*this, std::addressof(entry->value));
            *previous = entry->next;
            AllocatorTraits::deallocate(*this, entry, 1);
        }
        return { *previous, bucket };
    }

    size_t erase(const FindKey & key)
    {
        auto found = find(key);
        if (found == end())
            return 0;
        else
        {
            erase(found);
            return 1;
        }
    }

    void clear()
    {
        if (!num_slots_minus_one)
            return;
        for (EntryPointer * it = entries, * end = it + static_cast<ptrdiff_t>(num_slots_minus_one + 1); it != end; ++it)
        {
            for (EntryPointer e = *it; e;)
            {
                EntryPointer next = e->next;
                AllocatorTraits::destroy(*this, std::addressof(e->value));
                AllocatorTraits::deallocate(*this, e, 1);
                e = next;
            }
            *it = nullptr;
        }
        num_elements = 0;
    }

    void swap(sherwood_v10_table & other)
    {
        using std::swap;
        swap_pointers(other);
        swap(static_cast<ArgumentHash &>(*this), static_cast<ArgumentHash &>(other));
        swap(static_cast<ArgumentEqual &>(*this), static_cast<ArgumentEqual &>(other));
        if (AllocatorTraits::propagate_on_container_swap::value)
            swap(static_cast<EntryAlloc &>(*this), static_cast<EntryAlloc &>(other));
        if (BucketAllocatorTraits::propagate_on_container_swap::value)
            swap(static_cast<BucketAllocator &>(*this), static_cast<BucketAllocator &>(other));
    }

    size_t size() const
    {
        return num_elements;
    }
    size_t max_size() const
    {
        return (AllocatorTraits::max_size(*this)) / sizeof(Entry);
    }
    size_t bucket_count() const
    {
        return num_slots_minus_one + 1;
    }
    size_type max_bucket_count() const
    {
        return (AllocatorTraits::max_size(*this) - 1) / sizeof(Entry);
    }
    size_t bucket(const FindKey & key) const
    {
        return hash_policy.template index_for_hash<0>(hash_object(key), num_slots_minus_one);
    }
    float load_factor() const
    {
        size_t buckets = bucket_count();
        if (buckets)
            return static_cast<float>(num_elements) / bucket_count();
        else
            return 0;
    }
    void max_load_factor(float value)
    {
        _max_load_factor = value;
    }
    float max_load_factor() const
    {
        return _max_load_factor;
    }

    bool empty() const
    {
        return num_elements == 0;
    }

private:
    EntryPointer * entries = Entry::empty_pointer();
    size_t num_slots_minus_one = 0;
    typename HashPolicySelector<ArgumentHash>::type hash_policy;
    float _max_load_factor = 1.0f;
    size_t num_elements = 0;

    void rehash_for_other_container(const sherwood_v10_table & other)
    {
        reserve(other.size());
    }

    void swap_pointers(sherwood_v10_table & other)
    {
        using std::swap;
        swap(hash_policy, other.hash_policy);
        swap(entries, other.entries);
        swap(num_slots_minus_one, other.num_slots_minus_one);
        swap(num_elements, other.num_elements);
        swap(_max_load_factor, other._max_load_factor);
    }

    template<typename... Args>
    SKA_NOINLINE(std::pair<iterator, bool>) emplace_new_key(EntryPointer * bucket, Args &&... args)
    {
        using std::swap;
        if (is_full())
        {
            grow();
            return emplace(std::forward<Args>(args)...);
        }
        else
        {
            EntryPointer new_entry = AllocatorTraits::allocate(*this, 1);
            try
            {
                AllocatorTraits::construct(*this, std::addressof(new_entry->value), std::forward<Args>(args)...);
            }
            catch(...)
            {
                AllocatorTraits::deallocate(*this, new_entry, 1);
                throw;
            }
            ++num_elements;
            new_entry->next = *bucket;
            *bucket = new_entry;
            return { { new_entry, bucket }, true };
        }
    }

    bool is_full() const
    {
        if (!num_slots_minus_one)
            return true;
        else
            return num_elements + 1 > (num_slots_minus_one + 1) * static_cast<double>(_max_load_factor);
    }

    void grow()
    {
        rehash(std::max(size_t(4), 2 * bucket_count()));
    }

    void deallocate_data()
    {
        if (entries != Entry::empty_pointer())
        {
            BucketAllocatorTraits::deallocate(*this, entries - 1, num_slots_minus_one + 2);
        }
    }

    void reset_to_empty_state()
    {
        deallocate_data();
        entries = Entry::empty_pointer();
        num_slots_minus_one = 0;
        hash_policy.reset();
    }

    template<typename U>
    size_t hash_object(const U & key)
    {
        return static_cast<Hasher &>(*this)(key);
    }
    template<typename U>
    size_t hash_object(const U & key) const
    {
        return static_cast<const Hasher &>(*this)(key);
    }
    template<typename L, typename R>
    bool compares_equal(const L & lhs, const R & rhs)
    {
        return static_cast<Equal &>(*this)(lhs, rhs);
    }

    struct convertible_to_iterator
    {
        EntryPointer element;
        EntryPointer * bucket;

        operator iterator()
        {
            if (element)
                return { element, bucket };
            else
            {
                do
                {
                    --bucket;
                }
                while (!*bucket);
                return { *bucket, bucket };
            }
        }
        operator const_iterator()
        {
            if (element)
                return { element, bucket };
            else
            {
                do
                {
                    --bucket;
                }
                while (!*bucket);
                return { *bucket, bucket };
            }
        }
    };
};
}


template<typename K, typename V, typename H = std::hash<K>, typename E = std::equal_to<K>, typename A = std::allocator<std::pair<K, V> > >
class unordered_map
        : public detailv10::sherwood_v10_table
        <
            std::pair<K, V>,
            K,
            H,
            detailv10::KeyOrValueHasher<K, std::pair<K, V>, H>,
            E,
            detailv10::KeyOrValueEquality<K, std::pair<K, V>, E>,
            A,
            typename std::allocator_traits<A>::template rebind_alloc<detailv10::sherwood_v10_entry<std::pair<K, V>, A>>,
            typename std::allocator_traits<A>::template rebind_alloc<typename std::allocator_traits<A>::template rebind_traits<detailv10::sherwood_v10_entry<std::pair<K, V>, A>>::pointer>
        >
{
    using Table = detailv10::sherwood_v10_table
    <
        std::pair<K, V>,
        K,
        H,
        detailv10::KeyOrValueHasher<K, std::pair<K, V>, H>,
        E,
        detailv10::KeyOrValueEquality<K, std::pair<K, V>, E>,
        A,
        typename std::allocator_traits<A>::template rebind_alloc<detailv10::sherwood_v10_entry<std::pair<K, V>, A>>,
        typename std::allocator_traits<A>::template rebind_alloc<typename std::allocator_traits<A>::template rebind_traits<detailv10::sherwood_v10_entry<std::pair<K, V>, A>>::pointer>
    >;
public:

    using key_type = K;
    using mapped_type = V;

    using Table::Table;
    unordered_map()
    {
    }

    V & operator[](const K & key)
    {
        return emplace(key, convertible_to_value()).first->second;
    }
    V & operator[](K && key)
    {
        return emplace(std::move(key), convertible_to_value()).first->second;
    }
    V & at(const K & key)
    {
        auto found = this->find(key);
        if (found == this->end())
            throw std::out_of_range("Argument passed to at() was not in the map.");
        return found->second;
    }
    const V & at(const K & key) const
    {
        auto found = this->find(key);
        if (found == this->end())
            throw std::out_of_range("Argument passed to at() was not in the map.");
        return found->second;
    }

    using Table::emplace;
    std::pair<typename Table::iterator, bool> emplace()
    {
        return emplace(key_type(), convertible_to_value());
    }

    friend bool operator==(const unordered_map & lhs, const unordered_map & rhs)
    {
        if (lhs.size() != rhs.size())
            return false;
        for (const typename Table::value_type & value : lhs)
        {
            auto found = rhs.find(value.first);
            if (found == rhs.end())
                return false;
            else if (value.second != found->second)
                return false;
        }
        return true;
    }
    friend bool operator!=(const unordered_map & lhs, const unordered_map & rhs)
    {
        return !(lhs == rhs);
    }

private:
    struct convertible_to_value
    {
        operator V() const
        {
            return V();
        }
    };
};

template<typename T, typename H = std::hash<T>, typename E = std::equal_to<T>, typename A = std::allocator<T> >
class unordered_set
        : public detailv10::sherwood_v10_table
        <
            T,
            T,
            H,
            detailv10::functor_storage<size_t, H>,
            E,
            detailv10::functor_storage<bool, E>,
            A,
            typename std::allocator_traits<A>::template rebind_alloc<detailv10::sherwood_v10_entry<T, A>>,
            typename std::allocator_traits<A>::template rebind_alloc<typename std::allocator_traits<A>::template rebind_traits<detailv10::sherwood_v10_entry<T, A>>::pointer>
        >
{
    using Table = detailv10::sherwood_v10_table
    <
        T,
        T,
        H,
        detailv10::functor_storage<size_t, H>,
        E,
        detailv10::functor_storage<bool, E>,
        A,
        typename std::allocator_traits<A>::template rebind_alloc<detailv10::sherwood_v10_entry<T, A>>,
        typename std::allocator_traits<A>::template rebind_alloc<typename std::allocator_traits<A>::template rebind_traits<detailv10::sherwood_v10_entry<T, A>>::pointer>
    >;
public:

    using key_type = T;

    using Table::Table;
    unordered_set()
    {
    }

    template<typename... Args>
    std::pair<typename Table::iterator, bool> emplace(Args &&... args)
    {
        return Table::emplace(T(std::forward<Args>(args)...));
    }
    std::pair<typename Table::iterator, bool> emplace(const key_type & arg)
    {
        return Table::emplace(arg);
    }
    std::pair<typename Table::iterator, bool> emplace(key_type & arg)
    {
        return Table::emplace(arg);
    }
    std::pair<typename Table::iterator, bool> emplace(const key_type && arg)
    {
        return Table::emplace(std::move(arg));
    }
    std::pair<typename Table::iterator, bool> emplace(key_type && arg)
    {
        return Table::emplace(std::move(arg));
    }

    friend bool operator==(const unordered_set & lhs, const unordered_set & rhs)
    {
        if (lhs.size() != rhs.size())
            return false;
        for (const T & value : lhs)
        {
            if (rhs.find(value) == rhs.end())
                return false;
        }
        return true;
    }
    friend bool operator!=(const unordered_set & lhs, const unordered_set & rhs)
    {
        return !(lhs == rhs);
    }
};

} // end namespace ska
