//          Copyright Malte Skarupke 2017.
// Distributed under the Boost Software License, Version 1.0.
//    (See http://www.boost.org/LICENSE_1_0.txt)

#pragma once

#include <cstdint>
#include <cstddef>
#include <cmath>
#include <algorithm>
#include <iterator>
#include <utility>
#include <type_traits>
#include "flat_hash_map.hpp"
#include <vector>
#include <array>

namespace ska
{

namespace detailv8
{
using ska::detailv3::functor_storage;
using ska::detailv3::KeyOrValueHasher;
using ska::detailv3::KeyOrValueEquality;
using ska::detailv3::AssignIfTrue;
using ska::detailv3::HashPolicySelector;

template<typename = void>
struct sherwood_v8_constants
{
    static constexpr int8_t magic_for_empty = int8_t(0b11111111);
    static constexpr int8_t magic_for_reserved = int8_t(0b11111110);
    static constexpr int8_t bits_for_direct_hit = int8_t(0b10000000);
    static constexpr int8_t magic_for_direct_hit = int8_t(0b00000000);
    static constexpr int8_t magic_for_list_entry = int8_t(0b10000000);

    static constexpr int8_t bits_for_distance = int8_t(0b01111111);
    inline static int distance_from_metadata(int8_t metadata)
    {
        return metadata & bits_for_distance;
    }

    static constexpr int num_jump_distances = 126;
    // jump distances chosen like this:
    // 1. pick the first 16 integers to promote staying in the same block
    // 2. add the next 66 triangular numbers to get even jumps when
    // the hash table is a power of two
    // 3. add 44 more triangular numbers at a much steeper growth rate
    // to get a sequence that allows large jumps so that a table
    // with 10000 sequential numbers doesn't endlessly re-allocate
    static constexpr size_t jump_distances[num_jump_distances]
    {
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,

        21, 28, 36, 45, 55, 66, 78, 91, 105, 120, 136, 153, 171, 190, 210, 231,
        253, 276, 300, 325, 351, 378, 406, 435, 465, 496, 528, 561, 595, 630,
        666, 703, 741, 780, 820, 861, 903, 946, 990, 1035, 1081, 1128, 1176,
        1225, 1275, 1326, 1378, 1431, 1485, 1540, 1596, 1653, 1711, 1770, 1830,
        1891, 1953, 2016, 2080, 2145, 2211, 2278, 2346, 2415, 2485, 2556,

        3741, 8385, 18915, 42486, 95703, 215496, 485605, 1091503, 2456436,
        5529475, 12437578, 27986421, 62972253, 141700195, 318819126, 717314626,
        1614000520, 3631437253, 8170829695, 18384318876, 41364501751,
        93070021080, 209407709220, 471167588430, 1060127437995, 2385287281530,
        5366895564381, 12075513791265, 27169907873235, 61132301007778,
        137547673121001, 309482258302503, 696335090510256, 1566753939653640,
        3525196427195653, 7931691866727775, 17846306747368716,
        40154190394120111, 90346928493040500, 203280588949935750,
        457381324898247375, 1029107980662394500, 2315492957028380766,
        5209859150892887590,
    };
};
template<typename T>
constexpr int8_t sherwood_v8_constants<T>::magic_for_empty;
template<typename T>
constexpr int8_t sherwood_v8_constants<T>::magic_for_reserved;
template<typename T>
constexpr int8_t sherwood_v8_constants<T>::bits_for_direct_hit;
template<typename T>
constexpr int8_t sherwood_v8_constants<T>::magic_for_direct_hit;
template<typename T>
constexpr int8_t sherwood_v8_constants<T>::magic_for_list_entry;

template<typename T>
constexpr int8_t sherwood_v8_constants<T>::bits_for_distance;

template<typename T>
constexpr int sherwood_v8_constants<T>::num_jump_distances;
template<typename T>
constexpr size_t sherwood_v8_constants<T>::jump_distances[num_jump_distances];

template<typename T, uint8_t BlockSize>
struct sherwood_v8_block
{
    sherwood_v8_block()
    {
    }
    ~sherwood_v8_block()
    {
    }
    int8_t control_bytes[BlockSize];
    union
    {
        T data[BlockSize];
    };

    static sherwood_v8_block * empty_block()
    {
        static std::array<int8_t, BlockSize> empty_bytes = []
        {
            std::array<int8_t, BlockSize> result;
            result.fill(sherwood_v8_constants<>::magic_for_empty);
            return result;
        }();
        return reinterpret_cast<sherwood_v8_block *>(&empty_bytes);
    }

    int first_empty_index() const
    {
        for (int i = 0; i < BlockSize; ++i)
        {
            if (control_bytes[i] == sherwood_v8_constants<>::magic_for_empty)
                return i;
        }
        return -1;
    }

    void fill_control_bytes(int8_t value)
    {
        std::fill(std::begin(control_bytes), std::end(control_bytes), value);
    }
};

template<typename T, typename FindKey, typename ArgumentHash, typename Hasher, typename ArgumentEqual, typename Equal, typename ArgumentAlloc, typename ByteAlloc, uint8_t BlockSize>
class sherwood_v8_table : private ByteAlloc, private Hasher, private Equal
{
    using AllocatorTraits = std::allocator_traits<ByteAlloc>;
    using BlockType = sherwood_v8_block<T, BlockSize>;
    using BlockPointer = BlockType *;
    using BytePointer = typename AllocatorTraits::pointer;
    struct convertible_to_iterator;
    using Constants = sherwood_v8_constants<>;

public:

    using value_type = T;
    using size_type = size_t;
    using difference_type = std::ptrdiff_t;
    using hasher = ArgumentHash;
    using key_equal = ArgumentEqual;
    using allocator_type = ByteAlloc;
    using reference = value_type &;
    using const_reference = const value_type &;
    using pointer = value_type *;
    using const_pointer = const value_type *;

    sherwood_v8_table()
    {
    }
    explicit sherwood_v8_table(size_type bucket_count, const ArgumentHash & hash = ArgumentHash(), const ArgumentEqual & equal = ArgumentEqual(), const ArgumentAlloc & alloc = ArgumentAlloc())
        : ByteAlloc(alloc), Hasher(hash), Equal(equal)
    {
        if (bucket_count)
            rehash(bucket_count);
    }
    sherwood_v8_table(size_type bucket_count, const ArgumentAlloc & alloc)
        : sherwood_v8_table(bucket_count, ArgumentHash(), ArgumentEqual(), alloc)
    {
    }
    sherwood_v8_table(size_type bucket_count, const ArgumentHash & hash, const ArgumentAlloc & alloc)
        : sherwood_v8_table(bucket_count, hash, ArgumentEqual(), alloc)
    {
    }
    explicit sherwood_v8_table(const ArgumentAlloc & alloc)
        : ByteAlloc(alloc)
    {
    }
    template<typename It>
    sherwood_v8_table(It first, It last, size_type bucket_count = 0, const ArgumentHash & hash = ArgumentHash(), const ArgumentEqual & equal = ArgumentEqual(), const ArgumentAlloc & alloc = ArgumentAlloc())
        : sherwood_v8_table(bucket_count, hash, equal, alloc)
    {
        insert(first, last);
    }
    template<typename It>
    sherwood_v8_table(It first, It last, size_type bucket_count, const ArgumentAlloc & alloc)
        : sherwood_v8_table(first, last, bucket_count, ArgumentHash(), ArgumentEqual(), alloc)
    {
    }
    template<typename It>
    sherwood_v8_table(It first, It last, size_type bucket_count, const ArgumentHash & hash, const ArgumentAlloc & alloc)
        : sherwood_v8_table(first, last, bucket_count, hash, ArgumentEqual(), alloc)
    {
    }
    sherwood_v8_table(std::initializer_list<T> il, size_type bucket_count = 0, const ArgumentHash & hash = ArgumentHash(), const ArgumentEqual & equal = ArgumentEqual(), const ArgumentAlloc & alloc = ArgumentAlloc())
        : sherwood_v8_table(bucket_count, hash, equal, alloc)
    {
        if (bucket_count == 0)
            rehash(il.size());
        insert(il.begin(), il.end());
    }
    sherwood_v8_table(std::initializer_list<T> il, size_type bucket_count, const ArgumentAlloc & alloc)
        : sherwood_v8_table(il, bucket_count, ArgumentHash(), ArgumentEqual(), alloc)
    {
    }
    sherwood_v8_table(std::initializer_list<T> il, size_type bucket_count, const ArgumentHash & hash, const ArgumentAlloc & alloc)
        : sherwood_v8_table(il, bucket_count, hash, ArgumentEqual(), alloc)
    {
    }
    sherwood_v8_table(const sherwood_v8_table & other)
        : sherwood_v8_table(other, AllocatorTraits::select_on_container_copy_construction(other.get_allocator()))
    {
    }
    sherwood_v8_table(const sherwood_v8_table & other, const ArgumentAlloc & alloc)
        : ByteAlloc(alloc), Hasher(other), Equal(other), _max_load_factor(other._max_load_factor)
    {
        rehash_for_other_container(other);
        try
        {
            insert(other.begin(), other.end());
        }
        catch(...)
        {
            clear();
            deallocate_data(entries, num_slots_minus_one);
            throw;
        }
    }
    sherwood_v8_table(sherwood_v8_table && other) noexcept
        : ByteAlloc(std::move(other)), Hasher(std::move(other)), Equal(std::move(other))
        , _max_load_factor(other._max_load_factor)
    {
        swap_pointers(other);
    }
    sherwood_v8_table(sherwood_v8_table && other, const ArgumentAlloc & alloc) noexcept
        : ByteAlloc(alloc), Hasher(std::move(other)), Equal(std::move(other))
        , _max_load_factor(other._max_load_factor)
    {
        swap_pointers(other);
    }
    sherwood_v8_table & operator=(const sherwood_v8_table & other)
    {
        if (this == std::addressof(other))
            return *this;

        clear();
        if (AllocatorTraits::propagate_on_container_copy_assignment::value)
        {
            if (static_cast<ByteAlloc &>(*this) != static_cast<const ByteAlloc &>(other))
            {
                reset_to_empty_state();
            }
            AssignIfTrue<ByteAlloc, AllocatorTraits::propagate_on_container_copy_assignment::value>()(*this, other);
        }
        _max_load_factor = other._max_load_factor;
        static_cast<Hasher &>(*this) = other;
        static_cast<Equal &>(*this) = other;
        rehash_for_other_container(other);
        insert(other.begin(), other.end());
        return *this;
    }
    sherwood_v8_table & operator=(sherwood_v8_table && other) noexcept
    {
        if (this == std::addressof(other))
            return *this;
        else if (AllocatorTraits::propagate_on_container_move_assignment::value)
        {
            clear();
            reset_to_empty_state();
            AssignIfTrue<ByteAlloc, AllocatorTraits::propagate_on_container_move_assignment::value>()(*this, std::move(other));
            swap_pointers(other);
        }
        else if (static_cast<ByteAlloc &>(*this) == static_cast<ByteAlloc &>(other))
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
    ~sherwood_v8_table()
    {
        clear();
        deallocate_data(entries, num_slots_minus_one);
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
    private:
        friend class sherwood_v8_table;
        BlockPointer current = BlockPointer();
        size_t index = 0;

    public:
        templated_iterator()
        {
        }
        templated_iterator(BlockPointer entries, size_t index)
            : current(entries)
            , index(index)
        {
        }

        using iterator_category = std::forward_iterator_tag;
        using value_type = ValueType;
        using difference_type = ptrdiff_t;
        using pointer = ValueType *;
        using reference = ValueType &;

        friend bool operator==(const templated_iterator & lhs, const templated_iterator & rhs)
        {
            return lhs.index == rhs.index;
        }
        friend bool operator!=(const templated_iterator & lhs, const templated_iterator & rhs)
        {
            return !(lhs == rhs);
        }

        templated_iterator & operator++()
        {
            do
            {
                if (index % BlockSize == 0)
                    --current;
                if (index-- == 0)
                    break;
            }
            while(current->control_bytes[index % BlockSize] == Constants::magic_for_empty);
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
            return current->data[index % BlockSize];
        }
        ValueType * operator->() const
        {
            return current->data + index % BlockSize;
        }

        operator templated_iterator<const value_type>() const
        {
            return { current, index };
        }
    };
    using iterator = templated_iterator<value_type>;
    using const_iterator = templated_iterator<const value_type>;

    iterator begin()
    {
        size_t num_slots = num_slots_minus_one ? num_slots_minus_one + 1 : 0;
        return ++iterator{ entries + num_slots / BlockSize, num_slots };
    }
    const_iterator begin() const
    {
        size_t num_slots = num_slots_minus_one ? num_slots_minus_one + 1 : 0;
        return ++iterator{ entries + num_slots / BlockSize, num_slots };
    }
    const_iterator cbegin() const
    {
        return begin();
    }
    iterator end()
    {
        return { entries - 1, std::numeric_limits<size_t>::max() };
    }
    const_iterator end() const
    {
        return { entries - 1, std::numeric_limits<size_t>::max() };
    }
    const_iterator cend() const
    {
        return end();
    }

    inline iterator find(const FindKey & key)
    {
        size_t index = hash_object(key);
        size_t num_slots_minus_one = this->num_slots_minus_one;
        BlockPointer entries = this->entries;
        index = hash_policy.index_for_hash(index, num_slots_minus_one);
        bool first = true;
        for (;;)
        {
            size_t block_index = index / BlockSize;
            int index_in_block = index % BlockSize;
            BlockPointer block = entries + block_index;
            int8_t metadata = block->control_bytes[index_in_block];
            if (first)
            {
                if ((metadata & Constants::bits_for_direct_hit) != Constants::magic_for_direct_hit)
                    return end();
                first = false;
            }
            if (compares_equal(key, block->data[index_in_block]))
                return { block, index };
            int8_t to_next_index = metadata & Constants::bits_for_distance;
            if (to_next_index == 0)
                return end();
            index += Constants::jump_distances[to_next_index];
            index = hash_policy.keep_in_range(index, num_slots_minus_one);
        }
    }
    inline const_iterator find(const FindKey & key) const
    {
        return const_cast<sherwood_v8_table *>(this)->find(key);
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
    inline std::pair<iterator, bool> emplace(Key && key, Args &&... args)
    {
        size_t index = hash_object(key);
        size_t num_slots_minus_one = this->num_slots_minus_one;
        BlockPointer entries = this->entries;
        index = hash_policy.index_for_hash(index, num_slots_minus_one);
        bool first = true;
        for (;;)
        {
            size_t block_index = index / BlockSize;
            int index_in_block = index % BlockSize;
            BlockPointer block = entries + block_index;
            int8_t metadata = block->control_bytes[index_in_block];
            if (first)
            {
                if ((metadata & Constants::bits_for_direct_hit) != Constants::magic_for_direct_hit)
                    return emplace_direct_hit({ index, block }, std::forward<Key>(key), std::forward<Args>(args)...);
                first = false;
            }
            if (compares_equal(key, block->data[index_in_block]))
                return { { block, index }, false };
            int8_t to_next_index = metadata & Constants::bits_for_distance;
            if (to_next_index == 0)
                return emplace_new_key({ index, block }, std::forward<Key>(key), std::forward<Args>(args)...);
            index += Constants::jump_distances[to_next_index];
            index = hash_policy.keep_in_range(index, num_slots_minus_one);
        }
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

    void rehash(size_t num_items)
    {
        num_items = std::max(num_items, static_cast<size_t>(std::ceil(num_elements / static_cast<double>(_max_load_factor))));
        if (num_items == 0)
        {
            reset_to_empty_state();
            return;
        }
        auto new_prime_index = hash_policy.next_size_over(num_items);
        if (num_items == num_slots_minus_one + 1)
            return;
        size_t num_blocks = num_items / BlockSize;
        if (num_items % BlockSize)
            ++num_blocks;
        size_t memory_requirement = calculate_memory_requirement(num_blocks);
        unsigned char * new_memory = &*AllocatorTraits::allocate(*this, memory_requirement);

        BlockPointer new_buckets = reinterpret_cast<BlockPointer>(new_memory);

        BlockPointer special_end_item = new_buckets + num_blocks;
        for (BlockPointer it = new_buckets; it <= special_end_item; ++it)
            it->fill_control_bytes(Constants::magic_for_empty);
        using std::swap;
        swap(entries, new_buckets);
        swap(num_slots_minus_one, num_items);
        --num_slots_minus_one;
        hash_policy.commit(new_prime_index);
        num_elements = 0;
        if (num_items)
            ++num_items;
        size_t old_num_blocks = num_items / BlockSize;
        if (num_items % BlockSize)
            ++old_num_blocks;
        for (BlockPointer it = new_buckets, end = new_buckets + old_num_blocks; it != end; ++it)
        {
            for (int i = 0; i < BlockSize; ++i)
            {
                int8_t metadata = it->control_bytes[i];
                if (metadata != Constants::magic_for_empty && metadata != Constants::magic_for_reserved)
                {
                    emplace(std::move(it->data[i]));
                    AllocatorTraits::destroy(*this, it->data + i);
                }
            }
        }
        deallocate_data(new_buckets, num_items - 1);
    }

    void reserve(size_t num_elements)
    {
        size_t required_buckets = num_buckets_for_reserve(num_elements);
        if (required_buckets > bucket_count())
            rehash(required_buckets);
    }

    // the return value is a type that can be converted to an iterator
    // the reason for doing this is that it's not free to find the
    // iterator pointing at the next element. if you care about the
    // next iterator, turn the return value into an iterator
    convertible_to_iterator erase(const_iterator to_erase)
    {
        LinkedListIt current = { to_erase.index, to_erase.current };
        if (current.has_next())
        {
            LinkedListIt previous = current;
            LinkedListIt next = current.next(*this);
            while (next.has_next())
            {
                previous = next;
                next = next.next(*this);
            }
            AllocatorTraits::destroy(*this, std::addressof(*current));
            AllocatorTraits::construct(*this, std::addressof(*current), std::move(*next));
            AllocatorTraits::destroy(*this, std::addressof(*next));
            next.set_metadata(Constants::magic_for_empty);
            previous.clear_next();
        }
        else
        {
            if (!current.is_direct_hit())
                find_parent_block(current).clear_next();
            AllocatorTraits::destroy(*this, std::addressof(*current));
            current.set_metadata(Constants::magic_for_empty);
        }
        --num_elements;
        return { to_erase.current, to_erase.index };
    }

    iterator erase(const_iterator begin_it, const_iterator end_it)
    {
        if (begin_it == end_it)
            return { begin_it.current, begin_it.index };
        if (std::next(begin_it) == end_it)
            return erase(begin_it);
        if (begin_it == begin() && end_it == end())
        {
            clear();
            return { end_it.current, end_it.index };
        }
        std::vector<std::pair<int, LinkedListIt>> depth_in_chain;
        for (const_iterator it = begin_it; it != end_it; ++it)
        {
            LinkedListIt list_it(it.index, it.current);
            if (list_it.is_direct_hit())
                depth_in_chain.emplace_back(0, list_it);
            else
            {
                LinkedListIt root = find_direct_hit(list_it);
                int distance = 1;
                for (;;)
                {
                    LinkedListIt next = root.next(*this);
                    if (next == list_it)
                        break;
                    ++distance;
                    root = next;
                }
                depth_in_chain.emplace_back(distance, list_it);
            }
        }
        std::sort(depth_in_chain.begin(), depth_in_chain.end(), [](const auto & a, const auto & b) { return a.first < b.first; });
        for (auto it = depth_in_chain.rbegin(), end = depth_in_chain.rend(); it != end; ++it)
        {
            erase(it->second.it());
        }

        if (begin_it.current->control_bytes[begin_it.index % BlockSize] == Constants::magic_for_empty)
            return ++iterator{ begin_it.current, begin_it.index };
        else
            return { begin_it.current, begin_it.index };
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
        size_t num_slots = num_slots_minus_one + 1;
        size_t num_blocks = num_slots / BlockSize;
        if (num_slots % BlockSize)
            ++num_blocks;
        for (BlockPointer it = entries, end = it + num_blocks; it != end; ++it)
        {
            for (int i = 0; i < BlockSize; ++i)
            {
                if (it->control_bytes[i] != Constants::magic_for_empty)
                {
                    AllocatorTraits::destroy(*this, std::addressof(it->data[i]));
                    it->control_bytes[i] = Constants::magic_for_empty;
                }
            }
        }
        num_elements = 0;
    }

    void shrink_to_fit()
    {
        rehash_for_other_container(*this);
    }

    void swap(sherwood_v8_table & other)
    {
        using std::swap;
        swap_pointers(other);
        swap(static_cast<ArgumentHash &>(*this), static_cast<ArgumentHash &>(other));
        swap(static_cast<ArgumentEqual &>(*this), static_cast<ArgumentEqual &>(other));
        if (AllocatorTraits::propagate_on_container_swap::value)
            swap(static_cast<ByteAlloc &>(*this), static_cast<ByteAlloc &>(other));
    }

    size_t size() const
    {
        return num_elements;
    }
    size_t max_size() const
    {
        return (AllocatorTraits::max_size(*this)) / sizeof(T);
    }
    size_t bucket_count() const
    {
        return num_slots_minus_one ? num_slots_minus_one + 1 : 0;
    }
    size_type max_bucket_count() const
    {
        return (AllocatorTraits::max_size(*this)) / sizeof(T);
    }
    size_t bucket(const FindKey & key) const
    {
        return hash_policy.index_for_hash(hash_object(key), num_slots_minus_one);
    }
    float load_factor() const
    {
        return static_cast<double>(num_elements) / (num_slots_minus_one + 1);
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
    BlockPointer entries = BlockType::empty_block();
    size_t num_slots_minus_one = 0;
    typename HashPolicySelector<ArgumentHash>::type hash_policy;
    float _max_load_factor = 0.9375f;
    size_t num_elements = 0;

    size_t num_buckets_for_reserve(size_t num_elements) const
    {
        return static_cast<size_t>(std::ceil(num_elements / static_cast<double>(_max_load_factor)));
    }
    void rehash_for_other_container(const sherwood_v8_table & other)
    {
        rehash(std::min(num_buckets_for_reserve(other.size()), other.bucket_count()));
    }
    bool is_full() const
    {
        if (!num_slots_minus_one)
            return true;
        else
            return num_elements + 1 > (num_slots_minus_one + 1) * static_cast<double>(_max_load_factor);
    }

    void swap_pointers(sherwood_v8_table & other)
    {
        using std::swap;
        swap(hash_policy, other.hash_policy);
        swap(entries, other.entries);
        swap(num_slots_minus_one, other.num_slots_minus_one);
        swap(num_elements, other.num_elements);
        swap(_max_load_factor, other._max_load_factor);
    }

    struct LinkedListIt
    {
        size_t index = 0;
        BlockPointer block = nullptr;

        LinkedListIt()
        {
        }
        LinkedListIt(size_t index, BlockPointer block)
            : index(index), block(block)
        {
        }

        iterator it() const
        {
            return { block, index };
        }
        int index_in_block() const
        {
            return index % BlockSize;
        }
        bool is_direct_hit() const
        {
            return (metadata() & Constants::bits_for_direct_hit) == Constants::magic_for_direct_hit;
        }
        bool is_empty() const
        {
            return metadata() == Constants::magic_for_empty;
        }
        bool has_next() const
        {
            return jump_index() != 0;
        }
        int8_t jump_index() const
        {
            return Constants::distance_from_metadata(metadata());
        }
        int8_t metadata() const
        {
            return block->control_bytes[index_in_block()];
        }
        void set_metadata(int8_t metadata)
        {
            block->control_bytes[index_in_block()] = metadata;
        }

        LinkedListIt next(sherwood_v8_table & table) const
        {
            int8_t distance = jump_index();
            size_t next_index = table.hash_policy.keep_in_range(index + Constants::jump_distances[distance], table.num_slots_minus_one);
            return { next_index, table.entries + next_index / BlockSize };
        }
        void set_next(int8_t jump_index)
        {
            int8_t & metadata = block->control_bytes[index_in_block()];
            metadata = (metadata & ~Constants::bits_for_distance) | jump_index;
        }
        void clear_next()
        {
            set_next(0);
        }

        value_type & operator*() const
        {
            return block->data[index_in_block()];
        }
        bool operator!() const
        {
            return !block;
        }
        explicit operator bool() const
        {
            return block != nullptr;
        }
        bool operator==(const LinkedListIt & other) const
        {
            return index == other.index;
        }
        bool operator!=(const LinkedListIt & other) const
        {
            return !(*this == other);
        }
    };

    template<typename... Args>
    SKA_NOINLINE(std::pair<iterator, bool>) emplace_direct_hit(LinkedListIt block, Args &&... args)
    {
        using std::swap;
        if (is_full())
        {
            grow();
            return emplace(std::forward<Args>(args)...);
        }
        if (block.metadata() == Constants::magic_for_empty)
        {
            AllocatorTraits::construct(*this, std::addressof(*block), std::forward<Args>(args)...);
            block.set_metadata(Constants::magic_for_direct_hit);
            ++num_elements;
            return { block.it(), true };
        }
        else
        {
            LinkedListIt parent_block = find_parent_block(block);
            std::pair<int8_t, LinkedListIt> free_block = find_free_index(parent_block);
            if (!free_block.first)
            {
                grow();
                return emplace(std::forward<Args>(args)...);
            }
            value_type new_value(std::forward<Args>(args)...);
            for (LinkedListIt it = block;;)
            {
                AllocatorTraits::construct(*this, std::addressof(*free_block.second), std::move(*it));
                AllocatorTraits::destroy(*this, std::addressof(*it));
                parent_block.set_next(free_block.first);
                free_block.second.set_metadata(Constants::magic_for_list_entry);
                if (!it.has_next())
                {
                    it.set_metadata(Constants::magic_for_empty);
                    break;
                }
                LinkedListIt next = it.next(*this);
                it.set_metadata(Constants::magic_for_empty);
                block.set_metadata(Constants::magic_for_reserved);
                it = next;
                parent_block = free_block.second;
                free_block = find_free_index(free_block.second);
                if (!free_block.first)
                {
                    grow();
                    return emplace(std::move(new_value));
                }
            }
            AllocatorTraits::construct(*this, std::addressof(*block), std::move(new_value));
            block.set_metadata(Constants::magic_for_direct_hit);
            ++num_elements;
            return { block.it(), true };
        }
    }

    template<typename... Args>
    SKA_NOINLINE(std::pair<iterator, bool>) emplace_new_key(LinkedListIt parent, Args &&... args)
    {
        if (is_full())
        {
            grow();
            return emplace(std::forward<Args>(args)...);
        }
        std::pair<int8_t, LinkedListIt> free_block = find_free_index(parent);
        if (!free_block.first)
        {
            grow();
            return emplace(std::forward<Args>(args)...);
        }
        AllocatorTraits::construct(*this, std::addressof(*free_block.second), std::forward<Args>(args)...);
        free_block.second.set_metadata(Constants::magic_for_list_entry);
        parent.set_next(free_block.first);
        ++num_elements;
        return { free_block.second.it(), true };
    }

    LinkedListIt find_direct_hit(LinkedListIt child) const
    {
        size_t to_move_hash = hash_object(*child);
        size_t to_move_index = hash_policy.index_for_hash(to_move_hash, num_slots_minus_one);
        return { to_move_index, entries + to_move_index / BlockSize };
    }
    LinkedListIt find_parent_block(LinkedListIt child)
    {
        LinkedListIt parent_block = find_direct_hit(child);
        for (;;)
        {
            LinkedListIt next = parent_block.next(*this);
            if (next == child)
                return parent_block;
            parent_block = next;
        }
    }

    std::pair<int8_t, LinkedListIt> find_free_index(LinkedListIt parent) const
    {
        for (int8_t jump_index = 1; jump_index < Constants::num_jump_distances; ++jump_index)
        {
            size_t index = hash_policy.keep_in_range(parent.index + Constants::jump_distances[jump_index], num_slots_minus_one);
            BlockPointer block = entries + index / BlockSize;
            if (block->control_bytes[index % BlockSize] == Constants::magic_for_empty)
                return { jump_index, { index, block } };
        }
        return { 0, {} };
    }

    void grow()
    {
        rehash(std::max(size_t(10), 2 * bucket_count()));
    }

    size_t calculate_memory_requirement(size_t num_blocks)
    {
        size_t memory_required = sizeof(BlockType) * num_blocks;
        memory_required += BlockSize; // for metadata of past-the-end pointer
        return memory_required;
    }

    void deallocate_data(BlockPointer begin, size_t num_slots_minus_one)
    {
        if (begin == BlockType::empty_block())
            return;

        ++num_slots_minus_one;
        size_t num_blocks = num_slots_minus_one / BlockSize;
        if (num_slots_minus_one % BlockSize)
            ++num_blocks;
        size_t memory = calculate_memory_requirement(num_blocks);
        unsigned char * as_byte_pointer = reinterpret_cast<unsigned char *>(begin);
        AllocatorTraits::deallocate(*this, typename AllocatorTraits::pointer(as_byte_pointer), memory);
    }

    void reset_to_empty_state()
    {
        deallocate_data(entries, num_slots_minus_one);
        entries = BlockType::empty_block();
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
        BlockPointer it;
        size_t index;

        operator iterator()
        {
            if (it->control_bytes[index % BlockSize] == Constants::magic_for_empty)
                return ++iterator{it, index};
            else
                return { it, index };
        }
        operator const_iterator()
        {
            if (it->control_bytes[index % BlockSize] == Constants::magic_for_empty)
                return ++iterator{it, index};
            else
                return { it, index };
        }
    };
};
template<typename T, typename Enable = void>
struct AlignmentOr8Bytes
{
    static constexpr size_t value = 8;
};
template<typename T>
struct AlignmentOr8Bytes<T, typename std::enable_if<alignof(T) >= 1>::type>
{
    static constexpr size_t value = alignof(T);
};
template<typename... Args>
struct CalculateBytellBlockSize;
template<typename First, typename... More>
struct CalculateBytellBlockSize<First, More...>
{
    static constexpr size_t this_value = AlignmentOr8Bytes<First>::value;
    static constexpr size_t base_value = CalculateBytellBlockSize<More...>::value;
    static constexpr size_t value = this_value > base_value ? this_value : base_value;
};
template<>
struct CalculateBytellBlockSize<>
{
    static constexpr size_t value = 8;
};
}

template<typename K, typename V, typename H = std::hash<K>, typename E = std::equal_to<K>, typename A = std::allocator<std::pair<K, V> > >
class bytell_hash_map
        : public detailv8::sherwood_v8_table
        <
            std::pair<K, V>,
            K,
            H,
            detailv8::KeyOrValueHasher<K, std::pair<K, V>, H>,
            E,
            detailv8::KeyOrValueEquality<K, std::pair<K, V>, E>,
            A,
            typename std::allocator_traits<A>::template rebind_alloc<unsigned char>,
            detailv8::CalculateBytellBlockSize<K, V>::value
        >
{
    using Table = detailv8::sherwood_v8_table
    <
        std::pair<K, V>,
        K,
        H,
        detailv8::KeyOrValueHasher<K, std::pair<K, V>, H>,
        E,
        detailv8::KeyOrValueEquality<K, std::pair<K, V>, E>,
        A,
        typename std::allocator_traits<A>::template rebind_alloc<unsigned char>,
        detailv8::CalculateBytellBlockSize<K, V>::value
    >;
public:

    using key_type = K;
    using mapped_type = V;

    using Table::Table;
    bytell_hash_map()
    {
    }

    inline V & operator[](const K & key)
    {
        return emplace(key, convertible_to_value()).first->second;
    }
    inline V & operator[](K && key)
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
    template<typename M>
    std::pair<typename Table::iterator, bool> insert_or_assign(const key_type & key, M && m)
    {
        auto emplace_result = emplace(key, std::forward<M>(m));
        if (!emplace_result.second)
            emplace_result.first->second = std::forward<M>(m);
        return emplace_result;
    }
    template<typename M>
    std::pair<typename Table::iterator, bool> insert_or_assign(key_type && key, M && m)
    {
        auto emplace_result = emplace(std::move(key), std::forward<M>(m));
        if (!emplace_result.second)
            emplace_result.first->second = std::forward<M>(m);
        return emplace_result;
    }
    template<typename M>
    typename Table::iterator insert_or_assign(typename Table::const_iterator, const key_type & key, M && m)
    {
        return insert_or_assign(key, std::forward<M>(m)).first;
    }
    template<typename M>
    typename Table::iterator insert_or_assign(typename Table::const_iterator, key_type && key, M && m)
    {
        return insert_or_assign(std::move(key), std::forward<M>(m)).first;
    }

    friend bool operator==(const bytell_hash_map & lhs, const bytell_hash_map & rhs)
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
    friend bool operator!=(const bytell_hash_map & lhs, const bytell_hash_map & rhs)
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
class bytell_hash_set
        : public detailv8::sherwood_v8_table
        <
            T,
            T,
            H,
            detailv8::functor_storage<size_t, H>,
            E,
            detailv8::functor_storage<bool, E>,
            A,
            typename std::allocator_traits<A>::template rebind_alloc<unsigned char>,
            detailv8::CalculateBytellBlockSize<T>::value
        >
{
    using Table = detailv8::sherwood_v8_table
    <
        T,
        T,
        H,
        detailv8::functor_storage<size_t, H>,
        E,
        detailv8::functor_storage<bool, E>,
        A,
        typename std::allocator_traits<A>::template rebind_alloc<unsigned char>,
        detailv8::CalculateBytellBlockSize<T>::value
    >;
public:

    using key_type = T;

    using Table::Table;
    bytell_hash_set()
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

    friend bool operator==(const bytell_hash_set & lhs, const bytell_hash_set & rhs)
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
    friend bool operator!=(const bytell_hash_set & lhs, const bytell_hash_set & rhs)
    {
        return !(lhs == rhs);
    }
};

} // end namespace ska
