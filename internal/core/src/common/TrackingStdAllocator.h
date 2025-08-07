#pragma once

#include <cstddef>
#include <memory>
#include <atomic>
#include <type_traits>

// TrackingStdAllocator is a wrapper around std::allocator that tracks the
// total number of bytes allocated via this allocator family (shared across
// rebound types) in a thread-safe manner.
template <typename T>
struct TrackingStdAllocator : public std::allocator<T> {
    using base_type = std::allocator<T>;
    using value_type = T;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;
    using pointer = T*;
    using const_pointer = const T*;

    // Propagation traits to behave well with standard containers
    using propagate_on_container_move_assignment = std::true_type;
    using propagate_on_container_swap = std::true_type;
    using is_always_equal = std::false_type;

    template <class U>
    struct rebind {
        using other = TrackingStdAllocator<U>;
    };

    TrackingStdAllocator() noexcept = default;

    TrackingStdAllocator(const TrackingStdAllocator&) noexcept = default;

    template <class U>
    TrackingStdAllocator(const TrackingStdAllocator<U>& other) noexcept
        : base_type(), counter_(other.counter_) {
    }

    pointer allocate(size_type n) {
        pointer p = base_type::allocate(n);
        const size_type bytes = n * sizeof(T);
        counter_->fetch_add(bytes, std::memory_order_relaxed);
        return p;
    }

    void deallocate(pointer p, size_type n) noexcept {
        const size_type bytes = n * sizeof(T);
        counter_->fetch_sub(bytes, std::memory_order_relaxed);
        base_type::deallocate(p, n);
    }

    size_t total_allocated() const noexcept {
        return counter_->load(std::memory_order_relaxed);
    }

    void reset_counter() noexcept {
        counter_->store(0, std::memory_order_relaxed);
    }

    template <class U>
    bool operator==(const TrackingStdAllocator<U>& other) const noexcept {
        return counter_.get() == other.counter_.get();
    }

    template <class U>
    bool operator!=(const TrackingStdAllocator<U>& other) const noexcept {
        return !(*this == other);
    }

    template <typename>
    friend struct TrackingStdAllocator;

  private:
    std::shared_ptr<std::atomic<size_t>> counter_{
        std::make_shared<std::atomic<size_t>>(0)};
};
