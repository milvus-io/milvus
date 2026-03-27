#pragma once

#include <atomic>
#include <cstddef>
#include <functional>
#include <utility>

namespace knowhere {

using ViewDataOp = std::function<const void*(size_t)>;

class Object {
 public:
    Object() = default;

    Object(std::nullptr_t) {
    }

    uint32_t
    Ref() const {
        return ref_count_.load(std::memory_order_relaxed);
    }

    void
    IncRef() const {
        ref_count_.fetch_add(1, std::memory_order_relaxed);
    }

    void
    DecRef() const {
        ref_count_.fetch_sub(1, std::memory_order_relaxed);
    }

    virtual ~Object() = default;

 private:
    mutable std::atomic_uint32_t ref_count_{1};
};

template <typename T>
class Pack : public Object {
 public:
    Pack() = default;

    explicit Pack(T package) : package_(std::move(package)) {
    }

    const T&
    GetPack() const {
        return package_;
    }

 private:
    T package_{};
};

}  // namespace knowhere
