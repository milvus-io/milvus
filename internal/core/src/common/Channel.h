#include <oneapi/tbb/concurrent_queue.h>

#include <atomic>
#include <optional>

namespace milvus {
template <typename T>
class Channel {
 public:
    // unbounded channel
    Channel() = default;

    // bounded channel
    explicit Channel(size_t capacity) {
        inner_.set_capacity(capacity);
    }

    void
    set_capacity(size_t capacity) {
        inner_.set_capacity(capacity);
    }

    void
    push(const T& value) {
        inner_.push(value);
    }

    bool
    pop(T& value) {
        std::optional<T> result;
        inner_.pop(result);
        if (!result.has_value()) {
            return false;
        }
        value = std::move(result.value());
        return true;
    }

    void
    close() {
        inner_.push(std::nullopt);
    }

 private:
    oneapi::tbb::concurrent_bounded_queue<std::optional<T>> inner_{};
};
}  // namespace milvus
