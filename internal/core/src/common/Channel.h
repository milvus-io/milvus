#include <oneapi/tbb/concurrent_queue.h>

#include <atomic>
#include <exception>
#include <optional>

namespace milvus {
template <typename T>
class Channel {
 public:
    // unbounded channel
    Channel() = default;

    Channel(Channel<T>&& other) noexcept : inner_(std::move(other.inner_)) {
    }

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
            if (ex_.has_value()) {
                throw ex_.value();
            }
            return false;
        }
        value = std::move(result.value());
        return true;
    }

    void
    close(std::optional<std::exception> ex = std::nullopt) {
        if (ex.has_value()) {
            ex_ = std::move(ex);
        }
        inner_.push(std::nullopt);
    }

 private:
    oneapi::tbb::concurrent_bounded_queue<std::optional<T>> inner_{};
    std::optional<std::exception> ex_{};
};
}  // namespace milvus
