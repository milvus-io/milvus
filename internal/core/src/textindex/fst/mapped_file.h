#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/mman.h>
#include <sys/stat.h>
#include <system_error>
#include <unistd.h>

namespace milvus::textindex {

class MappedFile {
 public:
    MappedFile() = default;

    explicit MappedFile(std::string_view path) {
        Map(path);
    }

    ~MappedFile() {
        Reset();
    }

    MappedFile(const MappedFile&) = delete;
    MappedFile&
    operator=(const MappedFile&) = delete;

    MappedFile(MappedFile&& other) noexcept
        : address_(other.address_), size_(other.size_) {
        other.address_ = MAP_FAILED;
        other.size_ = 0;
    }

    MappedFile&
    operator=(MappedFile&& other) noexcept {
        if (this != &other) {
            Reset();
            address_ = other.address_;
            size_ = other.size_;
            other.address_ = MAP_FAILED;
            other.size_ = 0;
        }
        return *this;
    }

    void
    Map(std::string_view path) {
        Reset();
        const std::string owned_path(path);
        const int descriptor = ::open(owned_path.c_str(), O_RDONLY | O_CLOEXEC);
        if (descriptor < 0) {
            throw std::system_error(errno,
                                    std::generic_category(),
                                    "open mmap file: " + owned_path);
        }

        struct stat status {};
        if (::fstat(descriptor, &status) != 0) {
            const auto error = errno;
            ::close(descriptor);
            throw std::system_error(error,
                                    std::generic_category(),
                                    "stat mmap file: " + owned_path);
        }
        if (status.st_size <= 0) {
            ::close(descriptor);
            throw std::runtime_error("cannot mmap empty file: " + owned_path);
        }
        if (static_cast<std::uintmax_t>(status.st_size) >
            static_cast<std::uintmax_t>(SIZE_MAX)) {
            ::close(descriptor);
            throw std::runtime_error("mmap file is too large: " + owned_path);
        }

        size_ = static_cast<std::size_t>(status.st_size);
        address_ = ::mmap(nullptr, size_, PROT_READ, MAP_SHARED, descriptor, 0);
        const auto map_error = errno;
        ::close(descriptor);
        if (address_ == MAP_FAILED) {
            size_ = 0;
            throw std::system_error(
                map_error, std::generic_category(), "mmap file: " + owned_path);
        }
    }

    void
    Reset() noexcept {
        if (address_ != MAP_FAILED) {
            ::munmap(address_, size_);
            address_ = MAP_FAILED;
            size_ = 0;
        }
    }

    [[nodiscard]] bool
    IsMapped() const noexcept {
        return address_ != MAP_FAILED;
    }

    [[nodiscard]] std::span<const std::uint8_t>
    Bytes() const noexcept {
        if (!IsMapped()) {
            return {};
        }
        return {static_cast<const std::uint8_t*>(address_), size_};
    }

 private:
    void* address_ = MAP_FAILED;
    std::size_t size_ = 0;
};

}  // namespace milvus::textindex
