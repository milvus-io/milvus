#pragma once

#include <cstdint>
#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace knowhere {

struct Binary {
    std::shared_ptr<uint8_t[]> data;
    int64_t size = 0;
};

using BinaryPtr = std::shared_ptr<Binary>;

inline uint8_t*
CopyBinary(const BinaryPtr& binary) {
    uint8_t* copied = new uint8_t[binary->size];
    std::memcpy(copied, binary->data.get(), binary->size);
    return copied;
}

class BinarySet {
 public:
    BinaryPtr
    GetByName(const std::string& name) const {
        auto it = binary_map_.find(name);
        return it == binary_map_.end() ? nullptr : it->second;
    }

    BinaryPtr
    GetByNames(const std::vector<std::string>& names) const {
        for (const auto& name : names) {
            if (auto found = GetByName(name); found != nullptr) {
                return found;
            }
        }
        return nullptr;
    }

    void
    Append(const std::string& name, BinaryPtr binary) {
        binary_map_[name] = std::move(binary);
    }

    void
    Append(const std::string& name,
           std::shared_ptr<uint8_t[]> data,
           int64_t size) {
        auto binary = std::make_shared<Binary>();
        binary->data = std::move(data);
        binary->size = size;
        binary_map_[name] = std::move(binary);
    }

    BinaryPtr
    Erase(const std::string& name) {
        auto it = binary_map_.find(name);
        if (it == binary_map_.end()) {
            return nullptr;
        }
        auto binary = it->second;
        binary_map_.erase(it);
        return binary;
    }

    void
    clear() {
        binary_map_.clear();
    }

    bool
    Contains(const std::string& name) const {
        return binary_map_.find(name) != binary_map_.end();
    }

    std::map<std::string, BinaryPtr> binary_map_;
};

using BinarySetPtr = std::shared_ptr<BinarySet>;

}  // namespace knowhere
