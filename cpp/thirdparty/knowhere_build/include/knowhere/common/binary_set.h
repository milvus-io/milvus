#pragma once

#include <map>
#include <string>
#include <vector>
#include <memory>

#include "knowhere/common/id.h"


namespace zilliz {
namespace knowhere {


struct Binary {
    ID id;
    std::shared_ptr<uint8_t> data;
    int64_t size = 0;
};
using BinaryPtr = std::shared_ptr<Binary>;


class BinarySet {
 public:
    BinaryPtr
    GetByName(const std::string &name) const {
        return binary_map_.at(name);
    }

    void
    Append(const std::string &name, BinaryPtr binary) {
        binary_map_[name] = std::move(binary);
    }

    void
    Append(const std::string &name, std::shared_ptr<uint8_t> data, int64_t size) {
        auto binary = std::make_shared<Binary>();
        binary->data = data;
        binary->size = size;
        binary_map_[name] = std::move(binary);
    }

    //void
    //Append(const std::string &name, void *data, int64_t size, ID id) {
    //    Binary binary;
    //    binary.data = data;
    //    binary.size = size;
    //    binary.id = id;
    //    binary_map_[name] = binary;
    //}

    void clear() {
        binary_map_.clear();
    }

 public:
    std::map<std::string, BinaryPtr> binary_map_;
};


} // namespace knowhere
} // namespace zilliz
