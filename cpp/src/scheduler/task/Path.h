/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <vector>
#include <string>


namespace zilliz {
namespace milvus {
namespace engine {

class Path {
 public:
    Path() = default;

    Path(std::vector<std::string>& path, uint64_t index) : path_(path), index_(index) {}

    void
    push_back(const std::string &str) {
        path_.push_back(str);
    }

    std::vector<std::string>
    Dump() {
        return path_;
    }

    std::string &
    Next() {
        --index_;
        return path_[index_];
    }

    std::string &
    Last() {
        if (!path_.empty()) {
            return path_[0];
        } else {
            std::string str;
            return str;
        }
    }

 public:
    std::string &
    operator[](uint64_t index) {
        return path_[index];
    }

    std::vector<std::string>::iterator begin() { return path_.begin(); }
    std::vector<std::string>::iterator end() { return path_.end(); }

 public:
    std::vector<std::string> path_;
    uint64_t index_ = 0;
};

}
}
}