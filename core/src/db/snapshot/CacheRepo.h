// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <map>
#include <memory>
#include <shared_mutex>
#include <sstream>
#include "utils/Status.h"

namespace milvus {
namespace engine {
namespace snapshot {

template <typename T, typename IndexT, typename KeyT>
class CacheRepo {
 public:
    using RepoT = CacheRepo<T, IndexT, KeyT>;
    using RepoPtr = std::shared_ptr<RepoT>;

    static RepoPtr
    MutableRepo(const IndexT& index);
    static void
    Clear();
    static void
    Clear(const IndexT& index);
    static size_t
    IndexSize();

    void
    Cache(const KeyT& key, const T& data);

    Status
    MutableData(const KeyT& key, T& data) const;
    size_t
    KeyCount() const;
    const IndexT&
    Index() const;

    virtual ~CacheRepo();

 private:
    using Reposity = std::map<IndexT, RepoPtr>;
    using DataReposity = std::map<KeyT, T>;

    CacheRepo() = delete;
    explicit CacheRepo(const IndexT& index) : index_(index) {
    }

    DataReposity data_repo_;
    mutable std::shared_timed_mutex data_mutex_;
    IndexT index_;

    static Reposity root_repo_;
    static std::shared_timed_mutex index_mutex_;
};

template <typename T, typename IndexT, typename KeyT>
typename CacheRepo<T, IndexT, KeyT>::Reposity CacheRepo<T, IndexT, KeyT>::root_repo_;

template <typename T, typename IndexT, typename KeyT>
std::shared_timed_mutex CacheRepo<T, IndexT, KeyT>::index_mutex_;

template <typename T, typename IndexT, typename KeyT>
typename CacheRepo<T, IndexT, KeyT>::RepoPtr
CacheRepo<T, IndexT, KeyT>::MutableRepo(const IndexT& index) {
    std::unique_lock<std::shared_timed_mutex> lock(index_mutex_);
    RepoPtr repo = nullptr;
    auto it = root_repo_.find(index);
    if (it == root_repo_.end()) {
        auto new_repo = new RepoT(index);
        repo.reset(new_repo);
        root_repo_[index] = repo;
        /* std::cout << "Register new repo to index [" << index << "]" << std::endl; */
    } else {
        repo = it->second;
        /* std::cout << "Find repo at index [" << index << "]" << std::endl; */
    }

    return repo;
}

template <typename T, typename IndexT, typename KeyT>
size_t
CacheRepo<T, IndexT, KeyT>::IndexSize() {
    std::shared_lock<std::shared_timed_mutex> lock(index_mutex_);
    return root_repo_.size();
}

template <typename T, typename IndexT, typename KeyT>
const IndexT&
CacheRepo<T, IndexT, KeyT>::Index() const {
    return index_;
}

template <typename T, typename IndexT, typename KeyT>
void
CacheRepo<T, IndexT, KeyT>::Clear() {
    std::unique_lock<std::shared_timed_mutex> lock(index_mutex_);
    root_repo_.clear();
}

template <typename T, typename IndexT, typename KeyT>
void
CacheRepo<T, IndexT, KeyT>::Clear(const IndexT& index) {
    std::unique_lock<std::shared_timed_mutex> lock(index_mutex_);

    auto it = root_repo_.find(index);
    if (it != root_repo_.end()) {
        root_repo_.erase(it);
    }
}

template <typename T, typename IndexT, typename KeyT>
void
CacheRepo<T, IndexT, KeyT>::Cache(const KeyT& key, const T& data) {
    std::unique_lock<std::shared_timed_mutex> lock(data_mutex_);
    data_repo_[key] = data;
}

template <typename T, typename IndexT, typename KeyT>
Status
CacheRepo<T, IndexT, KeyT>::MutableData(const KeyT& key, T& data) const {
    std::unique_lock<std::shared_timed_mutex> lock(data_mutex_);

    auto it = data_repo_.find(key);
    if (it == data_repo_.end()) {
        std::stringstream errstream;
        errstream << "Error: fail to retrieve to cache of key " << key << "." << std::endl
                  << __PRETTY_FUNCTION__ << std::endl;
        return Status(SS_NOT_FOUND_ERROR, errstream.str());
    }

    data = const_cast<T&>(it->second);
    return Status::OK();
}

template <typename T, typename IndexT, typename KeyT>
size_t
CacheRepo<T, IndexT, KeyT>::KeyCount() const {
    std::shared_lock<std::shared_timed_mutex> lock(data_mutex_);
    return data_repo_.size();
}

template <typename T, typename IndexT, typename KeyT>
CacheRepo<T, IndexT, KeyT>::~CacheRepo() {
    data_repo_.clear();
}

}  // namespace snapshot
}  // namespace engine
}  // namespace milvus
