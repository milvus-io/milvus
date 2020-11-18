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
#include <string>
#include "utils/Status.h"

namespace milvus {
namespace engine {
namespace snapshot {

/**
 * A two-level cache repository
 *
 * Manages caching for T objects
 * The repository can be divided into sub-repositories by IndexT, and each sub-repo organizes
 * the T objects in format of Key-Value.
 */
template <typename T, typename IndexT, typename KeyT>
class CacheRepo {
 public:
    using RepoT = CacheRepo<T, IndexT, KeyT>;
    using RepoPtr = std::shared_ptr<RepoT>;

    /*
     * Get a mutable sub-repositories of specified index.
     * If there is no specified repo, create a new one and return the new repo
     * If there is a specified repo, return the repo
     */
    static RepoPtr
    MutableRepo(const IndexT& index);
    // Clear all managed repos
    static void
    Clear();
    // Clear the specified repo
    static void
    Clear(const IndexT& index);
    // Return the the number of managed repos
    static size_t
    IndexSize();
    /*
     * Helper function to dump all repos info as string
     * @param index_only == true means not show repo info in detail
     */
    static std::string
    ToString(bool index_only = true);

    /*
     * Cache data in repo with associated key
     */
    void
    Cache(const KeyT& key, const T& data);

    /*
     * Get a mutable data from repo
     * @param key Specify the key
     * @param data Store the mutable data
     * @returns: Return Status(SS_NOT_FOUND_ERROR) if specified key not found. Else return Status::OK
     */
    Status
    MutableData(const KeyT& key, T& data) const;
    // Get the count of keys in repo
    size_t
    KeyCount() const;

    // Get the immutable index of the repo
    const IndexT&
    Index() const;
    /*
     * Helper function to dump the repo info as string
     * @param meta_only Config wether or not dump meta data only
     * @param start_indent Config the start indent
     */
    std::string
    ObjectToString(bool meta_only = true, const std::string& start_indent = "") const;

    /*
     * Destructor
     */
    virtual ~CacheRepo();

 private:
    using Repository = std::map<IndexT, RepoPtr>;
    using DataRepository = std::map<KeyT, T>;

    CacheRepo() = delete;
    explicit CacheRepo(const IndexT& index) : index_(index) {
    }

    // data container
    DataRepository data_repo_;
    // concurrent operation control on data repo
    mutable std::shared_timed_mutex data_mutex_;
    // current index
    IndexT index_;

    // repo container
    static Repository root_repo_;
    // concurrent operation control on repos
    static std::shared_timed_mutex index_mutex_;
};

template <typename T, typename IndexT, typename KeyT>
typename CacheRepo<T, IndexT, KeyT>::Repository CacheRepo<T, IndexT, KeyT>::root_repo_;

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
std::string
CacheRepo<T, IndexT, KeyT>::ToString(bool index_only) {
    std::stringstream ss;
    std::string indent = "  ";
    ss << "CacheRepo<T=" << typeid(T).name() << ", ";
    ss << "IndexT=" << typeid(IndexT).name() << ", ";
    ss << "KeyT=" << typeid(KeyT).name() << "> {\n";
    ss << indent << "IndexSize=" << IndexSize() << ",\n";
    ss << indent << "Repos={\n";
    std::string blk_indent = indent + indent;
    std::string inner_indent = blk_indent + indent;
    std::string repo_indent = inner_indent + indent;
    std::string repo_blk_indent = repo_indent + indent;
    std::shared_lock<std::shared_timed_mutex> lock(index_mutex_);

    for (auto& [index, repo] : root_repo_) {
        ss << repo->ObjectToString(index_only, blk_indent) << ",\n";
    }
    ss << indent << "}\n";
    ss << "}";
    return ss.str();
}

template <typename T, typename IndexT, typename KeyT>
std::string
CacheRepo<T, IndexT, KeyT>::ObjectToString(bool meta_only, const std::string& start_indent) const {
    std::string indent = "  ";
    std::string inner_indent = start_indent + indent;
    std::string repo_indent = inner_indent + indent;
    std::string repo_blk_indent = repo_indent + indent;
    std::stringstream ss;
    ss << start_indent << "{\n";

    ss << inner_indent << "Index=" << index_ << ", \n";
    ss << inner_indent << "KeyCount=" << KeyCount() << ",\n";
    if (meta_only) {
        ss << start_indent << "}\n";
        return ss.str();
    }
    ss << inner_indent << "Repo={\n";
    for (auto& [key, data] : data_repo_) {
        ss << repo_indent << "{\n";
        ss << repo_blk_indent << "Key=" << key << "\n";
        ss << repo_indent << "},\n";
    }
    ss << inner_indent << "}\n";

    ss << start_indent << "}";

    return ss.str();
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
