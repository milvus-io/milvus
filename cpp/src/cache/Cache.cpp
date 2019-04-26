////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "Cache.h"
#include "utils/Log.h"

#include <set>

namespace zilliz {
namespace vecwise {
namespace cache {

Cache::Cache(int64_t capacity, uint64_t cache_max_count)
    : usage_(0),
      capacity_(capacity),
      lru_(cache_max_count) {
//    AGENT_LOG_DEBUG << "Construct Cache with capacity " << std::to_string(mem_capacity)
}

void Cache::set_capacity(int64_t capacity) {
    if(capacity > 0) {
        capacity_ = capacity;
        free_memory();
    }
}

size_t Cache::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return lru_.size();
}

bool Cache::exists(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    return lru_.exists(key);
}

DataObjPtr Cache::get(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    if(!lru_.exists(key)){
        return nullptr;
    }

    const CacheObjPtr& cache_obj = lru_.get(key);
    return cache_obj->data_;
}

void Cache::insert(const std::string& key, const DataObjPtr& data_ptr) {
    {
        std::lock_guard<std::mutex> lock(mutex_);

        /* if key already exist, over-write old data */
        if (lru_.exists(key)) {
            CacheObjPtr obj_ptr = lru_.get(key);

            usage_ -= obj_ptr->data_->size();
            obj_ptr->data_ = data_ptr;
            usage_ += data_ptr->size();
        } else {
            CacheObjPtr obj_ptr(new CacheObj(data_ptr));
            lru_.put(key, obj_ptr);
            usage_ += data_ptr->size();
        }

//        AGENT_LOG_DEBUG << "Insert into LRU(" << (capacity_ > 0 ? std::to_string(usage_ * 100 / capacity_) : "Nan")
//                        << "%, +" << data_ptr->size() << ", " << usage_ << ", " << lru_.size() << "):"
//                        << " " << key;
    }

    if (usage_ > capacity_) {
//        AGENT_LOG_TRACE << "Current usage " << usage_
//                        << " exceeds cache capacity " << capacity_
//                        << ", start free memory";
        free_memory();
    }
}

void Cache::erase(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    if(!lru_.exists(key)){
        return;
    }

    const CacheObjPtr& obj_ptr = lru_.get(key);
    const DataObjPtr& data_ptr = obj_ptr->data_;
    usage_ -= data_ptr->size();
//    AGENT_LOG_DEBUG << "Erase from LRU(" << (capacity_ > 0 ? std::to_string(usage_*100/capacity_) : "Nan")
//                    << "%, -" << data_ptr->size() << ", " << usage_ << ", " << lru_.size() << "): "
//                    << (data_ptr->flags().get_flag(DataObjAttr::kPinned) ? "Pinned " : "")
//                    << (data_ptr->flags().get_flag(DataObjAttr::kValid) ? "Valid " : "")
//                    << "(ref:" << obj_ptr->ref_ << ") "
//                    << key;
    lru_.erase(key);
}

void Cache::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    lru_.clear();
    usage_ = 0;
//    AGENT_LOG_DEBUG << "Clear LRU !";
}

#if 0 /* caiyd 20190221, need more testing before enable */
void Cache::flush_to_file(const std::string& key, const CacheObjPtr& obj_ptr) {
    if (!this->swap_enabled_) return;

    const DataObjPtr data_ptr = obj_ptr->data();

    if (data_ptr == nullptr || data_ptr->size() == 0) return;
    if (data_ptr->ptr() == nullptr) return;

    std::string name = std::to_string(reinterpret_cast<int64_t>(data_ptr.get()));
    filesys::CreateDirectory(this->swap_path_);

    /* write cache data to file */
    obj_ptr->set_file_path(this->swap_path_ + "/" + name);
    std::shared_ptr<arrow::io::OutputStream> outfile = nullptr;
    filesys::OpenWritableFile(obj_ptr->file_path(), false, &outfile);
    filesys::WriteFile(outfile, data_ptr->ptr().get(), data_ptr->size());
    (void)outfile->Close();

    AGENT_LOG_DEBUG << "Flush cache data: " << key << ", to file: " << obj_ptr->file_path();

    /* free cache memory */
    data_ptr->ptr().reset();
    usage_ -= data_ptr->size();
}

void Cache::restore_from_file(const std::string& key, const CacheObjPtr& obj_ptr) {
    if (!this->swap_enabled_) return;

    const DataObjPtr data_ptr = obj_ptr->data();
    if (data_ptr == nullptr || data_ptr->size() == 0) return;

    std::shared_ptr<arrow::io::RandomAccessFile> infile = nullptr;
    int64_t file_size, bytes_read;

    /* load cache data from file */
    if (!filesys::FileExist(obj_ptr->file_path())) {
        THROW_AGENT_UNEXPECTED_ERROR("File not exist: " + obj_ptr->file_path());
    }
    filesys::OpenReadableFile(obj_ptr->file_path(), &infile);
    infile->GetSize(&file_size);
    if (data_ptr->size() != file_size) {
        THROW_AGENT_UNEXPECTED_ERROR("File size not match: " + obj_ptr->file_path());
    }
    data_ptr->set_ptr(lib::gpu::MakeShared<char>(data_ptr->size(), lib::gpu::MallocHint::kUnifiedGlobal));
    infile->Read(file_size, &bytes_read, data_ptr->ptr().get());
    infile->Close();

    AGENT_LOG_DEBUG << "Restore cache data: " << key << ", from file: " << obj_ptr->file_path();

    /* clear file path */
    obj_ptr->set_file_path("");
    usage_ += data_ptr->size();
}
#endif

/* free memory space when CACHE occupation exceed its capacity */
void Cache::free_memory() {
    if (usage_ <= capacity_) return;

    int64_t threshhold = capacity_ * THRESHHOLD_PERCENT;
    int64_t delta_size = usage_ - threshhold;

    std::set<std::string> key_array;
    int64_t released_size = 0;

    {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = lru_.rbegin();
        while (it != lru_.rend() && released_size < delta_size) {
            auto& key = it->first;
            auto& obj_ptr = it->second;
            const auto& data_ptr = obj_ptr->data_;

            key_array.emplace(key);
            released_size += data_ptr->size();
            ++it;
        }
    }

//    AGENT_LOG_DEBUG << "to be released memory size: " << released_size;

    for (auto& key : key_array) {
        erase(key);
    }

    print();
}

void Cache::print() {
    int64_t still_pinned_count = 0;
    int64_t total_pinned_size = 0;
    int64_t total_valid_empty_size = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);

        for (auto it = lru_.begin(); it != lru_.end(); ++it) {
            auto& obj_ptr = it->second;
            const auto& data_ptr = obj_ptr->data_;
            if (data_ptr != nullptr) {
                total_pinned_size += data_ptr->size();
                ++still_pinned_count;
            } else {
                total_valid_empty_size += data_ptr->size();
            }
        }
    }

    SERVER_LOG_DEBUG << "[Still Pinned count]: " << still_pinned_count;
    SERVER_LOG_DEBUG << "[Pinned Memory total size(byte)]: " << total_pinned_size;
    SERVER_LOG_DEBUG << "[valid_empty total size(byte)]: " << total_valid_empty_size;
    SERVER_LOG_DEBUG << "[free memory size(byte)]: " << capacity_ - total_pinned_size - total_valid_empty_size;
}

}   // cache
}   // vecwise
}   // zilliz

