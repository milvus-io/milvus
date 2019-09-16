////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////


namespace zilliz {
namespace milvus {
namespace cache {

constexpr double DEFAULT_THRESHHOLD_PERCENT = 0.85;

template<typename ItemObj>
Cache<ItemObj>::Cache(int64_t capacity, uint64_t cache_max_count)
    : usage_(0),
      capacity_(capacity),
      freemem_percent_(DEFAULT_THRESHHOLD_PERCENT),
      lru_(cache_max_count) {
//    AGENT_LOG_DEBUG << "Construct Cache with capacity " << std::to_string(mem_capacity)
}

template<typename ItemObj>
void Cache<ItemObj>::set_capacity(int64_t capacity) {
    if(capacity > 0) {
        capacity_ = capacity;
        free_memory();
    }
}

template<typename ItemObj>
size_t Cache<ItemObj>::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return lru_.size();
}

template<typename ItemObj>
bool Cache<ItemObj>::exists(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    return lru_.exists(key);
}

template<typename ItemObj>
ItemObj Cache<ItemObj>::get(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    if(!lru_.exists(key)){
        return nullptr;
    }

    return lru_.get(key);
}

template<typename ItemObj>
void Cache<ItemObj>::insert(const std::string& key, const ItemObj& item) {
    if(item == nullptr) {
        return;
    }

    if(item->size() > capacity_) {
        SERVER_LOG_ERROR << "Item size " << item->size()
                        << " is too large to insert into cache, capacity " << capacity_;
        return;
    }

    //calculate usage
    {
        std::lock_guard<std::mutex> lock(mutex_);

        //if key already exist, subtract old item size
        if (lru_.exists(key)) {
            const ItemObj& old_item = lru_.get(key);
            usage_ -= old_item->size();
        }

        //plus new item size
        usage_ += item->size();
    }

    //if usage exceed capacity, free some items
    if (usage_ > capacity_) {
        SERVER_LOG_DEBUG << "Current usage " << usage_
                         << " exceeds cache capacity " << capacity_
                         << ", start free memory";
        free_memory();
    }

    //insert new item
    {
        std::lock_guard<std::mutex> lock(mutex_);

        lru_.put(key, item);
        SERVER_LOG_DEBUG << "Insert " << key << " size:" << item->size()
                         << " bytes into cache, usage: " << usage_ << " bytes";
    }
}

template<typename ItemObj>
void Cache<ItemObj>::erase(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    if(!lru_.exists(key)){
        return;
    }

    const ItemObj& old_item = lru_.get(key);
    usage_ -= old_item->size();

    SERVER_LOG_DEBUG << "Erase " << key << " size: " << old_item->size();

    lru_.erase(key);
}

template<typename ItemObj>
void Cache<ItemObj>::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    lru_.clear();
    usage_ = 0;
    SERVER_LOG_DEBUG << "Clear cache !";
}

/* free memory space when CACHE occupation exceed its capacity */
template<typename ItemObj>
void Cache<ItemObj>::free_memory() {
    if (usage_ <= capacity_) return;

    int64_t threshhold = capacity_ * freemem_percent_;
    int64_t delta_size = usage_ - threshhold;
    if(delta_size <= 0) {
        delta_size = 1;//ensure at least one item erased
    }

    std::set<std::string> key_array;
    int64_t released_size = 0;

    {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = lru_.rbegin();
        while (it != lru_.rend() && released_size < delta_size) {
            auto& key = it->first;
            auto& obj_ptr = it->second;

            key_array.emplace(key);
            released_size += obj_ptr->size();
            ++it;
        }
    }

    SERVER_LOG_DEBUG << "to be released memory size: " << released_size;

    for (auto& key : key_array) {
        erase(key);
    }

    print();
}

template<typename ItemObj>
void Cache<ItemObj>::print() {
    size_t cache_count = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        cache_count = lru_.size();
    }

    SERVER_LOG_DEBUG << "[Cache item count]: " << cache_count;
    SERVER_LOG_DEBUG << "[Cache usage]: " << usage_ << " bytes";
    SERVER_LOG_DEBUG << "[Cache capacity]: " << capacity_ << " bytes";
}

}   // cache
}   // milvus
}   // zilliz

