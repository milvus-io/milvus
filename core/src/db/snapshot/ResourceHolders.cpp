#include "ResourceHolders.h"
#include "Store.h"
#include <sstream>
#include <iostream>

CollectionsHolder::ResourcePtr
CollectionsHolder::Load(const std::string& name) {
    auto& store = Store::GetInstance();
    auto c = store.GetCollection(name);
    if (c) {
        BaseT::AddNoLock(c);
        return c;
    }
    return nullptr;
}

CollectionsHolder::ScopedT
CollectionsHolder::GetCollection(const std::string& name, bool scoped) {
    std::unique_lock<std::mutex> lock(BaseT::mutex_);
    auto cit = name_map_.find(name);
    if (cit == name_map_.end()) {
        auto ret = Load(name);
        if (!ret) return BaseT::ScopedT();
        return BaseT::ScopedT(ret, scoped);
    }
    return BaseT::ScopedT(cit->second, scoped);
}

bool CollectionsHolder::Add(CollectionsHolder::ResourcePtr resource) {
    if (!resource) return false;
    std::unique_lock<std::mutex> lock(BaseT::mutex_);
    return BaseT::AddNoLock(resource);
}

bool CollectionsHolder::Release(const std::string& name) {
    std::unique_lock<std::mutex> lock(BaseT::mutex_);
    auto it = name_map_.find(name);
    if (it == name_map_.end()) {
        return false;
    }

    BaseT::id_map_.erase(it->second->GetID());
    name_map_.erase(it);
    return true;
}

bool CollectionsHolder::Release(ID_TYPE id) {
    std::unique_lock<std::mutex> lock(mutex_);
    auto it = id_map_.find(id);
    if (it == id_map_.end()) {
        return false;
    }

    BaseT::id_map_.erase(it);
    name_map_.erase(it->second->GetName());
    return true;
}
