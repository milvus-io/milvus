#pragma once

#include "ResourceTypes.h"
#include "ScopedResource.h"
#include <string>
#include <map>
#include <memory>
#include <condition_variable>
#include <mutex>
#include <thread>

template <typename ResourceT, typename Derived>
class ResourceHolder {
public:
    using ResourcePtr = std::shared_ptr<ResourceT>;
    /* using ResourcePtr = typename ResourceT::Ptr; */
    using ScopedT = ScopedResource<ResourceT>;
    using ScopedPtr = std::shared_ptr<ScopedT>;
    using IdMapT = std::map<ID_TYPE, ResourcePtr>;
    using Ptr = std::shared_ptr<Derived>;
    ScopedT GetResource(ID_TYPE id, bool scoped = true);

    bool AddNoLock(ResourcePtr resource);
    bool ReleaseNoLock(ID_TYPE id);

    virtual bool Add(ResourcePtr resource);
    virtual bool Release(ID_TYPE id);
    virtual bool HardDelete(ID_TYPE id);

    static Derived& GetInstance() {
        static Derived holder;
        return holder;
    }

    virtual void Dump(const std::string& tag = "");

protected:
    virtual void OnNoRefCallBack(ResourcePtr resource);

    virtual ResourcePtr Load(ID_TYPE id);
    virtual ResourcePtr Load(const std::string& name);
    ResourceHolder() = default;
    virtual ~ResourceHolder() = default;

    std::mutex mutex_;
    IdMapT id_map_;
};

#include "BaseHolders.inl"
