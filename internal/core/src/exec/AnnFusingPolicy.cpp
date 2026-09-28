// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.
#include "exec/AnnFusingPolicy.h"

#include <dlfcn.h>

#include "log/Log.h"

namespace milvus::exec {

const AnnFusingPolicy&
AnnFusingPolicy::Instance() {
    return MutableInstance();
}

AnnFusingPolicy&
AnnFusingPolicy::MutableInstance() {
    static AnnFusingPolicy policy;
    return policy;
}

bool
AnnFusingPolicy::Initialize(const char* path, const char* config) {
    if (!path || !config) {
        return false;
    }
    auto& policy = MutableInstance();
    std::call_once(policy.init_once_, [&] {
        policy.library_path_ = path;
        policy.config_path_ = config;
        policy.initialized_ok_ = policy.Load(path, config);
    });
    return policy.initialized_ok_ && policy.library_path_ == path &&
           policy.config_path_ == config;
}

bool
AnnFusingPolicy::Load(const char* path, const char* config) {
    if (*path == '\0') {
        LOG_INFO("ann_fusing policy not configured; AUTO keeps baseline");
        return true;
    }
    library_ = dlopen(path, RTLD_NOW | RTLD_LOCAL);
    if (!library_) {
        LOG_WARN("ann_fusing configured policy load failed path={} error={}; "
                 "AUTO keeps baseline", path, dlerror());
        return false;
    }
    auto create = reinterpret_cast<MilvusCreateAnnFusingPluginV4Fn>(
        dlsym(library_, "MilvusCreateAnnFusingPluginV4"));
    MilvusAnnFusingPluginV4 candidate{};
    if (!create || !create(config, sizeof(candidate), &candidate) ||
        candidate.abi_major != 4 ||
        candidate.struct_size != sizeof(candidate) || !candidate.context ||
        !candidate.consider || !candidate.choose || !candidate.destroy) {
        // A conforming factory leaves output untouched on failure. Do not
        // dereference callbacks from an incompatible layout.
        dlclose(library_);
        library_ = nullptr;
        LOG_WARN("ann_fusing configured policy rejected path={} config={}; "
                 "check ABI and YAML; AUTO keeps baseline", path, config);
        return false;
    }
    api_ = candidate;
    ready_.store(true, std::memory_order_release);
    LOG_INFO("ann_fusing native policy loaded abi=4 path={} config={}",
             path,
             config);
    return true;
}

AnnFusingPolicy::~AnnFusingPolicy() {
    if (api_.context) {
        api_.destroy(api_.context);
    }
    if (library_) {
        dlclose(library_);
    }
}

bool
AnnFusingPolicy::Consider(const MilvusAnnFusingRuleV4& request) const {
    return available() && api_.consider(api_.context, &request);
}

bool
AnnFusingPolicy::Choose(const MilvusAnnFusingSampleV4& request) const {
    return available() && api_.choose(api_.context, &request);
}

}  // namespace milvus::exec
