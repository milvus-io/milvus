// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.
#pragma once

#include "common/AnnFusingPlugin.h"
#include <atomic>
#include <mutex>
#include <string>

namespace milvus::exec {

// Startup-configured, process-owned immutable native policy. No query-time
// loading or replacement: callbacks remain valid for all in-flight searches.
class AnnFusingPolicy final {
 public:
    static const AnnFusingPolicy&
    Instance();

    // Called by initcore before serving queries. Empty path is a normal OSS
    // deployment. Repeated identical initialization is harmless; changing the
    // configuration requires a process restart.
    static bool
    Initialize(const char* library_path, const char* config_path);

    bool
    available() const {
        return ready_.load(std::memory_order_acquire);
    }

    bool
    Consider(const MilvusAnnFusingRuleV4& request) const;

    bool
    Choose(const MilvusAnnFusingSampleV4& request) const;

 private:
    AnnFusingPolicy() = default;
    static AnnFusingPolicy&
    MutableInstance();
    bool
    Load(const char* library_path, const char* config_path);
    ~AnnFusingPolicy();
    AnnFusingPolicy(const AnnFusingPolicy&) = delete;
    AnnFusingPolicy&
    operator=(const AnnFusingPolicy&) = delete;
    void* library_{nullptr};
    MilvusAnnFusingPluginV4 api_{};
    std::once_flag init_once_;
    std::atomic<bool> ready_{false};
    std::string library_path_;
    std::string config_path_;
    bool initialized_ok_{false};
};

}  // namespace milvus::exec
