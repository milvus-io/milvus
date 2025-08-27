// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <dlfcn.h>
#include <map>
#include <memory>
#include <mutex>
#include "log/Log.h"
#include "storage/plugin/PluginInterface.h"
#include "common/EasyAssert.h"
#include "common/Exception.h"

namespace milvus::storage {

class PluginLoader {
 public:
    // Delete copy constructor and assignment operator to enforce singleton behavior
    PluginLoader(const PluginLoader&) = delete;
    PluginLoader&
    operator=(const PluginLoader&) = delete;

    static PluginLoader&
    GetInstance() {
        static PluginLoader instance;
        return instance;
    }

    ~PluginLoader() {
        unloadAll();
    }

    void
    load(const std::string& path) {
        std::lock_guard<std::mutex> lock(mutex_);
        void* handle = dlopen(path.c_str(), RTLD_LAZY);
        // void *handle = dlopen(path.c_str(), RTLD_LAZY | RTLD_DEEPBIND);
        if (!handle) {
            const char* error = dlerror();
            ThrowInfo(
                UnexpectedError,
                fmt::format("Failed to load plugin: {}, err={}", path, error));
        }

        // Rest error flags
        dlerror();

        using IPluginPtr = milvus::storage::plugin::IPlugin* (*)();
        auto createPluginFunc =
            reinterpret_cast<IPluginPtr>(dlsym(handle, "CreatePlugin"));

        const char* error = dlerror();
        if (error) {
            dlclose(handle);
            ThrowInfo(UnexpectedError,
                      fmt::format("Failed to load plugin: {}", error));
        }

        error = dlerror();
        auto pluginPtr = createPluginFunc();
        if (!pluginPtr) {
            dlclose(handle);
            ThrowInfo(
                UnexpectedError,
                fmt::format("Failed to init plugin: {}, {}", path, error));
        }

        std::string pluginName = pluginPtr->getPluginName();
        if (plugins_.find(pluginName) != plugins_.end()) {
            LOG_DEBUG("Plugin with name {} is already loaded.", pluginName);
            dlclose(handle);
            return;
        }

        // Store the plugin and its handle
        plugins_[pluginName] =
            std::shared_ptr<milvus::storage::plugin::IPlugin>(pluginPtr);
        handles_[pluginName] = handle;
        LOG_INFO("Loaded plugin: {}", pluginName);
    }

    void
    unloadAll() {
        std::lock_guard<std::mutex> lock(mutex_);
        plugins_.clear();
        for (auto& handle : handles_) {
            dlclose(handle.second);
        }
        handles_.clear();
    }

    std::shared_ptr<milvus::storage::plugin::ICipherPlugin>
    getCipherPlugin() {
        auto p = getPlugin("CipherPlugin");
        if (!p) {
            return nullptr;
        }
        return std::dynamic_pointer_cast<
            milvus::storage::plugin::ICipherPlugin>(p);
    }

    std::shared_ptr<milvus::storage::plugin::IPlugin>
    getPlugin(const std::string& name) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = plugins_.find(name);
        return it != plugins_.end() ? it->second : nullptr;
    }

    std::vector<std::string>
    listPlugins() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<std::string> names;
        for (const auto& pair : plugins_) {
            names.push_back(pair.first);
        }
        return names;
    }

    void
    unload(const std::string& name) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = plugins_.find(name);
        if (it != plugins_.end()) {
            plugins_.erase(it);
        }

        auto handleIt = handles_.find(name);
        if (handleIt != handles_.end()) {
            dlclose(handleIt->second);
            handles_.erase(handleIt);
        }
    }

 private:
    PluginLoader() {
    }

    mutable std::mutex mutex_;
    std::map<std::string, void*> handles_;
    std::map<std::string, std::shared_ptr<milvus::storage::plugin::IPlugin>>
        plugins_;
};

}  // namespace milvus::storage
