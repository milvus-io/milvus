#pragma once

#include <dlfcn.h>
#include <memory>
#include "storage/plugin/PluginInterface.h"
#include "common/EasyAssert.h"
#include "common/Exception.h"

namespace  milvus::storage{

class PluginLoader{
public:
    // Delete copy constructor and assignment operator to enforce singleton behavior
    PluginLoader(const PluginLoader&) = delete;
    PluginLoader& operator=(const PluginLoader&) = delete;

    static PluginLoader& GetInstance() {
        static PluginLoader instance;
        return instance;
    }

    ~PluginLoader(){ unloadAll(); }

    bool
    load(const std::string& path) {
        void *handle = dlopen(path.c_str(), RTLD_LAZY);
        if (!handle) {
            dlclose(handle);
            PanicInfo(PluginLoadFailed,
                      fmt::format("Failed to load plugin: {}", path));
        }

        // Rest error flags
        dlerror();

        auto createPlugin = reinterpret_cast<std::unique_ptr<milvus::storage::plugin::IPlugin> (*)()>(dlsym(handle, "CreatePlugin"));
        const char* error =dlerror();
        if (error) {
            dlclose(handle);
            PanicInfo(PluginLoadFailed,
                      fmt::format("Failed to load plugin: {}", error));
            return false;
        }

        auto plugin = createPlugin();
        if (!plugin) {
            dlclose(handle);
            PanicInfo(PluginLoadFailed,
                      fmt::format("Failed to init plugin: {}", path));
            return false;
        }

        std::string pluginName = plugin->getPluginName();
        if (plugins_.find(pluginName) != plugins_.end()) {
            std::cerr << "Plugin with name " << pluginName << " is already loaded." << std::endl;
            dlclose(handle);
            return false;
        }

        // Store the plugin and its handle
        plugins_[pluginName] = std::move(plugin);
        handles_[pluginName] = handle;
        return true;
    }

    void
    unloadAll() {
        plugins_.clear();
        for (auto& handle : handles_) {
            dlclose(handle.second);
        }
        handles_.clear();
    }

    std::shared_ptr<milvus::storage::plugin::ICipherPlugin>
    getCipherPlugin(){
        auto p = getPlugin("cipher");
        if (!p) {
            return nullptr;
        }
        return std::dynamic_pointer_cast<milvus::storage::plugin::ICipherPlugin>(p);
    }

    std::shared_ptr<milvus::storage::plugin::IPlugin>
    getPlugin(const std::string& name) {
        auto it = plugins_.find(name);
        return it != plugins_.end() ? it->second : nullptr;
    }

    std::vector<std::string>
    listPlugins() const {
        std::vector<std::string> names;
        for (const auto& pair : plugins_) {
            names.push_back(pair.first);
        }
        return names;
    }

    void
    unloadAll(const std::string& name) {
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
    PluginLoader() {}

    std::map<std::string, void*> handles_;
    std::map<std::string, std::shared_ptr<milvus::storage::plugin::IPlugin>> plugins_;
};

} // namespace milvus::storage
