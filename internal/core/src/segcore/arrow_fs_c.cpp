#include "segcore/arrow_fs_c.h"

#include <exception>
#include <string>

#include "common/EasyAssert.h"
#include "common/type_c.h"
#include "milvus-storage/filesystem/fs.h"
#include "storage/loon_ffi/property_singleton.h"

CStatus
InitArrowFileSystem(CStorageConfig c_storage_config) {
    try {
        milvus::storage::LoonFFIPropertiesSingleton::GetInstance().Init(
            c_storage_config);

        auto props = milvus::storage::LoonFFIPropertiesSingleton::GetInstance()
                         .GetProperties();
        if (props) {
            auto result =
                milvus_storage::FilesystemCache::getInstance().get(*props, "");
            if (!result.ok()) {
                throw std::runtime_error(
                    "Failed to populate FilesystemCache: " +
                    result.status().ToString());
            }

            auto config_result =
                milvus_storage::FilesystemCache::resolve_config(*props, "");
            if (!config_result.ok()) {
                throw std::runtime_error(
                    "Failed to resolve filesystem config: " +
                    config_result.status().ToString());
            }
            milvus_storage::ArrowFileSystemSingleton::GetInstance().Init(
                config_result.ValueOrDie());
        }

        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

void
CleanArrowFileSystem() {
    milvus_storage::ArrowFileSystemSingleton::GetInstance().Release();
    milvus_storage::FilesystemCache::getInstance().clean();
}
