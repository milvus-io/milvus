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
        }

        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

void
CleanArrowFileSystem() {
    milvus_storage::FilesystemCache::getInstance().clean();
}
