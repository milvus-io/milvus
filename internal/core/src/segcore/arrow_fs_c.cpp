#include "segcore/arrow_fs_c.h"

#include <exception>
#include <string>

#include "common/EasyAssert.h"
#include "common/type_c.h"
#include "milvus-storage/common/extend_status.h"
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
                auto error = milvus_storage::ToSegcoreError(result.status());
                return milvus::FailureCStatus(&error);
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
