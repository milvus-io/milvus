#include "segcore/default_fs.h"

#include <stdexcept>

#include "common/EasyAssert.h"
#include "milvus-storage/common/extend_status.h"
#include "milvus-storage/filesystem/fs.h"
#include "storage/loon_ffi/property_singleton.h"

namespace milvus::segcore {

milvus_storage::ArrowFileSystemPtr
GetDefaultArrowFileSystem() {
    auto props =
        storage::LoonFFIPropertiesSingleton::GetInstance().GetProperties();
    if (!props) {
        throw std::runtime_error(
            "GetDefaultArrowFileSystem: LoonFFIPropertiesSingleton not "
            "initialized");
    }
    auto result =
        milvus_storage::FilesystemCache::getInstance().get(*props, "");
    if (!result.ok()) {
        auto error = milvus_storage::ToSegcoreError(result.status());
        ThrowInfo(error.get_error_code(),
                  "GetDefaultArrowFileSystem: FilesystemCache lookup failed: "
                  "{}",
                  error.what());
    }
    return result.ValueOrDie();
}

}  // namespace milvus::segcore
