#include "segcore/default_fs.h"

#include <stdexcept>

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
        throw std::runtime_error(
            "GetDefaultArrowFileSystem: FilesystemCache lookup failed: " +
            result.status().ToString());
    }
    return result.ValueOrDie();
}

}  // namespace milvus::segcore
