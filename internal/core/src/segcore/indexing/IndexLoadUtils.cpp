#include "segcore/indexing/IndexLoadUtils.h"

#include "common/EasyAssert.h"
#include "storage/LocalChunkManagerSingleton.h"

namespace milvus::segcore {

std::string
LocalStagingRoot(const std::string& configured_root) {
    if (!configured_root.empty()) {
        return configured_root;
    }
    auto local =
        storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    AssertInfo(local != nullptr,
               "local chunk manager is not initialized for index staging");
    return local->GetRootPath();
}

}  // namespace milvus::segcore
