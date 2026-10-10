#include "segcore/storagev1translator/IndexReaderTranslator.h"

#include "common/EasyAssert.h"
#include "storage/LocalChunkManagerSingleton.h"

namespace milvus::segcore::storagev1translator {

const index::IndexFamily&
IndexReaderTranslator::Family() const noexcept {
    return family_;
}

DataType
IndexReaderTranslator::ValueType() const noexcept {
    return value_type_;
}

const index::ReaderCaps&
IndexReaderTranslator::Caps() const noexcept {
    return caps_;
}

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

}  // namespace milvus::segcore::storagev1translator
