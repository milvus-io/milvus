#include "segcore/storagev1translator/IndexReaderTranslator.h"

#include "common/EasyAssert.h"

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

storage::WarmupPolicy
ToStorageWarmup(CacheWarmupPolicy policy) {
    switch (policy) {
        case CacheWarmupPolicy::CacheWarmupPolicy_Disable:
            return storage::WarmupPolicy::Disable;
        case CacheWarmupPolicy::CacheWarmupPolicy_Async:
            return storage::WarmupPolicy::Async;
        case CacheWarmupPolicy::CacheWarmupPolicy_Sync:
            return storage::WarmupPolicy::Sync;
        default:
            AssertInfo(false,
                       "unknown cache warmup policy {}",
                       static_cast<int>(policy));
    }
    return storage::WarmupPolicy::Sync;
}

}  // namespace milvus::segcore::storagev1translator
