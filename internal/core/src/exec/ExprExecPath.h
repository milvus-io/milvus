// Licensed under the Apache License, Version 2.0.
#pragma once

namespace milvus::exec {

enum class ExprExecPath {
    Unknown = -1,
    RawData = 0,  // brute-force scan raw data
    ScalarIndex,  // pinned_index_ scalar index
    PkIndex,      // segment_->pk_range / search_ids
    TextIndex,    // segment_->GetTextIndex
    JsonStats,    // segment_->GetJsonStats
};

}  // namespace milvus::exec
