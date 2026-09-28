// Licensed under the Apache License, Version 2.0.
#pragma once

namespace milvus::index {

enum class ScalarIndexType {
    UNKNOWN = -1,
    NONE = 0,
    BITMAP,
    STLSORT,
    MARISA,
    INVERTED,
    HYBRID,
    JSONSTATS,
    RTREE,
    NGRAM,
    FMINDEX,
};

}  // namespace milvus::index
