// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.
#pragma once

#include <stdexcept>

namespace milvus {
// Only a pre-search dense-bitmap requirement may trigger materialization.
// Never use this for callback, cancellation, storage or generic search errors.
class DeferredFilterUnsupported : public std::runtime_error {
 public:
    using std::runtime_error::runtime_error;
};
}  // namespace milvus
