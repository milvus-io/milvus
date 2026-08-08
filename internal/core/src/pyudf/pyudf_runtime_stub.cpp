// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This translation unit intentionally has no Python headers or Python symbols.
// It is selected for MILVUS_ENABLE_PY_UDF=OFF builds to keep the C ABI stable
// without adding a CPython linkage dependency.
#include "pyudf/pyudf_runtime.h"

#include "common/EasyAssert.h"

namespace milvus::pyudf {
namespace {

[[noreturn]] void
ThrowRuntimeUnsupported() {
    ThrowInfo(
        ErrorCode::Unsupported,
        "py_udf: embedded CPython runtime is not compiled into this binary");
}

}  // namespace

bool
RuntimeBuildEnabled() noexcept {
    return false;
}

void
InitializeRuntime() {
    ThrowRuntimeUnsupported();
}

std::unique_ptr<PyUDFResource>
LoadResource(const uint8_t*, uint64_t) {
    ThrowRuntimeUnsupported();
}

}  // namespace milvus::pyudf
