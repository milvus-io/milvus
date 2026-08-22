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

#pragma once

#include <cstdint>
#include <memory>
#include <stdexcept>

namespace milvus::pyudf {

class PyUDFFunctionError : public std::runtime_error {
 public:
    using std::runtime_error::runtime_error;
};

class PyUDFInvocation;
class PyUDFResult;

// PyUDFResource is deliberately opaque at the C ABI. The enabled runtime owns
// CPython references behind this interface; the disabled implementation never
// needs Python headers or linkage.
class PyUDFResource {
 public:
    PyUDFResource() = default;
    virtual ~PyUDFResource() = default;

    PyUDFResource(const PyUDFResource&) = delete;
    PyUDFResource&
    operator=(const PyUDFResource&) = delete;
    PyUDFResource(PyUDFResource&&) = delete;
    PyUDFResource&
    operator=(PyUDFResource&&) = delete;

    // Run does not own the invocation container. It may move populated Arrow
    // descriptors from the invocation into the returned result. Failures before
    // descriptor movement leave invocation ownership unchanged.
    virtual std::unique_ptr<PyUDFResult>
    Run(PyUDFInvocation& invocation,
        const uint8_t* serialized_params,
        uint64_t serialized_params_len) = 0;

    // Close is idempotent. Implementations must release every owned reference
    // before throwing, even when an optional Python close method fails.
    virtual void
    Close() = 0;
};

bool
RuntimeBuildEnabled() noexcept;

void
InitializeRuntime();

std::unique_ptr<PyUDFResource>
LoadResource(const uint8_t* serialized_request,
             uint64_t serialized_request_len);

}  // namespace milvus::pyudf
