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

#include "index/contracts/Registry.h"

#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include "common/Array.h"
#include "common/EasyAssert.h"
#include "index/contracts/build/ScalarBuildInput.h"
#include "index/contracts/build/VectorBuildInput.h"

// Implementation of the contract-layer registries. Each family registers from
// its own implementation translation unit, so this file has no concrete-family
// implementation dependency. The explicit-instantiation list may name opaque
// family-local input shapes; it neither includes nor calls their family.
//
// STATIC-REGISTRATION HAZARD, stated once here so no family has to repeat it:
// self-registration lives in a namespace-scope object in each family's .cpp.
// That works while every family object file is linked into `milvus_core`
// (today's build: `add_library(milvus_index OBJECT ...)` + `$<TARGET_OBJECTS:>`
// into the shared object, so no archive-member stripping applies). If index/
// ever becomes a static archive, these TUs must be kept with
// `--whole-archive` / `/WHOLEARCHIVE`, or registration silently disappears and
// every `Lookup` returns an empty entry.

namespace milvus::index {

struct JsonProjectedString;

namespace {

template <typename Value>
class RegistryTable {
 public:
    void
    Put(IndexFamily family, Value value) {
        std::lock_guard lock(mu_);
        auto [it, inserted] =
            table_.emplace(std::move(family), std::move(value));
        AssertInfo(inserted,
                   "duplicate index family registration: {}",
                   it->first);
    }

    Value
    Get(const IndexFamily& family) const {
        std::lock_guard lock(mu_);
        auto it = table_.find(family);
        return it == table_.end() ? Value{} : it->second;
    }

 private:
    mutable std::mutex mu_;
    std::unordered_map<IndexFamily, Value> table_;
};

}  // namespace

RegistryTable<LoaderEntry>&
LoaderTable() {
    static RegistryTable<LoaderEntry> table;
    return table;
}

template <typename Input>
RegistryTable<typename BuilderRegistry<Input>::Factory>&
BuilderTable() {
    static RegistryTable<typename BuilderRegistry<Input>::Factory> table;
    return table;
}

LoaderRegistry&
LoaderRegistry::Instance() {
    static LoaderRegistry instance;
    return instance;
}

void
LoaderRegistry::RegisterEntry(std::string_view family, LoaderEntry entry) {
    AssertInfo(static_cast<bool>(entry),
               "cannot register invalid loader entry for {}",
               family);
    LoaderTable().Put(IndexFamily{family}, entry);
}

LoaderEntry
LoaderRegistry::Lookup(const IndexFamily& family) const {
    return LoaderTable().Get(family);
}

template <typename Input>
BuilderRegistry<Input>&
BuilderRegistry<Input>::Instance() {
    static BuilderRegistry<Input> instance;
    return instance;
}

template <typename Input>
void
BuilderRegistry<Input>::Register(IndexFamily family, Factory factory) {
    AssertInfo(static_cast<bool>(factory),
               "cannot register null builder factory for {}",
               family);
    BuilderTable<Input>().Put(std::move(family), std::move(factory));
}

template <typename Input>
std::unique_ptr<IArtifactBuilder<Input>>
BuilderRegistry<Input>::Create(const IndexFamily& family,
                               const BuildParams& params) const {
    auto factory = BuilderTable<Input>().Get(family);
    return factory ? factory(params) : nullptr;
}

// Registered complete input types. Variable-length scalar values use borrowed
// views whose backing storage is held by the materialized input generation.
#define INSTANTIATE_BUILDER_REGISTRY(T) template class BuilderRegistry<T>;
INSTANTIATE_BUILDER_REGISTRY(ScalarBuildInput<bool>)
INSTANTIATE_BUILDER_REGISTRY(ScalarBuildInput<int8_t>)
INSTANTIATE_BUILDER_REGISTRY(ScalarBuildInput<int16_t>)
INSTANTIATE_BUILDER_REGISTRY(ScalarBuildInput<int32_t>)
INSTANTIATE_BUILDER_REGISTRY(ScalarBuildInput<int64_t>)
INSTANTIATE_BUILDER_REGISTRY(ScalarBuildInput<float>)
INSTANTIATE_BUILDER_REGISTRY(ScalarBuildInput<double>)
INSTANTIATE_BUILDER_REGISTRY(ScalarBuildInput<std::string_view>)
INSTANTIATE_BUILDER_REGISTRY(ScalarBuildInput<ArrayView>)
INSTANTIATE_BUILDER_REGISTRY(ScalarBuildInput<JsonProjectedString>)
INSTANTIATE_BUILDER_REGISTRY(VectorBuildInput<float>)
INSTANTIATE_BUILDER_REGISTRY(VectorBuildInput<bin1>)
INSTANTIATE_BUILDER_REGISTRY(VectorBuildInput<float16>)
INSTANTIATE_BUILDER_REGISTRY(VectorBuildInput<bfloat16>)
INSTANTIATE_BUILDER_REGISTRY(VectorBuildInput<int8>)
INSTANTIATE_BUILDER_REGISTRY(VectorBuildInput<sparse_u32_f32>)
INSTANTIATE_BUILDER_REGISTRY(PreparedVectorBuildFiles<float>)
INSTANTIATE_BUILDER_REGISTRY(PreparedVectorBuildFiles<bin1>)
INSTANTIATE_BUILDER_REGISTRY(PreparedVectorBuildFiles<float16>)
INSTANTIATE_BUILDER_REGISTRY(PreparedVectorBuildFiles<bfloat16>)
INSTANTIATE_BUILDER_REGISTRY(PreparedVectorBuildFiles<int8>)
INSTANTIATE_BUILDER_REGISTRY(PreparedVectorBuildFiles<sparse_u32_f32>)
#undef INSTANTIATE_BUILDER_REGISTRY

}  // namespace milvus::index
