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

#include <concepts>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include "common/Types.h"
#include "index/contracts/build/IArtifactBuilder.h"
#include "index/IndexLoader.h"
#include "index/IndexLoadInput.h"
#include "storage/IndexEntryFormat.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "index/contracts/query/ReaderCaps.h"
#include "storage/artifact/FileSource.h"
#include "storage/artifact/LoadOptions.h"

// Family-level loader / builder registries. Selection parameters identify a
// family; the selected implementation parses its own remaining parameters.

namespace milvus::index {

// "inverted" / "bitmap" / "sort" / "marisa" / "text" / "ngram" /
// "json_flat" / "rtree" / "fmindex" / vector families...
//
// A canonical registry key. Load planning derives it from runtime parameters
// and, for the scalar hybrid family, the persisted selector.
using IndexFamily = std::string;

/**
 * @brief Family lookup result and complete index-load entry point.
 * @note Registry lookup owns no source state and performs no I/O;
 * LoaderEntry::Load is the explicit I/O boundary.
 */
struct LoaderEntry {
    using DeriveCapsFn = ReaderCaps (*)(const Config&);
    using CreateFn = folly::coro::Task<std::unique_ptr<IndexLoader>> (*)(
        OpenedIndexSource, storage::LoadOptions);

    DeriveCapsFn derive_caps{nullptr};
    // Create parses family metadata into reusable state without retaining an
    // operation context; IndexLoader::Load supplies its own context.
    CreateFn create{nullptr};

    explicit operator bool() const noexcept {
        return derive_caps != nullptr && create != nullptr;
    }

    /**
     * @brief Open the requested source, create family state and return a fully
     * initialized reader.
     * @note The call blocks at the cache boundary. When the request selects
     * asynchronous transport, storage I/O still runs through the async loading
     * executor.
     */
    IIndexReaderBasePtr
    Load(IndexLoadRequest request) const;
};

/** @brief Compile-time contract for a family registered in LoaderRegistry. */
template <typename Provider>
concept StaticLoaderProvider =
    std::derived_from<Provider, IndexLoader>&& requires(
        const Config& params,
        OpenedIndexSource source,
        storage::LoadOptions options) {
    { Provider::kFamily }
    ->std::convertible_to<std::string_view>;
    { Provider::DeriveCaps(params) }
    ->std::same_as<ReaderCaps>;
    { Provider::Create(std::move(source), std::move(options)) }
    ->std::same_as<folly::coro::Task<std::unique_ptr<IndexLoader>>>;
};

/**
 * @brief Select complete family factories and metadata-only capability
 * inspectors.
 * @note Stores stateless function pointers, not opened sources or reader state.
 * The top-level load pipeline owns storage opening and source-context cleanup
 * before it invokes the selected family constructor.
 */
class LoaderRegistry {
 public:
    /** @return Process-wide registry of stateless family entry points. */
    static LoaderRegistry&
    Instance();

    /**
     * @brief Register one family's capability inspector and constructor.
     */
    template <StaticLoaderProvider Provider>
    void
    Register() {
        LoaderEntry entry{&Provider::DeriveCaps, &Provider::Create};
        RegisterEntry(Provider::kFamily, entry);
    }

    /**
     * @brief Find the registered entry points for a family without opening it.
     * @return The family entry, or an empty entry for an unknown family.
     */
    LoaderEntry
    Lookup(const IndexFamily& family) const;

 private:
    LoaderRegistry() = default;

    void
    RegisterEntry(std::string_view family, LoaderEntry entry);
};

// Family-specific build knobs, opaque to the builder registry.
//
// `Config` is `nlohmann::json`. This is deliberately a
// bag at the builder registry boundary only: the registry finds the factory;
// each factory immediately parses its own typed parameters.
using BuildParams = Config;

// Input-typed builder registry. Family factories parse their own configuration;
// the registry does not interpret it or erase the complete input shape. The
// scalar hybrid family registers a build-time selector rather than a reader.
// Load-resource estimates are separate free functions, not mutable registry
// state.
template <typename Input>
class BuilderRegistry {
 public:
    using Factory = std::function<std::unique_ptr<IArtifactBuilder<Input>>(
        const BuildParams&)>;

    static BuilderRegistry&
    Instance();

    void
    Register(IndexFamily family, Factory factory);

    // Null when the family is unknown or does not build this input shape.
    std::unique_ptr<IArtifactBuilder<Input>>
    Create(const IndexFamily& family, const BuildParams& params) const;

 private:
    BuilderRegistry() = default;
};

// Pre-load resource estimation is implemented by the free functions in
// index/LoadResource.h; it is not registry or reader state.

}  // namespace milvus::index
