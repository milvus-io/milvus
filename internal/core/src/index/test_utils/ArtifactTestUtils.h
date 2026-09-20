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

#include <gtest/gtest.h>
#include "index/IndexLoaderFactory.h"
#include <arrow/io/memory.h>

#include <filesystem>
#include <stdexcept>
#include <string>

#include "index/IndexTypeAdapter.h"
#include "index/PackedIndexLoad.h"
#include "storage/AsyncLoadExecutor.h"
#include "folly/coro/BlockingWait.h"
#include "storage/IndexEntryDirectStreamWriter.h"
#include "storage/RemoteInputStream.h"
#include "storage/RemoteOutputStream.h"
#include "index/contracts/Registry.h"
#include "index/LegacyIndexLoad.h"
#include "index/scalar/json/JsonProjectedIndexLoad.h"
#include "index/test_utils/AssertHelpers.h"
#include "index/test_utils/ScalarReaderFactory.h"
#include "index/test_utils/TestArtifactIO.h"
#include "storage/artifact/FileSink.h"
#include "storage/artifact/FileSource.h"
#include "storage/artifact/LoadOptions.h"

namespace milvus::index::test {

inline TestArtifactData
SerializeV3(const storage::Artifact& artifact) {
    TestArtifactData result;
    TestArtifactWriter sink(result);
    artifact.Serialize(sink);
    static_cast<void>(sink.Finish());
    return result;
}

inline TestArtifactData
SerializeV1V2(const storage::Artifact& artifact) {
    TestArtifactData result;
    TestArtifactSink sink(result, storage::Generation::V1V2);
    artifact.Serialize(sink);
    static_cast<void>(sink.Finish());
    return result;
}

inline void
CheckReaderBackend(const IIndexReaderBasePtr& reader,
                   const ReaderBackend& backend,
                   const ReaderCaps& expected_caps) {
    if (!reader) {
        throw std::logic_error(backend.Name() +
                               ": test loader returned null reader");
    }
    const auto caps = reader->Caps();
    EXPECT_EQ(caps.predicate, expected_caps.predicate);
    EXPECT_EQ(caps.pattern_match, expected_caps.pattern_match);
    EXPECT_EQ(caps.text_match, expected_caps.text_match);
    EXPECT_EQ(caps.ngram_candidates, expected_caps.ngram_candidates);
    EXPECT_EQ(caps.spatial, expected_caps.spatial);
    EXPECT_EQ(caps.nested, expected_caps.nested);
    EXPECT_EQ(caps.value_lookup, expected_caps.value_lookup);
    EXPECT_EQ(caps.cheap_value_lookup, expected_caps.cheap_value_lookup);
    EXPECT_EQ(caps.json_paths, expected_caps.json_paths);
    EXPECT_EQ(caps.exact, expected_caps.exact);
    EXPECT_EQ(reader->CoordDomain(), backend.ExpectedDomain());
    EXPECT_TRUE(ScalarValueTypesMatch(reader->ValueType(),
                                      backend.ExpectedValueType()));
}

inline IIndexReaderBasePtr
OpenFromSource(const ReaderBackend& backend,
               storage::FileSource& source,
               const BackendCaseMetadata& metadata = {}) {
    const auto family = ResolveLoadFamily(backend.Family(), source);
    const auto loader = LoaderRegistry::Instance().Lookup(family);
    if (!loader) {
        throw std::logic_error(
            backend.Name() + ": test source resolved unknown loader " + family);
    }
    storage::LoadOptions options;
    options.enable_mmap = backend.MmapRequested();
    if (options.enable_mmap) {
        options.mmap_dir_path = std::filesystem::temp_directory_path().string();
    }
    options.params = backend.LoadParams(metadata);
    if (family != families::kJsonFlat) {
        options.params = AnnotateJsonProjectionCompleteness(
            std::move(options.params), source);
    }
    const auto expected_caps = loader.derive_caps(options.params);
    auto reader = LoadIndex(
        loader,
        {OpenedIndexInput{LegacyIndexSource{
             std::shared_ptr<storage::FileSource>(&source, [](auto*) {}),
             false}},
         options});
    CheckReaderBackend(reader, backend, expected_caps);
    return reader;
}

// Serialize test fixtures through the real packed format, including CRCs.
inline std::shared_ptr<arrow::Buffer>
MakePackedArtifactBuffer(const TestArtifactData& artifact) {
    auto buffer = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto output = std::make_shared<storage::RemoteOutputStream>(buffer);
    storage::IndexEntryDirectStreamWriter writer(output, 4096);
    for (const auto& [name, bytes] : artifact.entries) {
        writer.WriteEntry(name, bytes.data(), bytes.size());
    }
    for (const auto& [key, value] : artifact.metadata) {
        writer.PutMeta(key, value);
    }
    writer.Finish();
    return buffer->Finish().ValueOrDie();
}

inline IIndexReaderBasePtr
OpenV3(const ReaderBackend& backend,
       const TestArtifactData& artifact,
       const BackendCaseMetadata& metadata = {},
       bool use_async = false) {
    auto input = std::make_shared<storage::RemoteInputStream>(
        std::make_shared<arrow::io::BufferReader>(
            MakePackedArtifactBuffer(artifact)));
    auto source = storage::IndexEntryReader::Open(input, input->Size());
    storage::LoadOptions options;
    options.enable_mmap = backend.MmapRequested();
    if (options.enable_mmap) {
        options.mmap_dir_path = std::filesystem::temp_directory_path().string();
    }
    options.params = backend.LoadParams(metadata);
    if (backend.Family() != families::kJsonFlat) {
        options.params =
            AnnotateJsonProjectionCompleteness(std::move(options.params),
                                               source->Directory(),
                                               source->IndexMeta());
    }
    const auto family = ResolvePackedLoadFamily(
        backend.Family(), source->IndexMeta(), options.params);
    const auto loader = LoaderRegistry::Instance().Lookup(family);
    if (!loader) {
        throw std::logic_error(backend.Name() + ": missing packed loader for " +
                               family);
    }
    const auto expected_caps = loader.derive_caps(options.params);
    IIndexReaderBasePtr reader;
    if (use_async) {
        auto load = [&]() -> folly::coro::Task<IIndexReaderBasePtr> {
            const auto priority = proto::common::LoadPriority::HIGH;
            auto async_source = co_await storage::AsyncIndexEntryReader::Open(
                input, 0, priority);
            IndexOpenRequest request{
                OpenedIndexInput{PackedIndexSource{
                    std::shared_ptr<storage::AsyncIndexEntryReader>(
                        std::move(async_source))}},
                options};
            co_return co_await LoadIndexAsync(loader, std::move(request));
        };
        reader = folly::coro::blockingWait(folly::coro::co_withExecutor(
            storage::ResolveAsyncLoadExecutor(
                {}, proto::common::LoadPriority::HIGH),
            load()));
    } else {
        reader = LoadIndex(loader,
                           {OpenedIndexInput{PackedIndexSource{
                                std::shared_ptr<storage::IndexEntryReader>(
                                    std::move(source))}},
                            options});
    }
    CheckReaderBackend(reader, backend, expected_caps);
    return reader;
}

inline IIndexReaderBasePtr
OpenV1V2(const ReaderBackend& backend,
         const TestArtifactData& artifact,
         const BackendCaseMetadata& metadata = {}) {
    TestArtifactSource source(artifact, storage::Generation::V1V2);
    return OpenFromSource(backend, source, metadata);
}

}  // namespace milvus::index::test
