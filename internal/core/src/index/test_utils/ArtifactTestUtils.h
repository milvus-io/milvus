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

#include <filesystem>
#include <stdexcept>
#include <string>

#include "index/IndexTypeAdapter.h"
#include "index/contracts/Registry.h"
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
    TestArtifactSink sink(result);
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
    options.params = AnnotateJsonProjectionCompleteness(
        backend.LoadParams(metadata), source);
    const auto expected_caps = loader.derive_caps(options.params);
    auto reader = loader.open(source, options);
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
    return reader;
}

inline IIndexReaderBasePtr
OpenV3(const ReaderBackend& backend,
       const TestArtifactData& artifact,
       const BackendCaseMetadata& metadata = {}) {
    TestArtifactSource source(artifact);
    return OpenFromSource(backend, source, metadata);
}

inline IIndexReaderBasePtr
OpenV1V2(const ReaderBackend& backend,
         const TestArtifactData& artifact,
         const BackendCaseMetadata& metadata = {}) {
    TestArtifactSource source(artifact, storage::Generation::V1V2);
    return OpenFromSource(backend, source, metadata);
}

}  // namespace milvus::index::test
