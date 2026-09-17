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
#include <exception>
#include <memory>
#include <new>
#include <string>
#include <utility>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "storage/artifact/Artifact.h"
#include "storage/artifact/FileSink.h"

// Memory-shaped FM artifact. Build produces the in-memory structure and
// Serialize encodes it, unlike file-shaped families whose build already writes
// the engine files.

namespace milvus::index {

namespace fmindex {
class FMIndex;
}

// Classifying boundary for the vendored fm-index-lite library (see its NOTICE
// for the pinned upstream revision). That library does not depend on
// milvus-common, so it throws plain std:: exceptions; anything escaping it
// untyped reaches the cgo boundary as UnexpectedError(2001), the bucket that
// means "unclassified internal bug" -- and, on the load path, an unclassified
// failure is also indistinguishable from a transient one to whoever decides
// whether to retry. Every call into the library goes through here. It lives in
// this header because scripts/check_segcore_error_boundaries.sh RULE 2 confines
// `fmindex::` to this family's Artifact/Builder/Loader/Reader units.
//
// `fallback` is the code for a library failure in this phase: IndexBuildError
// while building, DataFormatBroken while loading (a blob the library rejects is
// a corrupt blob). std::bad_alloc is deliberately NOT folded into it: an
// allocation failure is transient and MemAllocateFailed is retriable, while both
// fallbacks are permanent. The library's parse path relies on exactly this --
// it self-classifies every other std::exception into a false return value and
// rethrows only bad_alloc (fmindex/FMIndexInl.h, FMIndex::parseView).
template <typename Fn>
decltype(auto)
GuardFmIndexLibrary(Fn&& fn, ErrorCode fallback, const char* action) {
    try {
        return std::forward<Fn>(fn)();
    } catch (const SegcoreError&) {
        // Already classified by a nested milvus call; keep its code.
        throw;
    } catch (const std::bad_alloc& error) {
        ThrowInfo(ErrorCode::MemAllocateFailed,
                  "failed to {} FM-index: {}",
                  action,
                  error.what());
    } catch (const std::exception& error) {
        ThrowInfo(fallback, "failed to {} FM-index: {}", action, error.what());
    } catch (...) {
        ThrowInfo(fallback, "failed to {} FM-index: unknown exception", action);
    }
}

class FmIndexStorage;

class FmIndexArtifact final : public storage::Artifact {
 public:
    FmIndexArtifact(fmindex::FMIndex engine,
                    TargetBitmap null_bitmap,
                    int64_t total_rows,
                    DataType value_type,
                    bool nullable,
                    std::string local_dir);

    ~FmIndexArtifact() override;

    void
    Serialize(storage::FileSink& sink) const override;

 private:
    std::shared_ptr<const FmIndexStorage> storage_;
    // Configured parent path is borrowed storage policy. Serialization owns
    // only the unique temporary file it creates below this directory.
    std::string local_dir_;
};

}  // namespace milvus::index
