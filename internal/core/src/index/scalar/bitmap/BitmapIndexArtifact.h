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
#include <map>
#include <memory>
#include <string>

#include <roaring/roaring.hh>

#include "common/Types.h"
#include "storage/artifact/Artifact.h"
#include "storage/artifact/FileSink.h"

// The ARTIFACT of the bitmap family. Memory-shaped: `Serialize` really
// does encode.

namespace milvus::index {

template <typename T>
using BitmapArtifactPostingMap =
    std::map<T, roaring::Roaring, std::less<>>;

template <typename T>
class BitmapIndexArtifact final : public storage::Artifact {
 public:
    BitmapIndexArtifact(BitmapArtifactPostingMap<T> postings,
                        TargetBitmap valid_bitset,
                        size_t total_num_rows,
                        bool nested,
                        bool nullable);

    ~BitmapIndexArtifact() override;

    void
    Serialize(storage::FileSink& sink) const override;

 private:
    struct BuilderState {
        BitmapArtifactPostingMap<T> postings;
        TargetBitmap valid_bitset;
        size_t total_num_rows{0};
        bool nested{false};
        bool nullable{false};
    };

    BuilderState state_;
};

}  // namespace milvus::index
