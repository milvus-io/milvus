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
#include <string>

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
