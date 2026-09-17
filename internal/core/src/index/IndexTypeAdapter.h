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

#include <string>

#include "common/Types.h"
#include "index/Families.h"
#include "index/contracts/Registry.h"

namespace milvus::storage {
class FileSource;
}

namespace milvus::index {

struct IndexTypeAdapterRequest {
    std::string index_type;
    DataType field_type{DataType::NONE};
    DataType element_type{DataType::NONE};
    // The version supplied by the existing build/load boundary. Zero preserves
    // knowhere's established default-version behavior.
    IndexVersion index_engine_version{0};
    Config params = Config::object();
    // Authoritative schema/boundary value. Any nested aliases already present
    // in params must agree before the adapter writes canonical runtime keys.
    bool is_nested{false};
    bool is_text_match{false};
};

struct AdaptedIndexType {
    IndexFamily family;
    DataType value_type{DataType::NONE};
    Config params = Config::object();

    // The outer scalar type controls the established V3 file name. HYBRID
    // keeps HYBRID here even though ResolveLoadFamily returns its concrete
    // bitmap/inverted/sort/marisa family.
    ScalarIndexType artifact_type{ScalarIndexType::NONE};
};

AdaptedIndexType
AdaptIndexType(const IndexTypeAdapterRequest& request);

// Resolve the legacy HYBRID selector without opening an index. For V1/V2 this
// reads only the existing one-byte `index_type` entry; for V3 it reads the
// existing typed `index_type` meta value already parsed by FileSource.
IndexFamily
ResolveLoadFamily(const IndexFamily& requested_family,
                  storage::FileSource& source);

IndexFamily
FamilyFromScalarIndexType(ScalarIndexType type);

std::string
PackedScalarIndexFileName(ScalarIndexType type);

}  // namespace milvus::index
