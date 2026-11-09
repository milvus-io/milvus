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

#include "common/Geometry.h"
#include "common/Types.h"

// Spatial candidate queries, independent of point/range predicates. Concrete
// readers also expose INullReader. This pure mixin does not inherit
// IIndexReaderBase; exact geometry evaluation belongs to the consumer.

namespace milvus::index {

// Native spatial operators. STIsValid is evaluated without an index. For
// DWithin, the consumer expands the query bounding box before candidate lookup
// and checks the exact distance afterwards, so no distance parameter is needed.
enum class SpatialOp {
    Equals,
    Touches,
    Overlaps,
    Crosses,
    Contains,
    Intersects,
    Within,
    DWithin,
};

class ISpatialReader {
 public:
    virtual ~ISpatialReader() = default;

    // Return an MBR candidate superset as a Count()-sized bitmap. The consumer
    // evaluates the exact spatial relation against original values. Current RTree
    // lookup uses bounding-box intersection rather than operator-specific pruning;
    // do not interpret these candidates as exact hits.
    virtual TargetBitmap
    Candidates(SpatialOp op, const Geometry& query_geom) const = 0;
};

}  // namespace milvus::index
