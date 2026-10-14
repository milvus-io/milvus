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

package clustercompaction

import (
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ClusterGroup contains indivisible Analyze offsets. Its position in Groups is
// a task-local ordinal, not a persistent identity. A writer may roll over into
// several segments, but must never share a segment with another group.
type ClusterGroup struct {
	Offsets []uint32
	Rows    int64
}

// LayoutPlan is an owned, in-memory result. Empty centroids are excluded from
// Groups, but retain their position in Counts. Row counts precede TTL/deletes.
type LayoutPlan struct {
	Counts []int64
	Groups []ClusterGroup
}

// Validate checks coverage independently of the provider. A group larger than
// the target is legal: an indivisible cluster must not be split across groups.
func (p *LayoutPlan) Validate() error {
	if p == nil || len(p.Counts) == 0 || uint64(len(p.Counts)) > math.MaxUint32 {
		return merr.WrapErrDataIntegrityMsg("invalid clustering layout count vector")
	}
	var total int64
	for _, count := range p.Counts {
		if count < 0 || count > math.MaxInt64-total {
			return merr.WrapErrDataIntegrityMsg("clustering layout row count overflows or is negative")
		}
		total += count
	}
	seen := make([]bool, len(p.Counts))
	for ordinal, group := range p.Groups {
		if len(group.Offsets) == 0 {
			return merr.WrapErrDataIntegrityMsg("empty clustering layout group %d", ordinal)
		}
		var rows int64
		for _, offset := range group.Offsets {
			if uint64(offset) >= uint64(len(p.Counts)) || seen[offset] || p.Counts[offset] == 0 {
				return merr.WrapErrDataIntegrityMsg("invalid or repeated clustering offset %d in group %d", offset, ordinal)
			}
			seen[offset] = true
			rows += p.Counts[offset] // Bounded by the checked sum above.
		}
		if rows != group.Rows {
			return merr.WrapErrDataIntegrityMsg("clustering layout group %d row count mismatch", ordinal)
		}
	}
	for offset, count := range p.Counts {
		if count > 0 && !seen[offset] {
			return merr.WrapErrDataIntegrityMsg("clustering layout omits offset %d", offset)
		}
	}
	return nil
}

// VectorElementBytes returns the dense floating vector element width, or zero
// for types outside the clustering pipeline (including binary/sparse/Int8).
func VectorElementBytes(t schemapb.DataType) int64 {
	switch t {
	case schemapb.DataType_FloatVector:
		return 4
	case schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector:
		return 2
	default:
		return 0
	}
}

// ValidateCentroids checks schema type, oneof, byte width and finite values.
func ValidateCentroids(stats *clusteringpb.ClusteringCentroidsStats, fieldType schemapb.DataType, dim int64) error {
	width := VectorElementBytes(fieldType)
	if width == 0 {
		return merr.Wrapf(merr.ErrServiceUnimplemented, "clustering planner field type %s", fieldType)
	}
	if stats == nil || len(stats.GetCentroids()) == 0 || uint64(len(stats.GetCentroids())) > math.MaxUint32 || dim <= 0 || dim > math.MaxInt64/width {
		return merr.WrapErrDataIntegrityMsg("invalid clustering centroid shape")
	}
	for _, c := range stats.GetCentroids() {
		if c == nil || c.GetDim() != dim {
			return merr.WrapErrDataIntegrityMsg("clustering centroid dimension mismatch")
		}
		switch fieldType {
		case schemapb.DataType_FloatVector:
			if c.GetFloatVector() == nil || int64(len(c.GetFloatVector().Data)) != dim {
				return merr.WrapErrDataIntegrityMsg("invalid FP32 centroid payload")
			}
			for _, v := range c.GetFloatVector().Data {
				if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
					return merr.WrapErrDataIntegrityMsg("non-finite clustering centroid")
				}
			}
		case schemapb.DataType_Float16Vector:
			if int64(len(c.GetFloat16Vector())) != dim*width {
				return merr.WrapErrDataIntegrityMsg("invalid FP16 centroid payload")
			}
			if err := typeutil.VerifyFloats16(c.GetFloat16Vector()); err != nil {
				return merr.WrapErrDataIntegrity(err, "non-finite FP16 centroid")
			}
		case schemapb.DataType_BFloat16Vector:
			if int64(len(c.GetBfloat16Vector())) != dim*width {
				return merr.WrapErrDataIntegrityMsg("invalid BF16 centroid payload")
			}
			if err := typeutil.VerifyBFloats16(c.GetBfloat16Vector()); err != nil {
				return merr.WrapErrDataIntegrity(err, "non-finite BF16 centroid")
			}
		}
	}
	return nil
}
