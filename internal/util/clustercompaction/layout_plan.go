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

// ValidateCentroids validates the current FP32 description contract. Half
// precision admission belongs to PR5; scalar mapping is a separate extension.
func ValidateCentroids(stats *clusteringpb.ClusteringCentroidsStats, fieldType schemapb.DataType, dim int64) error {
	if fieldType != schemapb.DataType_FloatVector {
		return merr.Wrapf(merr.ErrServiceUnimplemented, "clustering planner field type %s", fieldType)
	}
	if stats == nil || len(stats.GetCentroids()) == 0 || uint64(len(stats.GetCentroids())) > math.MaxUint32 || dim <= 0 {
		return merr.WrapErrDataIntegrityMsg("invalid clustering centroid shape")
	}
	for _, centroid := range stats.GetCentroids() {
		if centroid == nil || centroid.GetDim() != dim || centroid.GetFloatVector() == nil || int64(len(centroid.GetFloatVector().GetData())) != dim {
			return merr.WrapErrDataIntegrityMsg("clustering centroid type or dimension mismatch")
		}
		for _, value := range centroid.GetFloatVector().GetData() {
			if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
				return merr.WrapErrDataIntegrityMsg("non-finite clustering centroid")
			}
		}
	}
	return nil
}
