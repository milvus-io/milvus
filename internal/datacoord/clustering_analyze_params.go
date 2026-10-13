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

package datacoord

import (
	"context"
	"maps"
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Resolve K once, using raw vector bytes per centroid rather than segment size.
func clusteringAnalyzeK(rows, dim, chunkBytes, minK, maxK int64) (int64, error) {
	if rows < 0 || dim <= 0 || dim > math.MaxInt64/4 || chunkBytes <= 0 || minK <= 0 || maxK < minK || maxK > math.MaxUint32 {
		return 0, merr.WrapErrServiceInternalMsg("invalid clustering analyze shape or centroid limits")
	}
	rowBytes := dim * 4
	if rows > math.MaxInt64/rowBytes {
		return 0, merr.WrapErrServiceInternalMsg("clustering analyze input size overflows int64")
	}
	bytes := rows * rowBytes
	k := bytes / chunkBytes
	if bytes%chunkBytes != 0 {
		k++
	}
	return max(minK, min(k, maxK)), nil
}

func (t *clusteringCompactionTask) newAnalyzeTask() (*indexpb.AnalyzeTask, error) {
	plan := t.GetTaskProto()
	field := plan.GetClusteringKeyField()
	if field.GetDataType() != schemapb.DataType_FloatVector {
		return nil, merr.WrapErrServiceInternalMsg("clustering analyze currently requires FloatVector")
	}
	dim, err := storage.GetDimFromParams(field.GetTypeParams())
	if err != nil {
		return nil, merr.Wrap(err, "resolve clustering field dimension")
	}
	var rows int64
	seen := make(map[int64]struct{}, len(plan.GetInputSegments()))
	for _, id := range plan.GetInputSegments() {
		segment := t.meta.GetHealthySegment(context.TODO(), id)
		_, duplicate := seen[id]
		if segment == nil || duplicate || segment.GetCollectionID() != plan.GetCollectionID() || segment.GetPartitionID() != plan.GetPartitionID() {
			return nil, merr.WrapErrServiceInternalMsg("invalid clustering analyze input segment %d", id)
		}
		seen[id] = struct{}{}
		if segment.GetNumOfRows() < 0 || segment.GetNumOfRows() > math.MaxInt64-rows {
			return nil, merr.WrapErrServiceInternalMsg("invalid clustering analyze row count for segment %d", id)
		}
		rows += segment.GetNumOfRows()
	}
	p := paramtable.Get()
	k, err := clusteringAnalyzeK(rows, int64(dim), p.DataCoordCfg.ClusteringCompactionChunkSizePerCentroid.GetAsSize(),
		p.DataCoordCfg.ClusteringCompactionMinCentroidsNum.GetAsInt64(), p.DataCoordCfg.ClusteringCompactionMaxCentroidsNum.GetAsInt64())
	if err != nil {
		return nil, err
	}
	params := maps.Clone(p.KnowhereConfig.GetAnalyzeParams())
	// Keep the existing provider metric default for now. Explicit field metric
	// resolution, freezing and COSINE semantics are tracked in the plan's G2 TODO.
	return &indexpb.AnalyzeTask{
		CollectionID: plan.GetCollectionID(), PartitionID: plan.GetPartitionID(),
		FieldID: field.GetFieldID(), FieldName: field.GetName(), FieldType: field.GetDataType(), Dim: int64(dim),
		SegmentIDs: append([]int64(nil), plan.GetInputSegments()...), TaskID: plan.GetAnalyzeTaskID(),
		State: indexpb.JobState_JobStateInit, NumClusters: k,
		ClusterType: p.KnowhereConfig.ClusterType.GetValue(), ClusterParams: params,
	}, nil
}
