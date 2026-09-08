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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

func TestStatsDiscoveryCompactionPublication(t *testing.T) {
	for _, kind := range []datapb.CompactionType{
		datapb.CompactionType_MixCompaction,
		datapb.CompactionType_SortCompaction,
		datapb.CompactionType_ClusteringCompaction,
		datapb.CompactionType_BumpSchemaVersionCompaction,
	} {
		t.Run(kind.String(), func(t *testing.T) {
			input := newCompactionCreateTsTestSegment(1, datapb.SegmentLevel_L1)
			input.IsSorted = true
			input.SchemaVersion = 1
			manifest := packed.MarshalManifestPath("/test/stats/1", 10)
			input.StorageVersion = storage.StorageV3
			input.ManifestPath = manifest
			mt := newCompactionCreateTsTestMeta(input)
			q := newStatsReconcileQueue(64, 4)
			mt.statsDiscovery.Store(q)
			task := &datapb.CompactionTask{
				CollectionID: 100, InputSegments: []int64{1}, Type: kind, Channel: "ch-1",
				Schema: &schemapb.CollectionSchema{Version: 2},
			}
			output := int64(2)
			if kind == datapb.CompactionType_BumpSchemaVersionCompaction {
				output = 1
			}
			result := &datapb.CompactionPlanResult{Segments: []*datapb.CompactionSegment{{
				SegmentID: output, NumOfRows: 100, StorageVersion: storage.StorageV3,
				Manifest: manifest, BaseManifest: manifest,
				InsertLogs:          []*datapb.FieldBinlog{getFieldBinlogIDs(0, 20000)},
				Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogIDs(0, 20001)},
			}}}
			segments, _, err := mt.CompleteCompactionMutation(context.Background(), task, result)
			require.NoError(t, err)
			require.Len(t, segments, 1)
			require.NotNil(t, mt.GetSegment(context.Background(), output))
			require.Contains(t, q.pending, statsReconcileKey{output, indexpb.StatsSubJob_TextIndexJob})
			require.Contains(t, q.pending, statsReconcileKey{output, indexpb.StatsSubJob_JsonKeyIndexJob})
			if kind != datapb.CompactionType_ClusteringCompaction {
				// Clustering inputs stay healthy until the outputs become visible;
				// markInputSegmentsDropped publishes their notification then.
				require.Contains(t, q.pending, statsReconcileKey{1, indexpb.StatsSubJob_TextIndexJob})
			}
		})
	}
}
