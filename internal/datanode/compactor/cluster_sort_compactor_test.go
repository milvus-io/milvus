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

package compactor

import (
	"context"
	"math"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestClusterSortCompactorPlanValidation(t *testing.T) {
	for _, test := range []struct {
		name    string
		mutate  func(*datapb.CompactionPlan)
		message string
	}{
		{"no input", func(p *datapb.CompactionPlan) { p.SegmentBinlogs = nil }, "exactly one input"},
		{"multiple inputs in same group", func(p *datapb.CompactionPlan) {
			sibling := proto.Clone(p.SegmentBinlogs[0]).(*datapb.CompactionSegmentBinlogs)
			sibling.SegmentID++
			p.SegmentBinlogs = append(p.SegmentBinlogs, sibling)
		}, "exactly one input"},
		{"multiple output IDs", func(p *datapb.CompactionPlan) { p.PreAllocatedSegmentIDs.End++ }, "invalid cluster sort plan"},
		{"overlapping input output", func(p *datapb.CompactionPlan) {
			p.PreAllocatedSegmentIDs = &datapb.IDRange{Begin: 100, End: 101}
		}, "invalid cluster sort plan"},
	} {
		t.Run(test.name, func(t *testing.T) {
			w, cm, params := clusterStatsTestWriter(t, math.MaxInt64)
			plan := &datapb.CompactionPlan{
				PlanID: 10, Schema: genCollectionSchema(),
				SegmentBinlogs:         []*datapb.CompactionSegmentBinlogs{{SegmentID: 100, ClusterStats: w.template}},
				PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 300, End: 301},
			}
			test.mutate(plan)
			result, err := NewClusterSortCompactionTask(context.Background(), cm, plan, params).Compact()
			require.ErrorContains(t, err, test.message)
			require.Nil(t, result)
		})
	}
}

func TestClusterSortCompactorFailureAndEmptyResult(t *testing.T) {
	for _, mode := range []string{"missing stats", "corrupt stats", "wrong group", "wrong row count", "canceled", "all expired"} {
		t.Run(mode, func(t *testing.T) {
			w, cm, params := clusterStatsTestWriter(t, math.MaxInt64)
			require.NoError(t, w.WriteBatch(context.Background(), []clusterLayoutSortRow{clusterStatsTestRow(2), clusterStatsTestRow(1), clusterStatsTestRow(0)}))
			segments, err := w.Close(context.Background())
			require.NoError(t, err)
			require.Len(t, segments, 1)
			s := segments[0]
			input := &datapb.CompactionSegmentBinlogs{
				SegmentID: s.SegmentID, CollectionID: 10, PartitionID: 20,
				FieldBinlogs: s.InsertLogs, StorageVersion: s.StorageVersion, Manifest: s.Manifest,
				ClusterStats: proto.Clone(s.ClusterStats).(*datapb.ClusterStats),
			}
			plan := &datapb.CompactionPlan{
				PlanID: 10, Type: datapb.CompactionType_ClusterSortCompaction, Schema: genCollectionSchema(),
				SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{input}, TotalRows: 3, MaxSize: 1,
				PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 300, End: 301}, PreAllocatedLogIDs: &datapb.IDRange{Begin: 10000, End: 11000},
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "missing stats":
				input.ClusterStats.Files = []string{path.Join(params.StorageConfig.RootPath, "missing.keys")}
			case "corrupt stats":
				file := path.Join(params.StorageConfig.RootPath, "corrupt.keys")
				require.NoError(t, cm.Write(ctx, file, make([]byte, 40)))
				input.ClusterStats.Files = []string{file}
			case "wrong group":
				input.ClusterStats.CentroidIds = []uint32{10}
			case "wrong row count":
				input.ClusterStats.NumRows++
			case "canceled":
				cancel()
			case "all expired":
				plan.CollectionTtl = int64(time.Second)
			}
			result, err := NewClusterSortCompactionTask(ctx, cm, plan, params).Compact()
			if mode == "all expired" {
				require.NoError(t, err)
				require.Equal(t, datapb.CompactionTaskState_completed, result.State)
				require.Empty(t, result.Segments)
			} else {
				require.Error(t, err)
				require.Nil(t, result)
				if mode == "canceled" {
					require.ErrorIs(t, err, context.Canceled)
				}
			}
			// A failed/empty sort neither publishes an output nor retains its runs.
			var objects int
			err = cm.WalkWithPrefix(context.Background(), path.Join(params.StorageConfig.RootPath, "cluster_sort_runs")+"/", true,
				func(*storage.ChunkObjectInfo) bool { objects++; return true })
			if !os.IsNotExist(err) {
				require.NoError(t, err)
			}
			require.Zero(t, objects)
		})
	}
}
