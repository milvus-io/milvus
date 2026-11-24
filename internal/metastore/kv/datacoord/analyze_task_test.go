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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// goldenAnalyzeTaskKey is the key the removed SaveAnalyzeTask wrote
// (fmt.Sprintf("%s/%d", AnalyzeTaskPrefix, taskID), with AnalyzeTaskPrefix
// "datacoord-meta/analyze-task").
const goldenAnalyzeTaskKey = "datacoord-meta/analyze-task/4"

func newTestAnalyzeTask() *indexpb.AnalyzeTask {
	return &indexpb.AnalyzeTask{
		CollectionID:  1,
		PartitionID:   2,
		FieldID:       3,
		FieldName:     "vector",
		FieldType:     schemapb.DataType_FloatVector,
		TaskID:        4,
		Version:       1,
		NodeID:        1,
		State:         indexpb.JobState_JobStateFinished,
		Dim:           8,
		CentroidsFile: "centroids",
	}
}

// TestAnalyzeTask_Coordinate pins the coordinate and the legacy key.
func TestAnalyzeTask_Coordinate(t *testing.T) {
	coord := metastore.AnalyzeTaskType.Coordinate(newTestAnalyzeTask())
	require.Equal(t, []string{"4"}, coord)
	key, err := recordKey(metastore.AnalyzeTaskType, coord)
	require.NoError(t, err)
	require.Equal(t, goldenAnalyzeTaskKey, key)
	got, ok := metastore.ModelTypeOf(newTestAnalyzeTask())
	require.True(t, ok)
	require.Same(t, metastore.AnalyzeTaskType, got)
}

// TestAnalyzeTask_EncodingMatchesLegacy pins the key, the proto value and
// the KV calls of a save and a delete.
func TestAnalyzeTask_EncodingMatchesLegacy(t *testing.T) {
	ctx := context.Background()
	value, err := proto.Marshal(newTestAnalyzeTask())
	require.NoError(t, err)
	saved := []kvCall{{op: "MultiSaveAndRemove", saves: map[string]string{goldenAnalyzeTaskKey: string(value)}}}

	tests := []struct {
		name   string
		action metastore.UpdateAction
		want   []kvCall
	}{
		{"save", metastore.SaveAnalyzeTask(newTestAnalyzeTask()), saved},
		{"add is a save", metastore.UpdateAction{Type: metastore.ActionAdd, Entry: newTestAnalyzeTask()}, saved},
		{"delete", metastore.DropAnalyzeTask(4), []kvCall{{
			op: "MultiSaveAndRemove", saves: map[string]string{}, removals: []string{goldenAnalyzeTaskKey},
		}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			catalog, r := newRecordingCatalog(t, 128)
			require.NoError(t, catalog.Update(ctx, tt.action))
			require.Equal(t, tt.want, r.recorded())
		})
	}

	t.Run("nil task", func(t *testing.T) {
		catalog, r := newRecordingCatalog(t, 128)
		require.Error(t, catalog.Update(ctx, metastore.SaveAnalyzeTask(nil)))
		require.Empty(t, r.recorded(), "nothing is written")
	})
}

// TestAnalyzeTask_ListAndGet covers recovery and point reads through the
// generic primitives, including fail-closed decoding.
func TestAnalyzeTask_ListAndGet(t *testing.T) {
	ctx := context.Background()
	catalog, r := newRecordingCatalog(t, 128)
	task := newTestAnalyzeTask()
	require.NoError(t, catalog.Update(ctx, metastore.SaveAnalyzeTask(task)))
	list := func() (metastore.ListResult, error) {
		return catalog.List(ctx, metastore.AnalyzeTaskType, nil, metastore.ListOptions{})
	}

	listed, err := list()
	require.NoError(t, err)
	require.Len(t, listed.Values, 1)
	require.True(t, proto.Equal(task, listed.Values[0].(*indexpb.AnalyzeTask)))

	got, _, err := catalog.Get(ctx, metastore.AnalyzeTaskType, []string{"4"})
	require.NoError(t, err)
	require.True(t, proto.Equal(task, got.(*indexpb.AnalyzeTask)))
	_, _, err = catalog.Get(ctx, metastore.AnalyzeTaskType, []string{"5"})
	require.ErrorIs(t, err, merr.ErrIoKeyNotFound)

	t.Run("corrupt record", func(t *testing.T) {
		r.set("datacoord-meta/analyze-task/6", "not a proto \xff")
		defer r.del("datacoord-meta/analyze-task/6")
		_, err := list()
		require.ErrorContains(t, err, "datacoord-meta/analyze-task/6")
	})

	t.Run("record under another task's key", func(t *testing.T) {
		value, err := proto.Marshal(task)
		require.NoError(t, err)
		r.set("datacoord-meta/analyze-task/7", string(value))
		defer r.del("datacoord-meta/analyze-task/7")
		_, err = list()
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
	})
}

// TestAnalyzeTask_ComposesWithUnmigratedTypes covers a partition-stats
// cleanup: a registered analyze-task delete and the typed (not migrated)
// partition-stats actions in one Update. On the chunked fallback the analyze
// task removal is flushed first; the version pointer and the partition-stats
// info removal, both commit ops, land together in the final txn.
func TestAnalyzeTask_ComposesWithUnmigratedTypes(t *testing.T) {
	ctx := context.Background()
	info := &datapb.PartitionStatsInfo{CollectionID: 1, PartitionID: 2, VChannel: "ch-1", Version: 100}
	actions := []metastore.UpdateAction{
		metastore.DropAnalyzeTask(4),
		metastore.SavePartitionStatsVersion(1, 2, "ch-1", 90),
		metastore.DropPartitionStats(info),
	}
	catalog, r := newRecordingCatalog(t, 2)
	require.NoError(t, catalog.Update(ctx, actions...))
	calls := r.recorded()
	require.Len(t, calls, 2)
	require.Equal(t, []string{goldenAnalyzeTaskKey}, calls[0].removals)
	require.Empty(t, calls[0].saves)
	require.Equal(t, map[string]string{buildCurrentPartitionStatsVersionPath(1, 2, "ch-1"): "90"}, calls[1].saves)
	require.Equal(t, []string{buildPartitionStatsInfoPath(info)}, calls[1].removals,
		"the commit ops land last, in one txn")
}
