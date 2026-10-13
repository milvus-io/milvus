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
	"fmt"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func l0LockFixture(t *testing.T, targets ...int64) (*meta, *l0CompactionTask, []*datapb.CompactionSegment) {
	t.Helper()
	ctx := context.Background()
	mt, err := newMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, mt.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID: 100, CollectionID: 1, PartitionID: 10,
		State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L0,
	})))
	var output []*datapb.CompactionSegment
	for _, id := range targets {
		base := fmt.Sprintf("/tmp/milvus/insert_log/1/10/%d", id)
		require.NoError(t, mt.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
			ID: id, CollectionID: 1, PartitionID: 10, NumOfRows: 100,
			State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
			StorageVersion: storage.StorageV3, ManifestPath: packed.MarshalManifestPath(base, 7),
		})))
		output = append(output, &datapb.CompactionSegment{
			SegmentID: id,
			Deltalogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{
				LogID: 9000 + id, LogPath: base + "/_delta/1", EntriesNum: 3, MemorySize: 128,
			}}}},
		})
	}
	task := newL0CompactionTask(&datapb.CompactionTask{PlanID: 1, InputSegments: []int64{100}}, nil, mt)
	return mt, task, output
}

func awaitL0Result(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("operation blocked while L0 manifest I/O was paused")
	}
	return nil
}

func TestL0ManifestIOAllowsUnrelatedRPCs(t *testing.T) {
	ctx := context.Background()
	mt, task, output := l0LockFixture(t, 200)
	base := "/tmp/milvus/insert_log/1/10/300"
	require.NoError(t, mt.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID: 300, CollectionID: 1, PartitionID: 10, NumOfRows: 10,
		State: commonpb.SegmentState_Growing, Level: datapb.SegmentLevel_L1,
		StorageVersion: storage.StorageV3, ManifestPath: packed.MarshalManifestPath(base, 1),
	})))
	server := &Server{meta: mt}
	server.stateCode.Store(commonpb.StateCode_Healthy)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	patch := mockey.Mock(packed.AddDeltaLogsToManifestOverwrite).To(
		func(path string, _ *indexpb.StorageConfig, _ []packed.DeltaLogEntry) (string, error) {
			close(entered)
			<-release
			base, version, err := packed.UnmarshalManifestPath(path)
			return packed.MarshalManifestPath(base, version+1), err
		}).Build()
	defer patch.UnPatch()
	defer unblock()
	done := make(chan error, 1)
	go func() { done <- task.saveSegmentMeta(output) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("L0 manifest commit did not start")
	}
	flush := make(chan error, 1)
	go func() {
		status, err := server.SaveBinlogPaths(ctx, &datapb.SaveBinlogPathsRequest{
			SegmentID: 300, CollectionID: 1, PartitionID: 10,
			SegLevel: datapb.SegmentLevel_L1, StorageVersion: storage.StorageV3,
			ManifestPath: packed.MarshalManifestPath(base, 2),
		})
		if err == nil {
			err = merr.Error(status)
		}
		flush <- err
	}()
	require.NoError(t, awaitL0Result(t, flush))
	read := make(chan error, 1)
	go func() {
		response, err := server.GetSegmentInfo(ctx, &datapb.GetSegmentInfoRequest{SegmentIDs: []int64{300}})
		if err == nil {
			err = merr.Error(response.GetStatus())
		}
		read <- err
	}()
	require.NoError(t, awaitL0Result(t, read))
	require.Equal(t, packed.MarshalManifestPath(base, 2), mt.GetSegment(ctx, 300).GetManifestPath())
	// Also rebase a normal metadata change on the L0 target itself.
	require.NoError(t, mt.UpdateSegmentsInfo(ctx, UpdateIsImporting(200, true)))
	require.Equal(t, commonpb.SegmentState_Flushed, mt.GetSegment(ctx, 100).GetState())
	unblock()
	require.NoError(t, awaitL0Result(t, done))
	require.True(t, mt.GetSegment(ctx, 200).GetIsImporting())
	require.EqualValues(t, 3, mt.GetSegment(ctx, 200).GetStats().GetDeleteNumRows())
	require.Equal(t, commonpb.SegmentState_Dropped, mt.GetSegment(ctx, 100).GetState())
	require.True(t, mt.GetSegment(ctx, 100).GetCompacted())
}

type failOnceL0Catalog struct {
	metastore.DataCoordCatalog
	fail bool
}

func (c *failOnceL0Catalog) AlterSegments(ctx context.Context, segments []*datapb.SegmentInfo, increments ...metastore.BinlogsIncrement) error {
	if c.fail {
		c.fail = false
		return merr.WrapErrServiceUnavailable("injected catalog failure")
	}
	return c.DataCoordCatalog.AlterSegments(ctx, segments, increments...)
}

func TestL0ManifestRetryAfterCatalogFailure(t *testing.T) {
	ctx := context.Background()
	mt, task, output := l0LockFixture(t, 200, 201)
	mt.catalog = &failOnceL0Catalog{DataCoordCatalog: mt.catalog, fail: true}
	var mu sync.Mutex
	calls := make(map[string]int)
	patch := mockey.Mock(packed.AddDeltaLogsToManifestOverwrite).To(
		func(path string, _ *indexpb.StorageConfig, _ []packed.DeltaLogEntry) (string, error) {
			mu.Lock()
			calls[path]++
			mu.Unlock()
			base, version, err := packed.UnmarshalManifestPath(path)
			return packed.MarshalManifestPath(base, version+1), err
		}).Build()
	defer patch.UnPatch()
	require.ErrorIs(t, task.saveSegmentMeta(output), merr.ErrServiceUnavailable)
	for _, id := range []int64{200, 201} {
		require.Empty(t, mt.GetSegment(ctx, id).GetDeltalogs())
		require.Zero(t, mt.GetSegment(ctx, id).GetStats().GetDeleteNumRows())
	}
	require.Equal(t, commonpb.SegmentState_Flushed, mt.GetSegment(ctx, 100).GetState())
	require.NoError(t, task.saveSegmentMeta(output))
	// A later meta_saved failure replays the same result once more.
	require.NoError(t, task.saveSegmentMeta(output))
	require.Len(t, calls, 2)
	for _, n := range calls {
		require.Equal(t, 1, n)
	}
	for _, id := range []int64{200, 201} {
		require.EqualValues(t, 3, mt.GetSegment(ctx, id).GetStats().GetDeleteNumRows())
	}
	require.Equal(t, commonpb.SegmentState_Dropped, mt.GetSegment(ctx, 100).GetState())
	require.NotEmpty(t, output[0].GetDeltalogs()[0].GetBinlogs()[0].GetLogPath(), "retry input must retain its paths")
}

func TestL0ManifestRejectsChangedTargetBeforeBatchPublication(t *testing.T) {
	for _, drop := range []bool{false, true} {
		t.Run(fmt.Sprintf("drop=%v", drop), func(t *testing.T) {
			ctx := context.Background()
			mt, task, output := l0LockFixture(t, 200, 201)
			calls := 0
			var mu sync.Mutex
			patch := mockey.Mock(packed.AddDeltaLogsToManifestOverwrite).To(
				func(path string, _ *indexpb.StorageConfig, _ []packed.DeltaLogEntry) (string, error) {
					base, version, err := packed.UnmarshalManifestPath(path)
					mu.Lock()
					calls++
					change := calls == 1
					mu.Unlock()
					if change {
						if drop {
							err = mt.UpdateSegmentsInfo(ctx, UpdateStatusOperator(200, commonpb.SegmentState_Dropped))
						} else {
							err = mt.UpdateSegmentsInfo(ctx, UpdateManifestVersion(200, 9))
						}
					}
					return packed.MarshalManifestPath(base, version+1), err
				}).Build()
			defer patch.UnPatch()
			require.ErrorIs(t, task.saveSegmentMeta(output), merr.ErrServiceUnavailable)
			require.Equal(t, commonpb.SegmentState_Flushed, mt.GetSegment(ctx, 100).GetState())
			require.Empty(t, mt.GetSegment(ctx, 201).GetDeltalogs(), "other targets must not be published on conflict")
			require.NoError(t, task.saveSegmentMeta(output))
			require.Equal(t, commonpb.SegmentState_Dropped, mt.GetSegment(ctx, 100).GetState())
			if drop {
				require.Equal(t, commonpb.SegmentState_Dropped, mt.GetSegment(ctx, 200).GetState())
				// Preserve the existing behavior: a retired target can still
				// receive late L0 deltas needed by readers of that segment.
				require.EqualValues(t, 3, mt.GetSegment(ctx, 200).GetStats().GetDeleteNumRows())
			} else {
				base := "/tmp/milvus/insert_log/1/10/200"
				require.Equal(t, packed.MarshalManifestPath(base, 10), mt.GetSegment(ctx, 200).GetManifestPath())
			}
			require.EqualValues(t, 3, mt.GetSegment(ctx, 201).GetStats().GetDeleteNumRows())
		})
	}
}

func TestL0ManifestRetryAfterPartialCommitFailure(t *testing.T) {
	ctx := context.Background()
	mt, task, output := l0LockFixture(t, 200, 201)
	var mu sync.Mutex
	calls := make(map[string]int)
	failedBase := "/tmp/milvus/insert_log/1/10/201"
	patch := mockey.Mock(packed.AddDeltaLogsToManifestOverwrite).To(
		func(path string, _ *indexpb.StorageConfig, _ []packed.DeltaLogEntry) (string, error) {
			mu.Lock()
			defer mu.Unlock()
			calls[path]++
			base, version, err := packed.UnmarshalManifestPath(path)
			if base == failedBase && calls[path] == 1 {
				return "", merr.WrapErrServiceUnavailable("injected manifest failure")
			}
			return packed.MarshalManifestPath(base, version+1), err
		}).Build()
	defer patch.UnPatch()
	require.ErrorIs(t, task.saveSegmentMeta(output), merr.ErrServiceUnavailable)
	for _, id := range []int64{200, 201} {
		require.Empty(t, mt.GetSegment(ctx, id).GetDeltalogs())
	}
	require.Equal(t, commonpb.SegmentState_Flushed, mt.GetSegment(ctx, 100).GetState())
	require.NoError(t, task.saveSegmentMeta(output))
	require.Equal(t, 1, calls[packed.MarshalManifestPath("/tmp/milvus/insert_log/1/10/200", 7)])
	require.Equal(t, 2, calls[packed.MarshalManifestPath(failedBase, 7)])
	for _, id := range []int64{200, 201} {
		require.EqualValues(t, 3, mt.GetSegment(ctx, id).GetStats().GetDeleteNumRows())
	}
	require.Equal(t, commonpb.SegmentState_Dropped, mt.GetSegment(ctx, 100).GetState())
}

// Only scheduling is intercepted: both competing commits and the retry use
// real local manifests, and the final contents must retain both delta logs.
func TestL0ManifestConcurrentRetryPreservesManifestContents(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: root}
	base := path.Join(root, "insert_log/1/10/200")
	initial, err := packed.CommitManifestUpdates(base, packed.ManifestEarliest, cfg, &packed.ManifestUpdates{
		DeltaLogs: []packed.DeltaLogEntry{{Path: base + "/_delta/seed", NumEntries: 0}},
	})
	require.NoError(t, err)
	mt, err := newMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, mt.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID: 200, State: commonpb.SegmentState_Flushed, ManifestPath: initial,
	})))
	makeOp := func(id int64) UpdateOperator {
		return AddL0DeltalogsAndUpdateManifestOperator(200, []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{
			LogID: id, LogPath: fmt.Sprintf("%s/_delta/%d", base, id), EntriesNum: 1,
		}}}}, cfg, nil)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	var original func(string, *indexpb.StorageConfig, []packed.DeltaLogEntry) (string, error)
	patch := mockey.Mock(packed.AddDeltaLogsToManifestOverwrite).To(
		func(manifest string, config *indexpb.StorageConfig, entries []packed.DeltaLogEntry) (string, error) {
			if manifest == initial && entries[0].Path == base+"/_delta/9001" {
				close(entered)
				<-release
			}
			return original(manifest, config, entries)
		}).Origin(&original).Build()
	defer patch.UnPatch()
	defer unblock()
	first := make(chan error, 1)
	go func() { first <- mt.UpdateSegmentsInfo(ctx, makeOp(9001)) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first manifest commit did not start")
	}
	second := make(chan error, 1)
	go func() { second <- mt.UpdateSegmentsInfo(ctx, makeOp(9002)) }()
	require.NoError(t, awaitL0Result(t, second))
	unblock()
	require.ErrorIs(t, awaitL0Result(t, first), merr.ErrServiceUnavailable)
	require.NoError(t, mt.UpdateSegmentsInfo(ctx, makeOp(9001)))
	paths, err := packed.GetDeltaLogPathsFromManifest(mt.GetSegment(ctx, 200).GetManifestPath(), cfg)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{base + "/_delta/9001", base + "/_delta/9002"}, paths)
	require.EqualValues(t, 2, mt.GetSegment(ctx, 200).GetStats().GetDeleteNumRows())
}
