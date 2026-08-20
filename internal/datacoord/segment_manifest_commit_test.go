// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	metastoremocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCommitSegmentManifestPublishesOnlyAfterCatalogSuccess(t *testing.T) {
	basePath := "/tmp/milvus/insert_log/1/10/200"
	oldManifest := packed.MarshalManifestPath(basePath, 7)
	newManifest := packed.MarshalManifestPath(basePath, 8)
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, meta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:             200,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		ManifestPath:   oldManifest,
	})))

	commit := mockey.Mock(packed.CommitManifestUpdates).To(
		func(base string, version int64, _ *indexpb.StorageConfig, updates *packed.ManifestUpdates) (string, error) {
			require.Equal(t, basePath, base)
			require.EqualValues(t, 7, version)
			require.Len(t, updates.DeltaLogs, 1)
			return newManifest, nil
		},
	).Build()
	defer commit.UnPatch()

	err = meta.CommitSegmentManifest(context.Background(), SegmentManifestCommit{
		SegmentID:     200,
		StorageConfig: &indexpb.StorageConfig{},
		Mutation: ManifestMutation{
			Type: ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{DeltaLogs: []packed.DeltaLogEntry{{
				Path:       basePath + "/_delta/9001",
				NumEntries: 3,
			}}},
		},
		CatalogMutation: SegmentCatalogMutation{Operators: []UpdateOperator{AddL0DeltalogsOperator(200, []*datapb.FieldBinlog{{
			Binlogs: []*datapb.Binlog{{LogID: 9001, LogPath: basePath + "/_delta/9001", EntriesNum: 3, MemorySize: 128}},
		}})}},
	})
	require.NoError(t, err)

	updated := meta.GetSegment(context.Background(), 200)
	require.Equal(t, newManifest, updated.GetManifestPath())
	require.EqualValues(t, 3, updated.GetStats().GetDeleteNumRows())
	require.Empty(t, updated.GetDeltalogs()[0].GetBinlogs()[0].GetLogPath())
	manifest9 := packed.MarshalManifestPath(basePath, 9)
	require.NoError(t, meta.CommitSegmentManifest(context.Background(), SegmentManifestCommit{
		SegmentID:        200,
		ExpectedManifest: newManifest,
		Mutation: ManifestMutation{
			Type:         ManifestMutationNoop,
			ManifestPath: manifest9,
		},
		CatalogMutation: SegmentCatalogMutation{Operators: []UpdateOperator{
			UpdateIsImporting(200, true),
		}},
	}))
	require.Equal(t, manifest9, meta.GetSegment(context.Background(), 200).GetManifestPath())
	require.True(t, meta.GetSegment(context.Background(), 200).GetIsImporting())

	// The stale caller must not publish a later transaction on the old base.
	err = meta.CommitSegmentManifest(context.Background(), SegmentManifestCommit{
		SegmentID:        200,
		ExpectedManifest: oldManifest,
		Mutation: ManifestMutation{
			Type:         ManifestMutationNoop,
			ManifestPath: packed.MarshalManifestPath(basePath, 10),
		},
	})
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.ErrorIs(t, err, errSegmentManifestStale)
	require.Equal(t, manifest9, meta.GetSegment(context.Background(), 200).GetManifestPath())

	err = meta.CommitSegmentManifest(context.Background(), SegmentManifestCommit{
		SegmentID:        200,
		ExpectedManifest: manifest9,
		Mutation: ManifestMutation{
			Type:         ManifestMutationNoop,
			ManifestPath: newManifest,
		},
	})
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.ErrorIs(t, err, errSegmentManifestStale)
	require.Equal(t, manifest9, meta.GetSegment(context.Background(), 200).GetManifestPath())
}

func TestCommitSegmentManifestAllowsOmittedExpectedManifest(t *testing.T) {
	basePath := "/tmp/milvus/insert_log/1/10/209"
	manifest7 := packed.MarshalManifestPath(basePath, 7)
	manifest8 := packed.MarshalManifestPath(basePath, 8)
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, meta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:             209,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		ManifestPath:   manifest7,
	})))

	// No ExpectedManifest means this caller deliberately opts out of pointer
	// CAS, while the per-segment transaction lock still serializes publication.
	require.NoError(t, meta.CommitSegmentManifest(context.Background(), SegmentManifestCommit{
		SegmentID: 209,
		Mutation: ManifestMutation{
			Type:         ManifestMutationNoop,
			ManifestPath: manifest8,
		},
	}))
	require.Equal(t, manifest8, meta.GetSegment(context.Background(), 209).GetManifestPath())

	manifest9 := packed.MarshalManifestPath(basePath, 9)
	require.NoError(t, meta.CommitSegmentManifest(context.Background(), SegmentManifestCommit{
		SegmentID: 209,
		Mutation: ManifestMutation{
			Type:         ManifestMutationNoop,
			ManifestPath: manifest9,
		},
		CatalogMutation: SegmentCatalogMutation{Operators: []UpdateOperator{
			UpdateIsImporting(209, true),
		}},
	}))
	updated := meta.GetSegment(context.Background(), 209)
	require.Equal(t, manifest9, updated.GetManifestPath())
	require.True(t, updated.GetIsImporting())
}

func TestCommitSegmentManifestCreatesSegmentWithInitialPointer(t *testing.T) {
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	manifest := packed.MarshalManifestPath("/tmp/milvus/insert_log/1/10/210", 1)

	require.NoError(t, meta.CommitSegmentManifest(context.Background(), SegmentManifestCommit{
		SegmentID: 210,
		Mutation: ManifestMutation{
			Type:         ManifestMutationNoop,
			ManifestPath: manifest,
		},
		CatalogMutation: SegmentCatalogMutation{NewSegment: &datapb.SegmentInfo{
			ID:             210,
			State:          commonpb.SegmentState_Flushed,
			StorageVersion: storage.StorageV3,
		}},
	}))

	segment := meta.GetSegment(context.Background(), 210)
	require.NotNil(t, segment)
	require.Equal(t, manifest, segment.GetManifestPath())
	require.Equal(t, storage.StorageV3, segment.GetStorageVersion())
}

func TestCommitSegmentManifestLeavesMemoryUntouchedOnCatalogFailure(t *testing.T) {
	basePath := "/tmp/milvus/insert_log/1/10/201"
	oldManifest := packed.MarshalManifestPath(basePath, 7)
	newManifest := packed.MarshalManifestPath(basePath, 8)
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, meta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:             201,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		ManifestPath:   oldManifest,
	})))
	catalog := metastoremocks.NewDataCoordCatalog(t)
	catalog.EXPECT().Update(mock.Anything, mock.Anything).Return(merr.WrapErrServiceUnavailableMsg("catalog unavailable")).Once()
	meta.catalog = catalog

	commit := mockey.Mock(packed.CommitManifestUpdates).Return(newManifest, nil).Build()
	defer commit.UnPatch()

	err = meta.CommitSegmentManifest(context.Background(), SegmentManifestCommit{
		SegmentID:     201,
		StorageConfig: &indexpb.StorageConfig{},
		Mutation: ManifestMutation{
			Type:    ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{DeltaLogs: []packed.DeltaLogEntry{{Path: basePath + "/_delta/9001", NumEntries: 1}}},
		},
	})
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Equal(t, oldManifest, meta.GetSegment(context.Background(), 201).GetManifestPath())
}

func TestCommitSegmentManifestDoesNotSerializeDifferentSegmentsDuringManifestIO(t *testing.T) {
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	basePaths := map[int64]string{
		202: "/tmp/milvus/insert_log/1/10/202",
		203: "/tmp/milvus/insert_log/1/10/203",
	}
	for segmentID, basePath := range basePaths {
		require.NoError(t, meta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
			ID:             segmentID,
			State:          commonpb.SegmentState_Flushed,
			StorageVersion: storage.StorageV3,
			ManifestPath:   packed.MarshalManifestPath(basePath, 1),
		})))
	}

	entered := make(chan string, 2)
	release := make(chan struct{})
	commit := mockey.Mock(packed.CommitManifestUpdates).To(
		func(base string, version int64, _ *indexpb.StorageConfig, _ *packed.ManifestUpdates) (string, error) {
			entered <- base
			<-release
			return packed.MarshalManifestPath(base, version+1), nil
		},
	).Build()
	defer commit.UnPatch()

	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for segmentID, basePath := range basePaths {
		wg.Add(1)
		go func(segmentID int64, basePath string) {
			defer wg.Done()
			errs <- meta.CommitSegmentManifest(context.Background(), SegmentManifestCommit{
				SegmentID:     segmentID,
				StorageConfig: &indexpb.StorageConfig{},
				Mutation: ManifestMutation{
					Type:    ManifestMutationCommitUpdates,
					Updates: &packed.ManifestUpdates{DeltaLogs: []packed.DeltaLogEntry{{Path: basePath + "/_delta/1", NumEntries: 1}}},
				},
			})
		}(segmentID, basePath)
	}

	for range basePaths {
		select {
		case <-entered:
		case <-time.After(time.Second):
			require.FailNow(t, "different segments blocked before manifest I/O")
		}
	}
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func TestCommitSegmentManifestRebasesCatalogMutationAfterManifestIO(t *testing.T) {
	const segmentID = 208
	basePath := "/tmp/milvus/insert_log/1/10/208"
	oldManifest := packed.MarshalManifestPath(basePath, 1)
	newManifest := packed.MarshalManifestPath(basePath, 2)
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, meta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:             segmentID,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		ManifestPath:   oldManifest,
	})))

	entered := make(chan struct{})
	release := make(chan struct{})
	commit := mockey.Mock(packed.CommitManifestUpdates).To(
		func(string, int64, *indexpb.StorageConfig, *packed.ManifestUpdates) (string, error) {
			close(entered)
			<-release
			return newManifest, nil
		},
	).Build()
	defer commit.UnPatch()

	result := make(chan error, 1)
	go func() {
		result <- meta.CommitSegmentManifest(context.Background(), SegmentManifestCommit{
			SegmentID:     segmentID,
			StorageConfig: &indexpb.StorageConfig{},
			Mutation: ManifestMutation{
				Type:    ManifestMutationCommitUpdates,
				Updates: &packed.ManifestUpdates{},
			},
		})
	}()

	<-entered
	require.NoError(t, meta.UpdateSegmentsInfo(context.Background(), UpdateIsImporting(segmentID, true)))
	close(release)
	require.NoError(t, <-result)

	updated := meta.GetSegment(context.Background(), segmentID)
	require.Equal(t, newManifest, updated.GetManifestPath())
	require.True(t, updated.GetIsImporting())
}

// A CAS-free commit (the stats path) whose manifest pointer is advanced mid-I/O by
// an out-of-lock writer (the DDL/backfill ack adopting an externally minted version)
// must fail stale instead of publishing: the loon transaction does not merge the
// concurrent revision, and the prepared version (base+2 here, loon skips past the
// concurrent one) passes the monotonic guard, so only the base-stability check
// stands between publication and silently dropping the concurrent revision.
func TestCommitSegmentManifestFailsStaleWhenPointerAdvancesDuringManifestIO(t *testing.T) {
	const segmentID = 212
	basePath := "/tmp/milvus/insert_log/1/10/212"
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, meta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:             segmentID,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		ManifestPath:   packed.MarshalManifestPath(basePath, 7),
	})))

	entered := make(chan struct{})
	release := make(chan struct{})
	commit := mockey.Mock(packed.CommitManifestUpdates).To(
		func(base string, version int64, _ *indexpb.StorageConfig, _ *packed.ManifestUpdates) (string, error) {
			close(entered)
			<-release
			return packed.MarshalManifestPath(base, version+2), nil
		},
	).Build()
	defer commit.UnPatch()

	result := make(chan error, 1)
	go func() {
		// A structured commit (the stats shape) carries no CAS by contract, so the
		// base-stability check is the only guard against the mid-I/O movement.
		result <- meta.CommitSegmentManifest(context.Background(), SegmentManifestCommit{
			SegmentID:     segmentID,
			StorageConfig: &indexpb.StorageConfig{},
			Mutation: ManifestMutation{
				Type:    ManifestMutationCommitUpdates,
				Updates: &packed.ManifestUpdates{},
			},
			CatalogMutation: SegmentCatalogMutation{Operators: []UpdateOperator{
				UpdateIsImporting(segmentID, true),
			}},
		})
	}()

	<-entered
	// The real out-of-lock writer: the batch-update-manifest ack adopting v8.
	require.NoError(t, meta.UpdateSegmentsInfo(context.Background(), UpdateManifestVersion(segmentID, 8)))
	close(release)

	err = <-result
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.ErrorIs(t, err, errSegmentManifestStale)

	// The ack's pointer survives and nothing from the aborted commit leaks out.
	updated := meta.GetSegment(context.Background(), segmentID)
	require.Equal(t, packed.MarshalManifestPath(basePath, 8), updated.GetManifestPath())
	require.False(t, updated.GetIsImporting())
}

func TestCommitSegmentManifestSerializesCatalogWritesForDifferentSegments(t *testing.T) {
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	basePaths := map[int64]string{
		206: "/tmp/milvus/insert_log/1/10/206",
		207: "/tmp/milvus/insert_log/1/10/207",
	}
	for segmentID, basePath := range basePaths {
		require.NoError(t, meta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
			ID:             segmentID,
			State:          commonpb.SegmentState_Flushed,
			StorageVersion: storage.StorageV3,
			ManifestPath:   packed.MarshalManifestPath(basePath, 1),
		})))
	}

	entered := make(chan struct{}, len(basePaths))
	release := make(chan struct{})
	meta.catalog = &blockingManifestCatalog{
		entered: entered,
		release: release,
	}

	errs := make(chan error, len(basePaths))
	for segmentID, basePath := range basePaths {
		go func(segmentID int64, basePath string) {
			errs <- meta.CommitSegmentManifest(context.Background(), SegmentManifestCommit{
				SegmentID:        segmentID,
				ExpectedManifest: packed.MarshalManifestPath(basePath, 1),
				Mutation: ManifestMutation{
					Type:         ManifestMutationNoop,
					ManifestPath: packed.MarshalManifestPath(basePath, 2),
				},
			})
		}(segmentID, basePath)
	}

	select {
	case <-entered:
	case <-time.After(time.Second):
		require.FailNow(t, "first catalog write did not start")
	}
	select {
	case <-entered:
		require.FailNow(t, "different segment entered catalog write while segMu was held")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	for range basePaths {
		require.NoError(t, <-errs)
	}
}

type blockingManifestCatalog struct {
	metastore.DataCoordCatalog
	entered chan<- struct{}
	release <-chan struct{}
}

func (c *blockingManifestCatalog) Update(context.Context, ...metastore.UpdateAction) error {
	c.entered <- struct{}{}
	<-c.release
	return nil
}

// Two structured commits for the same segment are serialized by the per-segment
// manifest lock, and the queued one is generated from the pointer the first
// published — the in-lock base is the sole authority, so a queued CommitUpdates
// caller rebases instead of failing a caller-pinned CAS.
func TestCommitSegmentManifestSerializesSameSegment(t *testing.T) {
	const segmentID = 204
	basePath := "/tmp/milvus/insert_log/1/10/204"
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, meta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:             segmentID,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		ManifestPath:   packed.MarshalManifestPath(basePath, 1),
	})))

	versions := make(chan int64, 2)
	firstEntered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	commit := mockey.Mock(packed.CommitManifestUpdates).To(
		func(base string, version int64, _ *indexpb.StorageConfig, _ *packed.ManifestUpdates) (string, error) {
			versions <- version
			once.Do(func() {
				close(firstEntered)
				<-release
			})
			return packed.MarshalManifestPath(base, version+1), nil
		},
	).Build()
	defer commit.UnPatch()

	request := SegmentManifestCommit{
		SegmentID:     segmentID,
		StorageConfig: &indexpb.StorageConfig{},
		Mutation: ManifestMutation{
			Type:    ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{},
		},
	}
	results := make(chan error, 2)
	go func() { results <- meta.CommitSegmentManifest(context.Background(), request) }()
	<-firstEntered
	go func() { results <- meta.CommitSegmentManifest(context.Background(), request) }()
	close(release)

	require.NoError(t, <-results)
	require.NoError(t, <-results)
	// The queued transaction saw the first's published pointer as its base — it
	// was serialized behind the lock, not run against the stale snapshot.
	require.Equal(t, int64(1), <-versions)
	require.Equal(t, int64(2), <-versions)
	require.Equal(t, packed.MarshalManifestPath(basePath, 3), meta.GetSegment(context.Background(), segmentID).GetManifestPath())
}

// A structured mutation must not pin ExpectedManifest: its base is whatever is
// current under the commit lock, so a caller-pinned pointer is rejected outright
// rather than silently honored as a CAS.
func TestCommitSegmentManifestRejectsExpectedManifestOnStructuredMutation(t *testing.T) {
	basePath := "/tmp/milvus/insert_log/1/10/213"
	manifest7 := packed.MarshalManifestPath(basePath, 7)
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, meta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:             213,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		ManifestPath:   manifest7,
	})))

	err = meta.CommitSegmentManifest(context.Background(), SegmentManifestCommit{
		SegmentID:        213,
		ExpectedManifest: manifest7,
		StorageConfig:    &indexpb.StorageConfig{},
		Mutation: ManifestMutation{
			Type:    ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{},
		},
	})
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.Equal(t, manifest7, meta.GetSegment(context.Background(), 213).GetManifestPath())
}

// A StorageV3 segment's manifest is advanced inline via UpdateManifest by its
// single-writer flush path (SaveBinlogPaths, serialized by the segment's single
// WAL owner). There is no concurrent writer, so UpdateManifest does not reject
// the advancement and no CommitSegmentManifest serialization is required.
func TestUpdateManifestAllowsStorageV3Advancement(t *testing.T) {
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	oldManifest := packed.MarshalManifestPath("/tmp/milvus/insert_log/1/10/205", 1)
	newManifest := packed.MarshalManifestPath("/tmp/milvus/insert_log/1/10/205", 2)
	require.NoError(t, meta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:             205,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		ManifestPath:   oldManifest,
	})))

	err = meta.UpdateSegmentsInfo(context.Background(), UpdateManifest(205, newManifest))
	require.NoError(t, err)
	require.Equal(t, newManifest, meta.GetSegment(context.Background(), 205).GetManifestPath())
}

// A fresh StorageV3 segment (copy/import target) has no manifest yet; its first
// publication carries a complete worker-produced pointer with no DataCoord-side
// manifest I/O, so UpdateManifest sets it inline without CommitSegmentManifest.
func TestUpdateManifestAllowsStorageV3FirstPublication(t *testing.T) {
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	firstManifest := packed.MarshalManifestPath("/tmp/milvus/insert_log/1/10/206", 1)
	require.NoError(t, meta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:             206,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		// No ManifestPath: this is the segment's first publication.
	})))

	err = meta.UpdateSegmentsInfo(context.Background(), UpdateManifest(206, firstManifest))
	require.NoError(t, err)
	require.Equal(t, firstManifest, meta.GetSegment(context.Background(), 206).GetManifestPath())
}

// A segment retired by compaction while its stats task was still running is
// still present in meta (not yet GC'd) but Dropped. Publication must not advance
// its pointer, and the rejection must be ErrSegmentNotFound (terminal, not
// retriable) rather than an unclassified internal error, so the stats caller
// discards the obsolete worker result instead of re-polling the task forever.
func TestCommitSegmentManifestRejectsDroppedSegmentAsNotFound(t *testing.T) {
	basePath := "/tmp/milvus/insert_log/1/10/230"
	manifest7 := packed.MarshalManifestPath(basePath, 7)
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, meta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:             230,
		State:          commonpb.SegmentState_Dropped,
		StorageVersion: storage.StorageV3,
		ManifestPath:   manifest7,
	})))

	err = meta.CommitSegmentManifest(context.Background(), SegmentManifestCommit{
		SegmentID:        230,
		ExpectedManifest: manifest7,
		Mutation: ManifestMutation{
			Type:         ManifestMutationNoop,
			ManifestPath: packed.MarshalManifestPath(basePath, 8),
		},
	})
	require.ErrorIs(t, err, merr.ErrSegmentNotFound)
	require.Equal(t, manifest7, meta.GetSegment(context.Background(), 230).GetManifestPath())
}

// shouldPublishPreparedManifest is the guard that keeps a dropped segment from
// entering CommitSegmentManifest in the first place: GetSegment returns dropped
// segments, so without the health check a retired segment would take the
// prepared-commit path and fail. A healthy segment with a newer worker manifest
// still takes it.
func TestShouldPublishPreparedManifestSkipsUnhealthySegment(t *testing.T) {
	base := "/tmp/milvus/insert_log/1/10/"
	mt, err := newMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, mt.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:             231,
		State:          commonpb.SegmentState_Dropped,
		StorageVersion: storage.StorageV3,
		ManifestPath:   packed.MarshalManifestPath(base+"231", 7),
	})))
	require.NoError(t, mt.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:             232,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		ManifestPath:   packed.MarshalManifestPath(base+"232", 7),
	})))

	st := &statsTask{meta: mt}
	require.False(t, st.shouldPublishPreparedManifest(context.Background(), 231,
		&workerpb.StatsResult{Manifest: packed.MarshalManifestPath(base+"231", 8)}))
	require.True(t, st.shouldPublishPreparedManifest(context.Background(), 232,
		&workerpb.StatsResult{Manifest: packed.MarshalManifestPath(base+"232", 8)}))
}

// TestCommitSegmentManifestPublishesIndexTaskAtomically proves the index task
// record and the manifest pointer that publishes its artifact land in one
// catalog transaction, and that in-memory index metadata is installed only
// after that write.
func TestCommitSegmentManifestPublishesIndexTaskAtomically(t *testing.T) {
	const (
		collectionID = int64(1)
		partitionID  = int64(10)
		segmentID    = int64(210)
		indexID      = int64(101)
		buildID      = int64(102)
	)
	basePath := "/tmp/milvus/insert_log/1/10/210"
	oldManifest := packed.MarshalManifestPath(basePath, 3)
	newManifest := packed.MarshalManifestPath(basePath, 4)

	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, meta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:             segmentID,
		CollectionID:   collectionID,
		PartitionID:    partitionID,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		ManifestPath:   oldManifest,
	})))
	require.NoError(t, meta.indexMeta.AddSegmentIndex(context.Background(), &model.SegmentIndex{
		CollectionID:          collectionID,
		PartitionID:           partitionID,
		SegmentID:             segmentID,
		IndexID:               indexID,
		BuildID:               buildID,
		IndexVersion:          4,
		IndexState:            commonpb.IndexState_InProgress,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}))

	commit := mockey.Mock(packed.CommitManifestUpdates).To(
		func(base string, version int64, _ *indexpb.StorageConfig, updates *packed.ManifestUpdates) (string, error) {
			// The transaction opens at the segment's currently published
			// revision, not at whatever revision the build was issued against.
			require.Equal(t, basePath, base)
			require.EqualValues(t, 3, version)
			require.Len(t, updates.Indexes, 1)
			require.EqualValues(t, indexID, updates.Indexes[0].IndexID)
			return newManifest, nil
		},
	).Build()
	defer commit.UnPatch()

	taskInfo := &workerpb.IndexTaskInfo{
		BuildID:               buildID,
		State:                 commonpb.IndexState_Finished,
		IndexFileKeys:         []string{"0", "1"},
		SerializedSize:        2000,
		MemSize:               3000,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
	}
	require.NoError(t, meta.CommitSegmentManifest(context.Background(), SegmentManifestCommit{
		SegmentID:     segmentID,
		StorageConfig: &indexpb.StorageConfig{},
		Mutation: ManifestMutation{
			Type: ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{Indexes: []packed.ManifestIndexInfo{{
				IndexID: indexID,
				BuildID: buildID,
			}}},
		},
		CatalogMutation: SegmentCatalogMutation{
			SegmentIndex: &SegmentIndexMutation{
				Type:         SegmentIndexUpsert,
				BuildID:      buildID,
				FinishedTask: taskInfo,
			},
		},
	}))

	require.Equal(t, newManifest, meta.GetSegment(context.Background(), segmentID).GetManifestPath())
	published, ok := meta.indexMeta.GetIndexJob(buildID)
	require.True(t, ok)
	require.Equal(t, commonpb.IndexState_Finished, published.IndexState)
	require.Equal(t, []string{"0", "1"}, published.IndexFileKeys)
	require.EqualValues(t, 2000, published.IndexSerializedSize)
}

// A worker result for a task that was deleted mid-flight must not advance the
// pointer: publishing it would strand a manifest index entry with no
// SegmentIndex record to drive its GC.
func TestCommitSegmentManifestDiscardsResultForDeletedIndexTask(t *testing.T) {
	basePath := "/tmp/milvus/insert_log/1/10/211"
	oldManifest := packed.MarshalManifestPath(basePath, 3)
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, meta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:             211,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		ManifestPath:   oldManifest,
	})))

	commit := mockey.Mock(packed.CommitManifestUpdates).
		Return(packed.MarshalManifestPath(basePath, 4), nil).Build()
	defer commit.UnPatch()

	require.NoError(t, meta.CommitSegmentManifest(context.Background(), SegmentManifestCommit{
		SegmentID:     211,
		StorageConfig: &indexpb.StorageConfig{},
		Mutation: ManifestMutation{
			Type:    ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{Indexes: []packed.ManifestIndexInfo{{IndexID: 1, BuildID: 999}}},
		},
		CatalogMutation: SegmentCatalogMutation{
			SegmentIndex: &SegmentIndexMutation{
				Type:    SegmentIndexUpsert,
				BuildID: 999,
				FinishedTask: &workerpb.IndexTaskInfo{
					BuildID: 999,
					State:   commonpb.IndexState_Finished,
				},
			},
		},
	}))
	require.Equal(t, oldManifest, meta.GetSegment(context.Background(), 211).GetManifestPath())
}

// The batch path publishes through UpdateSegmentsInfo, which writes segment
// records only. It cannot stage the SegmentIndex action alongside them the way
// the single-segment commit does, so accepting a SegmentIndex mutation there
// would advance the manifest pointer while silently dropping the index record
// change. Reject it up front instead.
func TestCommitSegmentManifestsRejectsSegmentIndexMutation(t *testing.T) {
	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	commit := commitUpdates(213, "/tmp/milvus/insert_log/1/10/213")
	commit.CatalogMutation.SegmentIndex = &SegmentIndexMutation{
		Type:    SegmentIndexUpsert,
		BuildID: 1,
		FinishedTask: &workerpb.IndexTaskInfo{
			BuildID: 1,
			State:   commonpb.IndexState_Finished,
		},
	}
	err = meta.CommitSegmentManifests(context.Background(), []SegmentManifestCommit{commit})
	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

// The GC counterpart of the index-task commit: the revision that retracts an
// index artifact and the removal of the SegmentIndex record claiming it must
// become visible together, so a reader can never load an index whose artifact
// the published manifest no longer carries.
func TestCommitSegmentManifestRemovesSegmentIndexWithRetraction(t *testing.T) {
	const (
		collectionID = int64(1)
		partitionID  = int64(10)
		segmentID    = int64(214)
		indexID      = int64(700)
		buildID      = int64(701)
	)
	basePath := "/tmp/milvus/insert_log/1/10/214"
	oldManifest := packed.MarshalManifestPath(basePath, 3)
	newManifest := packed.MarshalManifestPath(basePath, 4)

	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, meta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:             segmentID,
		CollectionID:   collectionID,
		PartitionID:    partitionID,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		ManifestPath:   oldManifest,
	})))
	require.NoError(t, meta.indexMeta.AddSegmentIndex(context.Background(), &model.SegmentIndex{
		CollectionID:  collectionID,
		PartitionID:   partitionID,
		SegmentID:     segmentID,
		IndexID:       indexID,
		BuildID:       buildID,
		IndexVersion:  1,
		IndexState:    commonpb.IndexState_Finished,
		IndexFileKeys: []string{"0"},
	}))

	commit := mockey.Mock(packed.CommitManifestUpdates).To(
		func(base string, version int64, _ *indexpb.StorageConfig, updates *packed.ManifestUpdates) (string, error) {
			require.Equal(t, basePath, base)
			require.EqualValues(t, 3, version)
			require.Len(t, updates.DropIndexes, 1)
			require.EqualValues(t, indexID, updates.DropIndexes[0].IndexID)
			return newManifest, nil
		},
	).Build()
	defer commit.UnPatch()

	require.NoError(t, meta.CommitSegmentManifest(context.Background(), SegmentManifestCommit{
		SegmentID:     segmentID,
		StorageConfig: &indexpb.StorageConfig{},
		Mutation: ManifestMutation{
			Type: ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{
				DropIndexes: []packed.DropIndexEntry{{IndexID: indexID, ExpectedBuildID: buildID}},
			},
		},
		CatalogMutation: SegmentCatalogMutation{
			SegmentIndex: &SegmentIndexMutation{
				Type:    SegmentIndexRemove,
				BuildID: buildID,
			},
		},
	}))

	require.Equal(t, newManifest, meta.GetSegment(context.Background(), segmentID).GetManifestPath())
	_, ok := meta.indexMeta.GetIndexJob(buildID)
	require.False(t, ok)
	require.Empty(t, meta.indexMeta.GetSegmentIndexes(collectionID, segmentID))
}

// A removal whose record is already gone is not an error: the record's absence
// is the mutation's intended end state, and the manifest may still carry the
// entry an earlier interrupted attempt failed to retract. Publish anyway.
func TestCommitSegmentManifestRemovesMissingSegmentIndexStillPublishes(t *testing.T) {
	basePath := "/tmp/milvus/insert_log/1/10/215"
	oldManifest := packed.MarshalManifestPath(basePath, 3)
	newManifest := packed.MarshalManifestPath(basePath, 4)

	meta, err := newMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, meta.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
		ID:             215,
		State:          commonpb.SegmentState_Flushed,
		StorageVersion: storage.StorageV3,
		ManifestPath:   oldManifest,
	})))

	commit := mockey.Mock(packed.CommitManifestUpdates).Return(newManifest, nil).Build()
	defer commit.UnPatch()

	require.NoError(t, meta.CommitSegmentManifest(context.Background(), SegmentManifestCommit{
		SegmentID:     215,
		StorageConfig: &indexpb.StorageConfig{},
		Mutation: ManifestMutation{
			Type: ManifestMutationCommitUpdates,
			Updates: &packed.ManifestUpdates{
				DropIndexes: []packed.DropIndexEntry{{IndexID: 800, ExpectedBuildID: 801}},
			},
		},
		CatalogMutation: SegmentCatalogMutation{
			SegmentIndex: &SegmentIndexMutation{Type: SegmentIndexRemove, BuildID: 801},
		},
	}))
	require.Equal(t, newManifest, meta.GetSegment(context.Background(), 215).GetManifestPath())
}

// The mutation is the framework's only SegmentIndex contract, so its shape is
// validated before any manifest I/O rather than producing a half-applied
// commit.
func TestCommitSegmentManifestRejectsMalformedSegmentIndexMutation(t *testing.T) {
	basePath := "/tmp/milvus/insert_log/1/10/216"
	newMeta := func(t *testing.T) *meta {
		m, err := newMemoryMeta(t)
		require.NoError(t, err)
		require.NoError(t, m.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{
			ID:             216,
			State:          commonpb.SegmentState_Flushed,
			StorageVersion: storage.StorageV3,
			ManifestPath:   packed.MarshalManifestPath(basePath, 3),
		})))
		return m
	}

	cases := []struct {
		name     string
		mutation *SegmentIndexMutation
	}{
		{"no build ID", &SegmentIndexMutation{Type: SegmentIndexUpsert, FinishedTask: &workerpb.IndexTaskInfo{}}},
		{"unknown type", &SegmentIndexMutation{BuildID: 1}},
		{"upsert without task", &SegmentIndexMutation{Type: SegmentIndexUpsert, BuildID: 1}},
		{"upsert with mismatched task", &SegmentIndexMutation{
			Type: SegmentIndexUpsert, BuildID: 1,
			FinishedTask: &workerpb.IndexTaskInfo{BuildID: 2},
		}},
		{"removal with task", &SegmentIndexMutation{
			Type: SegmentIndexRemove, BuildID: 1,
			FinishedTask: &workerpb.IndexTaskInfo{BuildID: 1},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newMeta(t)
			commit := mockey.Mock(packed.CommitManifestUpdates).
				Return(packed.MarshalManifestPath(basePath, 4), nil).Build()
			defer commit.UnPatch()

			err := m.CommitSegmentManifest(context.Background(), SegmentManifestCommit{
				SegmentID:       216,
				StorageConfig:   &indexpb.StorageConfig{},
				Mutation:        ManifestMutation{Type: ManifestMutationCommitUpdates, Updates: &packed.ManifestUpdates{}},
				CatalogMutation: SegmentCatalogMutation{SegmentIndex: tc.mutation},
			})
			require.Error(t, err)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			// The pointer must not have advanced.
			require.Equal(t, packed.MarshalManifestPath(basePath, 3),
				m.GetSegment(context.Background(), 216).GetManifestPath())
		})
	}
}
