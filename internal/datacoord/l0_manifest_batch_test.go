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
	"fmt"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Existing operator tests supply a blocking fake. Only this test adapter uses
// a goroutine to model native callback delivery; production submits directly.
func mockL0ManifestSubmissions(commit func(context.Context, *packed.ManifestIOContext, string, *indexpb.StorageConfig, []packed.DeltaLogEntry) (string, error)) *mockey.Mocker {
	return mockey.Mock(packed.SubmitManifestUpdates).To(func(ctx context.Context, io *packed.ManifestIOContext, base string, version int64, cfg *indexpb.StorageConfig, updates *packed.ManifestUpdates, complete func(packed.ManifestUpdateResult, error)) error {
		go func() {
			manifest, err := commit(ctx, io, packed.MarshalManifestPath(base, version), cfg, updates.DeltaLogs)
			complete(packed.ManifestUpdateResult{ManifestPath: manifest}, err)
		}()
		return nil
	}).Build()
}

func TestL0ManifestBatchSingleWorker(t *testing.T) {
	executor := newManifestCommitExecutor(1)
	defer executor.close()
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}
	cache := make(map[int64]string)
	var updates []*l0ManifestUpdate
	for id := int64(1); id <= 4; id++ {
		base := fmt.Sprintf("%s/segment-%d", cfg.RootPath, id)
		initial, err := packed.CommitManifestUpdates(base, 0, cfg, &packed.ManifestUpdates{DeltaLogs: []packed.DeltaLogEntry{{Path: base + "/_delta/1", NumEntries: 1}}})
		require.NoError(t, err)
		segment := NewSegmentInfo(&datapb.SegmentInfo{ID: id, ManifestPath: initial})
		// Cached and empty steps must advance inline without losing the chain.
		updates = append(updates, &l0ManifestUpdate{segmentID: id, segment: segment, manifestPath: initial}, &l0ManifestUpdate{segmentID: id, segment: segment})
		for logID := 2; logID <= 3; logID++ {
			updates = append(updates, &l0ManifestUpdate{
				segmentID: id, segment: segment, storageConfig: cfg, committedV3Manifests: cache,
				entries: []packed.DeltaLogEntry{{Path: fmt.Sprintf("%s/_delta/%d", base, logID), NumEntries: 1}},
			})
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, commitL0ManifestUpdates(ctx, executor, updates))
	require.Len(t, cache, 4)
	for _, manifest := range cache {
		base, version, err := packed.UnmarshalManifestPath(manifest)
		require.NoError(t, err)
		require.EqualValues(t, 3, version)
		paths, err := packed.GetDeltaLogPathsFromManifest(manifest, cfg)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{base + "/_delta/1", base + "/_delta/2", base + "/_delta/3"}, paths)
	}
}

func TestL0ManifestBatchDrainsAndStopsChains(t *testing.T) {
	executor := newManifestCommitExecutor(1)
	defer executor.close()
	cache := make(map[int64]string)
	var updates []*l0ManifestUpdate
	for id := int64(1); id <= 3; id++ {
		segment := NewSegmentInfo(&datapb.SegmentInfo{ID: id, ManifestPath: packed.MarshalManifestPath(fmt.Sprintf("/tmp/l0/%d", id), 7)})
		for range 2 {
			updates = append(updates, &l0ManifestUpdate{segmentID: id, segment: segment, committedV3Manifests: cache, entries: []packed.DeltaLogEntry{{Path: "delta", NumEntries: 1}}})
		}
	}
	type submission struct {
		ctx      context.Context
		base     string
		complete func(packed.ManifestUpdateResult, error)
	}
	submitted := make(chan submission, 6)
	patch := mockey.Mock(packed.SubmitManifestUpdates).To(func(ctx context.Context, _ *packed.ManifestIOContext, base string, _ int64, _ *indexpb.StorageConfig, _ *packed.ManifestUpdates, complete func(packed.ManifestUpdateResult, error)) error {
		submitted <- submission{ctx, base, complete}
		return nil
	}).Build()
	defer patch.UnPatch()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- commitL0ManifestUpdates(ctx, executor, updates) }()
	pending := make([]submission, 0, 3)
	for range 3 {
		select {
		case request := <-submitted:
			pending = append(pending, request)
		case <-ctx.Done():
			t.Fatal("extra concurrency limit prevented submission")
		}
	}
	// An UNKNOWN result must stop all dependent revisions without replay.
	failure := &packed.ManifestCommitError{Outcome: packed.ManifestCommitUnknown, Err: merr.ErrServiceUnavailable}
	pending[0].complete(packed.ManifestUpdateResult{}, failure)
	require.ErrorIs(t, pending[1].ctx.Err(), context.Canceled)
	pending[1].complete(packed.ManifestUpdateResult{ManifestPath: packed.MarshalManifestPath(pending[1].base, 8)}, nil)
	select {
	case err := <-done:
		t.Fatalf("returned before final callback: %v", err)
	default:
	}
	pending[2].complete(packed.ManifestUpdateResult{}, context.Canceled)
	require.ErrorIs(t, <-done, failure)
	require.Len(t, cache, 1, "preserve only confirmed commits for catalog retry")
	select {
	case <-submitted:
		t.Fatal("submitted a dependent revision after failure")
	default:
	}
}

func TestL0ManifestBatchInlineAndRejected(t *testing.T) {
	for _, reject := range []bool{false, true} {
		t.Run(fmt.Sprint(reject), func(t *testing.T) {
			executor := newManifestCommitExecutor(1)
			defer executor.close()
			segment := NewSegmentInfo(&datapb.SegmentInfo{ID: 1, ManifestPath: packed.MarshalManifestPath("/tmp/l0/1", 7)})
			updates := []*l0ManifestUpdate{
				{segmentID: 1, segment: segment, entries: []packed.DeltaLogEntry{{Path: "a"}}},
				{segmentID: 1, segment: segment, entries: []packed.DeltaLogEntry{{Path: "b"}}},
			}
			calls := 0
			patch := mockey.Mock(packed.SubmitManifestUpdates).To(func(_ context.Context, _ *packed.ManifestIOContext, base string, version int64, _ *indexpb.StorageConfig, _ *packed.ManifestUpdates, complete func(packed.ManifestUpdateResult, error)) error {
				calls++
				if reject {
					return merr.ErrServiceUnavailable
				}
				require.EqualValues(t, 6+calls, version)
				complete(packed.ManifestUpdateResult{ManifestPath: packed.MarshalManifestPath(base, version+1)}, nil)
				return nil
			}).Build()
			defer patch.UnPatch()
			err := commitL0ManifestUpdates(context.Background(), executor, updates)
			if reject {
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
				require.Equal(t, 1, calls)
			} else {
				require.NoError(t, err)
				require.Equal(t, 2, calls)
			}
		})
	}
}
