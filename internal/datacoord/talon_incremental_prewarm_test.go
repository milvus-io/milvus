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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestIncrementalPrewarmDelta(t *testing.T) {
	client := &recordingTalonPrewarm{stats: map[string]int{}}
	w := &talonIncrementalPrewarm{ctx: context.Background(), client: client, prefix: "s3://bucket/"}
	objects := []packed.PrewarmObject{{Path: "data", Size: 42}, {Path: "empty", Size: 0}, {Path: "data", Size: 42}, {Path: "manifest", Size: 7}}
	require.NoError(t, w.prewarm(incrementalPrewarmCommit{objects: objects}))
	require.Empty(t, client.stats)
	require.Equal(t, [][]TalonPrewarmRequest{{{URI: "s3://bucket/data", Size: 42}, {URI: "s3://bucket/empty", Size: 0}, {URI: "s3://bucket/manifest", Size: 7}}}, client.batches)
}

func TestIncrementalPrewarmContinuesAfterBatchFailure(t *testing.T) {
	client := &recordingTalonPrewarm{batchFailure: true}
	w := &talonIncrementalPrewarm{ctx: context.Background(), client: client, prefix: "s3://bucket/"}
	objects := make([]packed.PrewarmObject, 2049)
	for i := range objects {
		objects[i] = packed.PrewarmObject{Path: strconv.Itoa(i), Size: uint64(i)}
	}
	require.Error(t, w.prewarm(incrementalPrewarmCommit{objects: objects}))
	require.Len(t, client.batches, 3)
	require.Len(t, client.batches[0], 1024)
	require.Len(t, client.batches[1], 1024)
	require.Len(t, client.batches[2], 1)
	require.Equal(t, "s3://bucket/2048", client.batches[2][0].URI)
}

func TestIncrementalPrewarmAdmission(t *testing.T) {
	w := &talonIncrementalPrewarm{ctx: context.Background(), queue: make(chan incrementalPrewarmCommit, 3)}
	m := &meta{ctx: context.Background(), incrementalPrewarm: w}
	before := packed.MarshalManifestPath("files/segment", 1)
	after := packed.MarshalManifestPath("files/segment", 2)
	for _, state := range []commonpb.SegmentState{commonpb.SegmentState_Growing, commonpb.SegmentState_Sealed, commonpb.SegmentState_Flushing, commonpb.SegmentState_Flushed, commonpb.SegmentState_Dropped} {
		m.prewarmManifestCommit(before, NewSegmentInfo(&datapb.SegmentInfo{ID: 1, State: state, ManifestPath: after}), ManifestMutation{}, 32)
	}
	require.Len(t, w.queue, 3)
	done := make(chan struct{})
	go func() {
		m.prewarmManifestCommit(before, NewSegmentInfo(&datapb.SegmentInfo{ID: 1, State: commonpb.SegmentState_Flushed, ManifestPath: after}), ManifestMutation{}, 32)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("full prewarm queue blocked publication")
	}
	require.Len(t, w.queue, 3)
	m.prewarmManifestCommit(after, NewSegmentInfo(&datapb.SegmentInfo{ID: 1, State: commonpb.SegmentState_Flushed, ManifestPath: after}), ManifestMutation{}, 32)
	require.Len(t, w.queue, 3)
}

func TestIncrementalPrewarmCopiesCommitObjects(t *testing.T) {
	w := &talonIncrementalPrewarm{ctx: context.Background(), queue: make(chan incrementalPrewarmCommit, 1)}
	m := &meta{ctx: context.Background(), incrementalPrewarm: w}
	segment := NewSegmentInfo(&datapb.SegmentInfo{ID: 1, State: commonpb.SegmentState_Flushed, ManifestPath: packed.MarshalManifestPath("files/segment", 2)})
	updates := &packed.ManifestUpdates{Stats: []packed.StatEntry{{Files: []string{"files/segment/stats"}, FileSizes: map[string]uint64{"files/segment/stats": 17}}}}
	m.prewarmManifestCommit(packed.MarshalManifestPath("files/segment", 1), segment, ManifestMutation{Updates: updates}, 32)
	updates.Stats[0].Files[0] = "changed"
	updates.Stats[0].FileSizes["files/segment/stats"] = 100
	queued := <-w.queue
	manifest, err := packed.ManifestFilePath(segment.GetManifestPath())
	require.NoError(t, err)
	require.Equal(t, []packed.PrewarmObject{{Path: "files/segment/stats", Size: 17}, {Path: manifest, Size: 32}}, queued.objects)
}

func TestIncrementalPrewarmSkipsUnknownSizeWithoutStat(t *testing.T) {
	w := &talonIncrementalPrewarm{ctx: context.Background(), queue: make(chan incrementalPrewarmCommit, 1)}
	m := &meta{ctx: context.Background(), incrementalPrewarm: w}
	segment := NewSegmentInfo(&datapb.SegmentInfo{ID: 1, State: commonpb.SegmentState_Flushed, ManifestPath: packed.MarshalManifestPath("files/segment", 2)})
	m.prewarmManifestCommit(packed.MarshalManifestPath("files/segment", 1), segment, ManifestMutation{Updates: &packed.ManifestUpdates{ColumnGroups: []packed.ColumnGroupEntry{{Files: []packed.ColumnGroupFileEntry{{Path: "unknown"}}}}}}, 32)
	queued := <-w.queue
	require.Len(t, queued.objects, 1)
	require.EqualValues(t, 32, queued.objects[0].Size)
}

func TestIncrementalPrewarmPublicationDoesNotWaitForQueue(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(strconv.FormatBool(batch), func(t *testing.T) {
			m, err := newMemoryMeta(t)
			require.NoError(t, err)
			w := &talonIncrementalPrewarm{ctx: context.Background(), queue: make(chan incrementalPrewarmCommit, 1)}
			m.incrementalPrewarm = w
			before := packed.MarshalManifestPath("files/segment", 1)
			after := packed.MarshalManifestPath("files/segment", 2)
			require.NoError(t, m.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{ID: 1, State: commonpb.SegmentState_Flushed, StorageVersion: storage.StorageV3, ManifestPath: before})))
			commit := SegmentManifestCommit{SegmentID: 1, Mutation: ManifestMutation{Type: ManifestMutationNoop, ManifestPath: after, ManifestSize: 77, NewObjects: []packed.PrewarmObject{{Path: "data", Size: 123}}}}
			publish := func(c SegmentManifestCommit) error {
				if batch {
					return m.CommitSegmentManifests(context.Background(), []SegmentManifestCommit{c})
				}
				return m.CommitSegmentManifest(context.Background(), c)
			}
			require.NoError(t, publish(commit))
			require.Len(t, w.queue, 1)
			event := <-w.queue
			require.Equal(t, packed.PrewarmObject{Path: "data", Size: 123}, event.objects[0])
			require.EqualValues(t, 77, event.objects[1].Size)
			w.queue <- event
			commit.Mutation.ManifestPath = packed.MarshalManifestPath("files/segment", 3)
			done := make(chan error, 1)
			go func() { done <- publish(commit) }()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("manifest publication waited for prewarm queue")
			}
			require.Equal(t, commit.Mutation.ManifestPath, m.GetSegment(context.Background(), 1).GetManifestPath())
			require.Len(t, w.queue, 1)
		})
	}
}
