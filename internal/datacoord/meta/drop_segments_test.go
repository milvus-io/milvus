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

package meta

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

type segmentDropCatalog struct {
	metastore.DataCoordCatalog
	drop func(context.Context, []*datapb.SegmentInfo) error
}

func (c *segmentDropCatalog) DropSegments(ctx context.Context, segments []*datapb.SegmentInfo) error {
	return c.drop(ctx, segments)
}

func TestDropSegmentsRevalidatesImmutableProto(t *testing.T) {
	m, err := NewMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()
	for _, id := range []int64{1, 2, 3} {
		require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{ID: id, CollectionID: 10, PartitionID: 20, State: commonpb.SegmentState_Dropped})))
	}
	current, stale, live := m.GetSegment(ctx, 1), m.GetSegment(ctx, 2), m.GetSegment(ctx, 3)
	replacement := proto.Clone(stale.SegmentInfo).(*datapb.SegmentInfo)
	m.Segments.SetSegment(2, NewSegmentInfo(replacement))
	liveReplacement := proto.Clone(live.SegmentInfo).(*datapb.SegmentInfo)
	liveReplacement.State = commonpb.SegmentState_Flushed
	m.Segments.SetSegment(3, NewSegmentInfo(liveReplacement))
	// Reads wrap the same immutable proto in different DC-only objects.
	require.NotSame(t, current, m.GetSegment(ctx, 1))
	require.Same(t, current.SegmentInfo, m.GetSegment(ctx, 1).SegmentInfo)
	var deleted []*datapb.SegmentInfo
	m.Catalog = &segmentDropCatalog{DataCoordCatalog: m.Catalog, drop: func(_ context.Context, records []*datapb.SegmentInfo) error {
		deleted = records
		return nil
	}}
	count, err := m.DropSegments(ctx, []*SegmentInfo{current, stale, live, current, nil, NewSegmentInfo(&datapb.SegmentInfo{ID: 99})})
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Len(t, deleted, 1)
	require.EqualValues(t, 1, deleted[0].ID)
	require.Nil(t, m.GetSegment(ctx, 1))
	require.Same(t, replacement, m.GetSegment(ctx, 2).SegmentInfo)
	require.Same(t, liveReplacement, m.GetSegment(ctx, 3).SegmentInfo)
}

func TestDropSegmentsFailureKeepsMemoryForRetry(t *testing.T) {
	m, err := NewMemoryMeta(t)
	require.NoError(t, err)
	ctx := context.Background()
	for _, id := range []int64{1, 2} {
		require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{ID: id, CollectionID: 10, PartitionID: 20, State: commonpb.SegmentState_Dropped})))
	}
	candidates := []*SegmentInfo{m.GetSegment(ctx, 1), m.GetSegment(ctx, 2)}
	durable := map[int64]bool{1: true, 2: true}
	failure := errors.New("partial durable delete")
	calls := 0
	m.Catalog = &segmentDropCatalog{DataCoordCatalog: m.Catalog, drop: func(_ context.Context, records []*datapb.SegmentInfo) error {
		calls++
		delete(durable, records[0].ID)
		if calls == 1 {
			return failure
		}
		for _, record := range records {
			delete(durable, record.ID)
		}
		return nil
	}}
	count, err := m.DropSegments(ctx, candidates)
	require.ErrorIs(t, err, failure)
	require.Zero(t, count)
	require.Len(t, durable, 1)
	for _, candidate := range candidates {
		require.Same(t, candidate.SegmentInfo, m.GetSegment(ctx, candidate.ID).SegmentInfo)
	}
	count, err = m.DropSegments(ctx, candidates)
	require.NoError(t, err)
	require.Equal(t, 2, count)
	require.Empty(t, durable)
	for _, candidate := range candidates {
		require.Nil(t, m.GetSegment(ctx, candidate.ID))
	}
}

func TestDropSegmentsCanceledBeforePersistence(t *testing.T) {
	m, err := NewMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, m.AddSegment(context.Background(), NewSegmentInfo(&datapb.SegmentInfo{ID: 1, State: commonpb.SegmentState_Dropped})))
	m.Catalog = &segmentDropCatalog{drop: func(context.Context, []*datapb.SegmentInfo) error {
		t.Fatal("canceled deletion must not reach catalog")
		return nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	count, err := m.DropSegments(ctx, []*SegmentInfo{m.GetSegment(ctx, 1)})
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, count)
	require.NotNil(t, m.GetSegment(ctx, 1))
}

func TestDropSegmentsHoldsOnlyCandidateWriterLocks(t *testing.T) {
	m, err := NewMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, m.AddSegment(t.Context(), NewSegmentInfo(&datapb.SegmentInfo{ID: 1, CollectionID: 10, State: commonpb.SegmentState_Dropped})))
	candidate := m.GetSegment(t.Context(), 1)
	entered, release := make(chan struct{}), make(chan struct{})
	m.Catalog = &segmentDropCatalog{DataCoordCatalog: m.Catalog, drop: func(context.Context, []*datapb.SegmentInfo) error {
		close(entered)
		<-release
		return nil
	}}
	dropped := make(chan error, 1)
	go func() { _, err := m.DropSegments(t.Context(), []*SegmentInfo{candidate}); dropped <- err }()
	<-entered
	same, other := make(chan struct{}), make(chan struct{})
	go func() { defer m.LockSegments(1)(); close(same) }()
	go func() { defer m.LockSegments(2)(); close(other) }()
	select {
	case <-other:
	case <-time.After(5 * time.Second):
		t.Error("unrelated writer blocked")
	}
	select {
	case <-same:
		t.Error("candidate writer entered before durable deletion")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-dropped)
	<-same
	require.Nil(t, m.GetSegment(t.Context(), 1))
}
