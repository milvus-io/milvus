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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	basekv "github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// Exercise real in-memory deletes while injecting bounded backend failures.
type segmentDeleteKV struct {
	basekv.MetaKv
	data          *memkv.MemoryKV
	limit         int
	prefixCalls   int
	recordCalls   int
	failPrefix    int
	failRecord    int
	afterPrefixes func()
	beforeRecords func()
	failure       error
}

func (s *segmentDeleteKV) MaxTxnOps() int { return s.limit }

func (s *segmentDeleteKV) MultiSaveAndRemoveWithPrefix(ctx context.Context, saves map[string]string, prefixes []string, preds ...predicates.Predicate) error {
	s.prefixCalls++
	if s.prefixCalls == s.failPrefix {
		return s.failure
	}
	if len(prefixes) > s.limit {
		panic("prefix transaction exceeds limit")
	}
	if err := s.data.MultiSaveAndRemoveWithPrefix(ctx, saves, prefixes, preds...); err != nil {
		return err
	}
	if s.afterPrefixes != nil {
		s.afterPrefixes()
	}
	return nil
}

func (s *segmentDeleteKV) MultiRemove(ctx context.Context, keys []string) error {
	s.recordCalls++
	if s.beforeRecords != nil {
		s.beforeRecords()
	}
	if s.recordCalls == s.failRecord {
		return s.failure
	}
	if len(keys) > s.limit {
		panic("record transaction exceeds limit")
	}
	return s.data.MultiRemove(ctx, keys)
}

func newSegmentDeleteFixture(t *testing.T, limit int) (*Catalog, *segmentDeleteKV, []*datapb.SegmentInfo, []string) {
	t.Helper()
	ctx := context.Background()
	store := &segmentDeleteKV{data: memkv.NewMemoryKV(), limit: limit, failure: errors.New("injected delete failure")}
	segments := []*datapb.SegmentInfo{
		{CollectionID: 1, PartitionID: 2, ID: 12},
		{CollectionID: 1, PartitionID: 2, ID: 14},
		{CollectionID: 1, PartitionID: 2, ID: 16},
	}
	all := append(append([]*datapb.SegmentInfo{}, segments...), &datapb.SegmentInfo{CollectionID: 1, PartitionID: 2, ID: 123})
	var logs []string
	for _, seg := range all {
		require.NoError(t, store.data.Save(ctx, buildSegmentPath(1, 2, seg.ID), "segment"))
		for _, namespace := range []string{SegmentBinlogPathPrefix, SegmentDeltalogPathPrefix, SegmentStatslogPathPrefix, SegmentBM25logPathPrefix} {
			key := fmt.Sprintf("%s/1/2/%d/100", namespace, seg.ID)
			require.NoError(t, store.data.Save(ctx, key, "log"))
			if seg.ID != 123 {
				logs = append(logs, key)
			}
		}
	}
	return &Catalog{MetaKv: store}, store, segments, logs
}

func assertSegmentRecordExists(t *testing.T, store *segmentDeleteKV, id int64, expected bool) {
	t.Helper()
	exists, err := store.data.Has(context.Background(), buildSegmentPath(1, 2, id))
	require.NoError(t, err)
	require.Equal(t, expected, exists, "segment %d", id)
}

func TestCatalogDropSegmentsTwoPhases(t *testing.T) {
	catalog, store, segments, logs := newSegmentDeleteFixture(t, 2)
	store.afterPrefixes = func() {
		for _, seg := range segments {
			assertSegmentRecordExists(t, store, seg.ID, true)
		}
	}
	store.beforeRecords = func() {
		for _, key := range logs {
			exists, err := store.data.Has(context.Background(), key)
			require.NoError(t, err)
			require.False(t, exists, "all binlog namespaces must be removed before any segment record")
		}
	}
	require.NoError(t, catalog.DropSegments(context.Background(), append(segments, nil, segments[0])))
	require.Equal(t, 6, store.prefixCalls)
	require.Equal(t, 2, store.recordCalls)
	for _, seg := range segments {
		assertSegmentRecordExists(t, store, seg.ID, false)
	}
	assertSegmentRecordExists(t, store, 123, true)
	for _, namespace := range []string{SegmentBinlogPathPrefix, SegmentDeltalogPathPrefix, SegmentStatslogPathPrefix, SegmentBM25logPathPrefix} {
		exists, err := store.data.Has(context.Background(), namespace+"/1/2/123/100")
		require.NoError(t, err)
		require.True(t, exists, "slash-terminated prefix must preserve sibling segment logs")
	}
}

func TestCatalogDropSegmentsFailureAndRetry(t *testing.T) {
	for _, phase := range []string{"prefix", "record", "cancel before records"} {
		t.Run(phase, func(t *testing.T) {
			catalog, store, segments, _ := newSegmentDeleteFixture(t, 2)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch phase {
			case "prefix":
				store.failPrefix = 2
			case "record":
				store.failRecord = 2
			default:
				store.afterPrefixes = func() {
					if store.prefixCalls == 6 {
						cancel()
					}
				}
			}
			err := catalog.DropSegments(ctx, segments)
			if phase == "cancel before records" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorIs(t, err, store.failure)
			}
			if phase == "record" {
				assertSegmentRecordExists(t, store, 12, false)
				assertSegmentRecordExists(t, store, 14, false)
				assertSegmentRecordExists(t, store, 16, true)
			} else {
				require.Zero(t, store.recordCalls)
				for _, seg := range segments {
					assertSegmentRecordExists(t, store, seg.ID, true)
				}
			}
			store.afterPrefixes = nil
			store.failPrefix, store.failRecord = 0, 0
			require.NoError(t, catalog.DropSegments(context.Background(), segments))
			for _, seg := range segments {
				assertSegmentRecordExists(t, store, seg.ID, false)
			}
			assertSegmentRecordExists(t, store, 123, true)
		})
	}
}

func TestCatalogDropSegmentsNoWritesForInvalidLimitOrCanceledContext(t *testing.T) {
	for _, limit := range []int{0, -1, 2} {
		catalog, store, segments, _ := newSegmentDeleteFixture(t, limit)
		ctx, cancel := context.WithCancel(context.Background())
		if limit > 0 {
			cancel()
		}
		require.Error(t, catalog.DropSegments(ctx, segments))
		cancel()
		require.Zero(t, store.prefixCalls)
		require.Zero(t, store.recordCalls)
		for _, seg := range segments {
			assertSegmentRecordExists(t, store, seg.ID, true)
		}
	}
	catalog, store, _, _ := newSegmentDeleteFixture(t, 2)
	require.NoError(t, catalog.DropSegments(context.Background(), nil))
	require.NoError(t, catalog.DropSegments(context.Background(), []*datapb.SegmentInfo{nil}))
	require.Zero(t, store.prefixCalls)
	require.Zero(t, store.recordCalls)
}
