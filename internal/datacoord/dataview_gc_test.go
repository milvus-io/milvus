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
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type fakeDataViewGarbageCollector struct {
	calls []struct {
		collectionID int64
		retainLatest int
	}
}

func (c *fakeDataViewGarbageCollector) GarbageCollect(_ context.Context, collectionID int64, retainLatest int) error {
	c.calls = append(c.calls, struct {
		collectionID int64
		retainLatest int
	}{collectionID: collectionID, retainLatest: retainLatest})
	return nil
}

func TestGarbageCollectorRecycleDataViewsUsesReferenceGuard(t *testing.T) {
	manager := &fakeGCDataViewManager{}
	m := &meta{
		collections:     typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		dataViewManager: manager,
	}
	m.collections.Insert(1, &collectionInfo{ID: 1})
	m.collections.Insert(2, &collectionInfo{ID: 2})
	guardedGC := &fakeDataViewGarbageCollector{}
	gc := newGarbageCollector(m, newMockHandler(), GcOption{dataViewGC: guardedGC})

	gc.recycleDataViews(context.Background(), nil)

	require.Empty(t, manager.calls, "normal GC must not bypass the collection reference guard")
	require.Len(t, guardedGC.calls, 2)
	byCollection := make(map[int64]int, len(guardedGC.calls))
	for _, call := range guardedGC.calls {
		byCollection[call.collectionID] = call.retainLatest
	}
	require.Equal(t, map[int64]int{1: 1, 2: 1}, byCollection)
}

func TestGarbageCollectorKeepsSegmentsReferencedByDataView(t *testing.T) {
	for _, test := range []struct {
		name    string
		manager *fakeGCDataViewManager
	}{
		{name: "referenced", manager: &fakeGCDataViewManager{segmentReferenced: true}},
		{name: "reference check failure", manager: &fakeGCDataViewManager{segmentRefErr: errors.New("reference check failed")}},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			m, err := newMemoryMeta(t)
			require.NoError(t, err)
			m.dataViewManager = test.manager
			segment := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
				ID:            1001,
				CollectionID:  100,
				PartitionID:   10,
				State:         commonpb.SegmentState_Dropped,
				DroppedAt:     uint64(time.Now().Add(-time.Hour).UnixNano()),
				InsertChannel: "ch1",
			}}
			require.NoError(t, m.AddSegment(ctx, segment))
			gc := newGarbageCollector(m, newMockHandler(), GcOption{
				cli:           storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir())),
				enabled:       true,
				dropTolerance: 0,
			})

			gc.recycleDroppedSegments(ctx, nil)

			require.NotNil(t, m.GetSegment(ctx, segment.GetID()))
		})
	}
}
