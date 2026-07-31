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
	"sync"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type testDataViewReferenceCatalog struct {
	mu            sync.Mutex
	dropped       []int64
	marked        []int64
	unmarked      []int64
	markerPresent map[int64]struct{}
}

func (c *testDataViewReferenceCatalog) MarkDataViewCollectionDropped(_ context.Context, collectionID int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.marked = append(c.marked, collectionID)
	c.markerPresent[collectionID] = struct{}{}
	return nil
}

func (c *testDataViewReferenceCatalog) ListDroppedDataViewCollections(context.Context) ([]int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]int64, 0, len(c.markerPresent))
	for collectionID := range c.markerPresent {
		result = append(result, collectionID)
	}
	return result, nil
}

func (c *testDataViewReferenceCatalog) UnmarkDataViewCollectionDropped(_ context.Context, collectionID int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.unmarked = append(c.unmarked, collectionID)
	delete(c.markerPresent, collectionID)
	return nil
}

type testDataViewReferenceDataViews struct {
	dataViewFn       func(context.Context, int64, *viewpb.DataVersion) (*viewpb.DataViewOfCollection, error)
	garbageCollectFn func(context.Context, int64, []*viewpb.DataVersion, int) error
	dropCollectionFn func(context.Context, int64) (*viewpb.DataVersion, error)
}

func (m *testDataViewReferenceDataViews) DataView(ctx context.Context, collectionID int64, version *viewpb.DataVersion) (*viewpb.DataViewOfCollection, error) {
	return m.dataViewFn(ctx, collectionID, version)
}

func (m *testDataViewReferenceDataViews) GarbageCollect(ctx context.Context, collectionID int64, protected []*viewpb.DataVersion, retainLatest int) error {
	return m.garbageCollectFn(ctx, collectionID, protected, retainLatest)
}

func (m *testDataViewReferenceDataViews) OnDropCollection(ctx context.Context, collectionID int64) (*viewpb.DataVersion, error) {
	return m.dropCollectionFn(ctx, collectionID)
}

func newTestDataViewReferenceManager(t *testing.T, catalog *testDataViewReferenceCatalog, dataViews *testDataViewReferenceDataViews, collectionExists func(int64) bool) *dataViewReferenceManager {
	t.Helper()
	manager, err := recoverDataViewReferenceManager(context.Background(), catalog, dataViews, collectionExists)
	require.NoError(t, err)
	return manager
}

func TestDataViewReferenceManagerPinDoesNotDependOnCollectionCache(t *testing.T) {
	version := qviews.DataVersion{StreamingVersion: 1}
	catalog := &testDataViewReferenceCatalog{markerPresent: make(map[int64]struct{})}
	dataViewCalled := false
	dataViews := &testDataViewReferenceDataViews{
		dataViewFn: func(context.Context, int64, *viewpb.DataVersion) (*viewpb.DataViewOfCollection, error) {
			dataViewCalled = true
			return &viewpb.DataViewOfCollection{CollectionId: 100, DataVersion: version.IntoProto()}, nil
		},
		garbageCollectFn: func(context.Context, int64, []*viewpb.DataVersion, int) error { return nil },
		dropCollectionFn: func(context.Context, int64) (*viewpb.DataVersion, error) { return nil, nil },
	}
	manager := newTestDataViewReferenceManager(t, catalog, dataViews, func(int64) bool { return false })

	err := manager.PinDataView(context.Background(), 100, version)

	require.NoError(t, err)
	require.True(t, dataViewCalled)
}

func TestDataViewReferenceManagerPinProtectsGC(t *testing.T) {
	version := qviews.DataVersion{StreamingVersion: 3, CompactVersion: 1}
	catalog := &testDataViewReferenceCatalog{markerPresent: make(map[int64]struct{})}
	pinEntered := make(chan struct{})
	allowPin := make(chan struct{})
	gcEntered := make(chan struct{})
	var protected []*viewpb.DataVersion
	dataViews := &testDataViewReferenceDataViews{
		dataViewFn: func(context.Context, int64, *viewpb.DataVersion) (*viewpb.DataViewOfCollection, error) {
			close(pinEntered)
			<-allowPin
			return &viewpb.DataViewOfCollection{}, nil
		},
		garbageCollectFn: func(_ context.Context, collectionID int64, versions []*viewpb.DataVersion, retainLatest int) error {
			close(gcEntered)
			require.Equal(t, int64(100), collectionID)
			require.Equal(t, 1, retainLatest)
			protected = versions
			return nil
		},
		dropCollectionFn: func(context.Context, int64) (*viewpb.DataVersion, error) { return nil, nil },
	}
	manager := newTestDataViewReferenceManager(t, catalog, dataViews, func(int64) bool { return true })

	pinDone := make(chan error, 1)
	go func() { pinDone <- manager.PinDataView(context.Background(), 100, version) }()
	<-pinEntered
	gcDone := make(chan error, 1)
	go func() { gcDone <- manager.GarbageCollect(context.Background(), 100, 1) }()
	select {
	case <-gcEntered:
		t.Fatal("GC entered the DataView manager before the in-flight pin completed")
	default:
	}
	close(allowPin)
	require.NoError(t, <-pinDone)
	require.NoError(t, <-gcDone)
	require.Equal(t, []*viewpb.DataVersion{version.IntoProto()}, protected)

	manager.UnpinDataView(100, version)
	gcEntered = make(chan struct{})
	require.NoError(t, manager.GarbageCollect(context.Background(), 100, 1))
	require.Empty(t, protected)
}

func TestDataViewReferenceManagerGCCanWinBeforePin(t *testing.T) {
	version := qviews.DataVersion{StreamingVersion: 3, CompactVersion: 1}
	catalog := &testDataViewReferenceCatalog{markerPresent: make(map[int64]struct{})}
	gcEntered := make(chan struct{})
	allowGC := make(chan struct{})
	dataViewEntered := make(chan struct{})
	available := true
	dataViews := &testDataViewReferenceDataViews{
		dataViewFn: func(context.Context, int64, *viewpb.DataVersion) (*viewpb.DataViewOfCollection, error) {
			close(dataViewEntered)
			if !available {
				return nil, nil
			}
			return &viewpb.DataViewOfCollection{}, nil
		},
		garbageCollectFn: func(context.Context, int64, []*viewpb.DataVersion, int) error {
			close(gcEntered)
			<-allowGC
			available = false
			return nil
		},
		dropCollectionFn: func(context.Context, int64) (*viewpb.DataVersion, error) { return nil, nil },
	}
	manager := newTestDataViewReferenceManager(t, catalog, dataViews, func(int64) bool { return true })

	gcDone := make(chan error, 1)
	go func() { gcDone <- manager.GarbageCollect(context.Background(), 100, 1) }()
	<-gcEntered

	pinDone := make(chan error, 1)
	go func() { pinDone <- manager.PinDataView(context.Background(), 100, version) }()
	select {
	case <-dataViewEntered:
		t.Fatal("pin validated the data view before the in-flight GC completed")
	default:
	}

	close(allowGC)
	require.NoError(t, <-gcDone)
	err := <-pinDone
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
	require.True(t, merr.IsRetryableErr(err))
}

func TestDataViewReferenceManagerRecoverMissingDV(t *testing.T) {
	catalog := &testDataViewReferenceCatalog{markerPresent: make(map[int64]struct{})}
	dataViews := &testDataViewReferenceDataViews{
		dataViewFn: func(context.Context, int64, *viewpb.DataVersion) (*viewpb.DataViewOfCollection, error) {
			return nil, nil
		},
		garbageCollectFn: func(context.Context, int64, []*viewpb.DataVersion, int) error { return nil },
		dropCollectionFn: func(context.Context, int64) (*viewpb.DataVersion, error) { return nil, nil },
	}
	manager := newTestDataViewReferenceManager(t, catalog, dataViews, func(int64) bool { return true })

	pinned, err := manager.RecoverDataViewReference(context.Background(), 100, qviews.DataVersion{StreamingVersion: 3})
	require.False(t, pinned)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
}

func TestDataViewReferenceManagerRecoverTerminalCollection(t *testing.T) {
	catalog := &testDataViewReferenceCatalog{
		markerPresent: map[int64]struct{}{100: {}},
	}
	dataViews := &testDataViewReferenceDataViews{
		dataViewFn: func(context.Context, int64, *viewpb.DataVersion) (*viewpb.DataViewOfCollection, error) {
			return nil, errors.New("terminal recovery must not validate a data view")
		},
		garbageCollectFn: func(context.Context, int64, []*viewpb.DataVersion, int) error { return nil },
		dropCollectionFn: func(context.Context, int64) (*viewpb.DataVersion, error) { return nil, nil },
	}
	manager := newTestDataViewReferenceManager(t, catalog, dataViews, func(int64) bool { return true })

	pinned, err := manager.RecoverDataViewReference(context.Background(), 100, qviews.DataVersion{StreamingVersion: 3})
	require.NoError(t, err)
	require.False(t, pinned)
	require.True(t, manager.IsTerminal(100))
}

func TestDataViewReferenceManagerDropIsTerminal(t *testing.T) {
	catalog := &testDataViewReferenceCatalog{markerPresent: make(map[int64]struct{})}
	dataViews := &testDataViewReferenceDataViews{
		dataViewFn: func(context.Context, int64, *viewpb.DataVersion) (*viewpb.DataViewOfCollection, error) {
			return &viewpb.DataViewOfCollection{}, nil
		},
		garbageCollectFn: func(context.Context, int64, []*viewpb.DataVersion, int) error { return nil },
		dropCollectionFn: func(_ context.Context, collectionID int64) (*viewpb.DataVersion, error) {
			catalog.mu.Lock()
			defer catalog.mu.Unlock()
			_, marked := catalog.markerPresent[collectionID]
			require.True(t, marked, "drop marker must be durable before deleting data views")
			catalog.dropped = append(catalog.dropped, collectionID)
			return nil, nil
		},
	}
	manager := newTestDataViewReferenceManager(t, catalog, dataViews, func(int64) bool { return true })

	require.NoError(t, manager.DropCollection(context.Background(), 100))
	require.True(t, manager.IsTerminal(100))
	require.Equal(t, []int64{100}, catalog.marked)
	require.Equal(t, []int64{100}, catalog.dropped)

	err := manager.PinDataView(context.Background(), 100, qviews.DataVersion{StreamingVersion: 3})
	require.ErrorIs(t, err, merr.ErrServiceNotReady)

	require.NoError(t, manager.FinalizeDropCollection(context.Background(), 100))
	require.Equal(t, []int64{100}, catalog.unmarked)
	require.True(t, manager.IsTerminal(100), "finalization must not reopen the collection in this process")
}
