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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/metacache"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
)

func TestRecycleChannelCPMetaReadsCollectionView(t *testing.T) {
	for _, tc := range []struct {
		name    string
		coll    *collectionInfo // view entry of 123 and 124, nil if absent
		confirm bool
		dropped bool
	}{
		{"created", &collectionInfo{}, false, false},
		{"dropping-unconfirmed", &collectionInfo{State: etcdpb.CollectionState_CollectionDropping}, false, false},
		{"dropping-confirmed", &collectionInfo{State: etcdpb.CollectionState_CollectionDropping}, true, true},
		{"missing-unconfirmed", nil, false, false},
		{"missing-confirmed", nil, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := catalogmocks.NewDataCoordCatalog(t)
			checkpoints := map[string]*msgpb.MsgPosition{
				"cluster-rootcoord-dm_0_123v0": nil,
				"cluster-rootcoord-dm_1_123v0": nil,
				"cluster-rootcoord-dm_0_124v0": nil,
			}
			catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(checkpoints, nil).Once()
			if !tc.coll.IsCreated() {
				// Once per collection, not per channel.
				catalog.EXPECT().GcConfirm(mock.Anything, mock.Anything, int64(-1)).Return(tc.confirm).Twice()
			}
			if tc.dropped {
				// Each dropped collection's DataCoord meta goes before its checkpoints.
				catalog.EXPECT().DropSegmentChangeGroups(mock.Anything, mock.Anything).Return(nil).Twice()
				catalog.EXPECT().DropChannelCheckpoint(mock.Anything, mock.Anything).Return(nil).Times(3)
			}
			store := metacache.NewMetaStore(catalog)
			if tc.coll != nil {
				for _, id := range []int64{123, 124} {
					coll := *tc.coll
					coll.ID = id
					store.PutCollection(&coll)
				}
			}
			m := &meta{catalog: catalog, metaStore: store, channelSync: newChannelSync()}
			store.LoadChannelCheckpoints(checkpoints)
			gc := newGarbageCollector(m, newMockHandlerWithMeta(m), GcOption{})
			gc.recycleChannelCPMeta(context.Background(), nil)
			if tc.dropped {
				require.Empty(t, m.metaStore.GetChannelCheckpoints())
			} else {
				require.Len(t, m.metaStore.GetChannelCheckpoints(), 3)
			}
		})
	}
}

func TestRecycleChannelCPMetaSkipsCanceledAndPaused(t *testing.T) {
	catalog := catalogmocks.NewDataCoordCatalog(t)
	checkpoints := map[string]*msgpb.MsgPosition{
		"cluster-rootcoord-dm_0_123v0": nil,
	}
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(checkpoints, nil).Twice()
	store := metacache.NewMetaStore(catalog)
	m := &meta{catalog: catalog, metaStore: store, channelSync: newChannelSync()}
	store.LoadChannelCheckpoints(checkpoints)
	gc := newGarbageCollector(m, newMockHandlerWithMeta(m), GcOption{})

	// The strict catalog mock rejects GcConfirm and DropChannelCheckpoint:
	// collection 123 is missing from the view, so only the early exits
	// keep its checkpoint.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	gc.recycleChannelCPMeta(ctx, nil)

	pauseRecords := NewGCPauseRecords()
	_, err := pauseRecords.Insert("test", time.Now().Add(time.Minute))
	require.NoError(t, err)
	gc.pausedCollection.Insert(123, pauseRecords)
	gc.recycleChannelCPMeta(context.Background(), nil)
	require.Len(t, m.metaStore.GetChannelCheckpoints(), 1)
}
