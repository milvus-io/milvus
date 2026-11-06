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
	"github.com/milvus-io/milvus/internal/datacoord/gc"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metacache"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
)

func TestRecycleChannelCPMetaReadsCollectionView(t *testing.T) {
	for _, tc := range []struct {
		name    string
		coll    *meta.CollectionInfo // view entry of 123 and 124, nil if absent
		confirm bool
		dropped bool
	}{
		{"created", &meta.CollectionInfo{}, false, false},
		{"dropping-unconfirmed", &meta.CollectionInfo{State: etcdpb.CollectionState_CollectionDropping}, false, false},
		{"dropping-confirmed", &meta.CollectionInfo{State: etcdpb.CollectionState_CollectionDropping}, true, true},
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
			m := &meta.Meta{Catalog: catalog, MetaStore: store, ChannelSync: meta.NewChannelSync()}
			store.LoadChannelCheckpoints(checkpoints)
			gcx := gc.NewGarbageCollector(m, meta.NewMockHandlerWithMeta(m), gc.GcOption{})
			gcx.RecycleChannelCPMeta(context.Background(), nil)
			if tc.dropped {
				require.Empty(t, m.MetaStore.GetChannelCheckpoints())
			} else {
				require.Len(t, m.MetaStore.GetChannelCheckpoints(), 3)
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
	m := &meta.Meta{Catalog: catalog, MetaStore: store, ChannelSync: meta.NewChannelSync()}
	store.LoadChannelCheckpoints(checkpoints)
	gcx := gc.NewGarbageCollector(m, meta.NewMockHandlerWithMeta(m), gc.GcOption{})

	// The strict catalog mock rejects GcConfirm and DropChannelCheckpoint:
	// collection 123 is missing from the view, so only the early exits
	// keep its checkpoint.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	gcx.RecycleChannelCPMeta(ctx, nil)

	pauseRecords := gc.NewGCPauseRecords()
	_, err := pauseRecords.Insert("test", time.Now().Add(time.Minute))
	require.NoError(t, err)
	gcx.PausedCollection.Insert(123, pauseRecords)
	gcx.RecycleChannelCPMeta(context.Background(), nil)
	require.Len(t, m.MetaStore.GetChannelCheckpoints(), 1)
}
