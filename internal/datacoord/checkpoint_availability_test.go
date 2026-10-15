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
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type checkpointAvailabilityCoord struct {
	types.MixCoord
	available bool
	checks    int
}

func (c *checkpointAvailabilityCoord) IsCollectionAvailable(int64) bool {
	c.checks++
	return c.available
}

func TestCheckpointAvailabilityFastPath(t *testing.T) {
	catalog := catalogmocks.NewDataCoordCatalog(t)
	coord := &checkpointAvailabilityCoord{available: true}
	checkpoints := map[string]*msgpb.MsgPosition{
		"cluster-rootcoord-dm_0_123v0": nil,
		"cluster-rootcoord-dm_0_124v0": nil,
	}
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(checkpoints, nil).Times(3)
	m := &meta{catalog: catalog, channelCPs: newChannelCps()}
	m.channelCPs.checkpoints = checkpoints
	gc := newGarbageCollector(m, newMockHandlerWithMeta(m), GcOption{broker: broker.NewCoordinatorBroker(coord)})
	gc.recycleChannelCPMeta(context.Background(), nil)
	require.Equal(t, 2, coord.checks)
	require.Len(t, m.channelCPs.checkpoints, 2)
	// The embedded nil RPC interface and strict catalog mock reject any
	// DescribeCollection/GcConfirm/Drop call on a positive cache hit.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	gc.recycleChannelCPMeta(ctx, nil)
	require.Equal(t, 2, coord.checks, "a canceled sweep must stop before checking live IDs")
	pauseRecords := NewGCPauseRecords()
	_, err := pauseRecords.Insert("test", time.Now().Add(time.Minute))
	require.NoError(t, err)
	gc.pausedCollection.Insert(123, pauseRecords)
	gc.recycleChannelCPMeta(context.Background(), nil)
	require.Equal(t, 3, coord.checks, "paused collections must not reach the fast path")
}

type checkpointLookupCoord struct {
	types.MixCoord
	describe func(context.Context, *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error)
}

func (c *checkpointLookupCoord) DescribeCollection(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
	return c.describe(ctx, req)
}

type checkpointCacheMissCoord struct {
	*checkpointLookupCoord
}

func (c *checkpointCacheMissCoord) IsCollectionAvailable(int64) bool { return false }

func TestCheckpointAvailabilityFallback(t *testing.T) {
	for _, withCache := range []bool{false, true} {
		for _, tc := range []struct {
			name    string
			status  *commonpb.Status
			rpcErr  error
			confirm bool
			dropped bool
		}{
			{"available", merr.Success(), nil, false, false},
			{"missing-unconfirmed", merr.Status(merr.WrapErrCollectionNotFound(123)), nil, false, false},
			{"missing-confirmed", merr.Status(merr.WrapErrCollectionNotFound(123)), nil, true, true},
			{"not-ready", merr.Status(merr.ErrServiceNotReady), nil, false, false},
			{"deadline", nil, context.DeadlineExceeded, false, false},
		} {
			name := tc.name + "/no-cache"
			if withCache {
				name = tc.name + "/cache-miss"
			}
			t.Run(name, func(t *testing.T) {
				catalog := catalogmocks.NewDataCoordCatalog(t)
				checkpoints := map[string]*msgpb.MsgPosition{
					"cluster-rootcoord-dm_0_123v0": nil,
					"cluster-rootcoord-dm_1_123v0": nil,
					"cluster-rootcoord-dm_0_124v0": nil,
				}
				catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(checkpoints, nil).Once()
				calls := 0
				var previousOuter context.Context
				coord := &checkpointLookupCoord{describe: func(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
					calls++
					require.Empty(t, req.CollectionName)
					require.Contains(t, []int64{123, 124}, req.CollectionID)
					return &milvuspb.DescribeCollectionResponse{Status: tc.status}, tc.rpcErr
				}}
				var mixCoord types.MixCoord = coord
				if withCache {
					mixCoord = &checkpointCacheMissCoord{coord}
				}
				// Capture the GC's outer timeout as well as exercising the real
				// broker's status/error classification and ID-based request.
				actualBroker := broker.NewCoordinatorBroker(mixCoord)
				wrapped := &checkpointContextBroker{Broker: actualBroker, check: func(ctx context.Context) {
					if previousOuter != nil {
						require.ErrorIs(t, previousOuter.Err(), context.Canceled)
					}
					previousOuter = ctx
				}}
				if tc.rpcErr == nil && merr.Code(merr.Error(tc.status)) == merr.Code(merr.ErrCollectionNotFound) {
					catalog.EXPECT().GcConfirm(mock.Anything, mock.Anything, int64(-1)).Return(tc.confirm).Twice()
				}
				if tc.dropped {
					catalog.EXPECT().DropChannelCheckpoint(mock.Anything, mock.Anything).Return(nil).Times(3)
				}
				m := &meta{catalog: catalog, channelCPs: newChannelCps()}
				m.channelCPs.checkpoints = checkpoints
				gc := newGarbageCollector(m, newMockHandlerWithMeta(m), GcOption{broker: wrapped})
				gc.recycleChannelCPMeta(context.Background(), nil)
				require.Equal(t, 2, calls, "fallback results must be reused across channels of one collection")
				require.ErrorIs(t, previousOuter.Err(), context.Canceled)
				if tc.dropped {
					require.Empty(t, m.channelCPs.checkpoints)
				} else {
					require.Len(t, m.channelCPs.checkpoints, 3)
				}
			})
		}
	}
}

type checkpointContextBroker struct {
	broker.Broker
	check func(context.Context)
}

func (b *checkpointContextBroker) HasCollection(ctx context.Context, id int64) (bool, error) {
	b.check(ctx)
	return b.Broker.HasCollection(ctx, id)
}

func (b *checkpointContextBroker) IsCollectionAvailable(id int64) bool {
	return b.Broker.(broker.CollectionAvailability).IsCollectionAvailable(id)
}
