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

package tasks

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	mock_segcore "github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/searchutil/scheduler"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Exercise the real scheduler and SearchTask.Execute/Done together. Only the
// segment search is held at a controllable boundary. Canceling ownerCtx models
// the loser's RPC context ending after its request succeeded on another replica.
func TestSchedulerMergedSearchOwnerCancellation(t *testing.T) {
	paramtable.Init()
	const collectionID int64 = 1000
	schema := mock_segcore.GenTestCollectionSchema("merged-cancellation", schemapb.DataType_Int64, false)
	manager := segments.NewManager()
	require.NoError(t, manager.Collection.PutOrRef(collectionID, schema, mock_segcore.GenTestIndexMeta(collectionID, schema), &querypb.LoadMetaInfo{
		LoadType: querypb.LoadType_LoadCollection, CollectionID: collectionID,
		PartitionIDs: []int64{1},
	}))
	collection := manager.Collection.Get(collectionID)
	defer manager.Collection.Unref(collectionID, 1)
	var vectorFieldID int64
	for _, field := range schema.Fields {
		if field.DataType == schemapb.DataType_FloatVector {
			vectorFieldID = field.FieldID
			break
		}
	}
	require.NotZero(t, vectorFieldID)
	plan, err := proto.Marshal(&planpb.PlanNode{Node: &planpb.PlanNode_VectorAnns{VectorAnns: &planpb.VectorANNS{
		FieldId: vectorFieldID, PlaceholderTag: "$0",
		QueryInfo: &planpb.QueryInfo{Topk: 10, MetricType: "L2", SearchParams: "{}"},
	}}})
	require.NoError(t, err)
	placeholder, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{
		Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: [][]byte{make([]byte, mock_segcore.DefaultDim*4)},
	}}})
	require.NoError(t, err)
	request := &querypb.SearchRequest{
		Req: &internalpb.SearchRequest{
			CollectionID: collectionID, PartitionIDs: []int64{1}, MvccTimestamp: 100,
			Nq: 1, Topk: 10, DslType: commonpb.DslType_BoolExprV1,
			SerializedExprPlan: plan, PlaceholderGroup: placeholder,
		},
		DmlChannels: []string{"channel1"}, Scope: querypb.DataScope_Historical,
	}

	for _, cancelPeer := range []bool{false, true} {
		name := "live_peer_succeeds"
		if cancelPeer {
			name = "all_canceled_stops_execution"
		}
		t.Run(name, func(t *testing.T) {
			ownerCtx, cancelOwner := context.WithCancel(context.Background())
			defer cancelOwner()
			peerCtx, cancelPeerContext := context.WithCancel(context.Background())
			defer cancelPeerContext()
			entered := make(chan context.Context, 1)
			release := make(chan struct{})
			search := mockey.Mock(segments.SearchHistorical).To(func(ctx context.Context, _ *segments.Manager, _ *segments.SearchRequest, _ int64, _, _ []int64) ([]*segments.SearchResult, []segments.Segment, error) {
				entered <- ctx
				select {
				case <-release:
					return nil, nil, ctx.Err()
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				}
			}).Build()
			defer search.UnPatch()
			sched := scheduler.NewScheduler("fifo")
			sched.Start()
			defer sched.Stop()
			defer close(release)
			owner := NewSearchTask(ownerCtx, collection, manager, proto.Clone(request).(*querypb.SearchRequest), 1)
			peer := NewSearchTask(peerCtx, collection, manager, proto.Clone(request).(*querypb.SearchRequest), 1)
			require.True(t, owner.Merge(peer), "the requests must actually share one execution")
			require.NoError(t, sched.Add(owner))
			var executionCtx context.Context
			select {
			case executionCtx = <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("merged search never reached segment execution")
			}
			cancelOwner()
			if cancelPeer {
				cancelPeerContext()
				select {
				case <-executionCtx.Done():
				case <-time.After(time.Second):
					t.Fatal("all canceled requests must stop execution")
				}
			} else {
				require.NoError(t, executionCtx.Err(), "canceling the owner must not cancel the group's execution")
				// A send releases the search without closing the channel twice.
				select {
				case release <- struct{}{}:
				case <-executionCtx.Done():
					t.Fatal("owner cancellation reached the live peer")
				case <-time.After(time.Second):
					t.Fatal("segment execution did not resume")
				}
			}
			require.ErrorIs(t, waitResult(t, owner), context.Canceled)
			if cancelPeer {
				require.ErrorIs(t, waitResult(t, peer), context.Canceled)
			} else {
				require.NoError(t, waitResult(t, peer))
				require.NoError(t, peerCtx.Err())
				require.NotNil(t, peer.SearchResult())
				require.Equal(t, int64(1), peer.SearchResult().GetNumQueries())
			}
			require.Equal(t, 1, search.Times())
			assertNotNotified(t, owner)
			assertNotNotified(t, peer)
		})
	}
}
