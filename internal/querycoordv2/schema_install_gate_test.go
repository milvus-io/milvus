// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information regarding copyright
// ownership. The ASF licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package querycoordv2

import (
	"context"
	"testing"
	"time"

	"github.com/blang/semver/v4"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/schemaevolution"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func newSchemaInstallTestServer(t *testing.T, collectionID int64) (*Server, *session.MockCluster) {
	nodeMgr := session.NewNodeManager()
	cluster := session.NewMockCluster(t)
	server := &Server{
		meta:                         meta.NewMeta(nil, nil, nodeMgr),
		dist:                         meta.NewDistributionManager(nodeMgr),
		cluster:                      cluster,
		installGate:                  schemaevolution.NewGateManager(),
		schemaInstallVersionProvider: schemaInstallVersionProvider{},
	}
	server.UpdateStateCode(commonpb.StateCode_Healthy)
	require.NoError(t, server.meta.PutCollectionWithoutSave(context.Background(), &meta.Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{CollectionID: collectionID},
	}))
	server.installGate.Close(collectionID)
	return server, cluster
}

type schemaInstallVersionProvider struct{}

func (schemaInstallVersionProvider) GetSessions(_ context.Context, role string) (map[string]*sessionutil.Session, int64, error) {
	return map[string]*sessionutil.Session{
		role: {
			SessionRaw: sessionutil.SessionRaw{ServerID: 1, Version: "3.0.1"},
			Version:    semver.MustParse("3.0.1"),
		},
	}, 1, nil
}

type oldSchemaInstallVersionProvider struct{}

func (oldSchemaInstallVersionProvider) GetSessions(_ context.Context, role string) (map[string]*sessionutil.Session, int64, error) {
	return map[string]*sessionutil.Session{
		role: {
			SessionRaw: sessionutil.SessionRaw{ServerID: 1, Version: "3.0.0"},
			Version:    semver.MustParse("3.0.0"),
		},
	}, 1, nil
}

func TestCompleteSchemaInstallIncludesLeaderViewWorkers(t *testing.T) {
	const collectionID int64 = 100
	server, cluster := newSchemaInstallTestServer(t, collectionID)
	server.dist.SegmentDistManager.Update(1, &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 10, CollectionID: collectionID}})
	server.dist.ChannelDistManager.Update(2, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: "channel-1"},
		View: &meta.LeaderView{
			ID:           2,
			CollectionID: collectionID,
			Channel:      "channel-1",
			Segments: map[int64]*querypb.SegmentDist{
				10: {NodeID: 3},
			},
			GrowingSegments: map[int64]*meta.Segment{
				11: {Node: 4},
			},
		},
	})

	seen := make(map[int64]struct{})
	cluster.EXPECT().UpdateSchema(mock.Anything, mock.Anything, mock.AnythingOfType("*querypb.UpdateSchemaRequest")).
		RunAndReturn(func(_ context.Context, nodeID int64, _ *querypb.UpdateSchemaRequest) (*commonpb.Status, error) {
			seen[nodeID] = struct{}{}
			return merr.Success(), nil
		}).Times(4)

	err := server.CompleteSchemaInstall(context.Background(), collectionID, &schemapb.CollectionSchema{Version: 2}, 200)
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{1, 2, 3, 4}, mapKeys(seen))
	require.False(t, server.installGate.IsClosed(collectionID))
}

func TestCompleteSchemaInstallFailureKeepsGateClosed(t *testing.T) {
	const collectionID int64 = 101
	server, cluster := newSchemaInstallTestServer(t, collectionID)
	server.dist.SegmentDistManager.Update(1, &meta.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 10, CollectionID: collectionID}})
	installErr := merr.WrapErrServiceUnavailableMsg("querynode unavailable")
	cluster.EXPECT().UpdateSchema(mock.Anything, int64(1), mock.AnythingOfType("*querypb.UpdateSchemaRequest")).
		Return(merr.Status(installErr), installErr).Once()

	err := server.CompleteSchemaInstall(context.Background(), collectionID, &schemapb.CollectionSchema{Version: 2}, 200)
	require.Error(t, err)
	require.True(t, server.installGate.IsClosed(collectionID))
}

func TestCompleteSchemaInstallRejectsOldNodeAndKeepsGateClosed(t *testing.T) {
	const collectionID int64 = 105
	server, _ := newSchemaInstallTestServer(t, collectionID)
	server.schemaInstallVersionProvider = oldSchemaInstallVersionProvider{}

	err := server.CompleteSchemaInstall(context.Background(), collectionID, &schemapb.CollectionSchema{Version: 2}, 200)
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
	require.True(t, server.installGate.IsClosed(collectionID))
}

func TestNormalLoadRejectsClosedSchemaInstallGate(t *testing.T) {
	const collectionID int64 = 102

	for name, load := range map[string]func(*Server) (*commonpb.Status, error){
		"collection": func(server *Server) (*commonpb.Status, error) {
			return server.LoadCollection(context.Background(), &querypb.LoadCollectionRequest{
				CollectionID: collectionID,
			})
		},
		"partitions": func(server *Server) (*commonpb.Status, error) {
			return server.LoadPartitions(context.Background(), &querypb.LoadPartitionsRequest{
				CollectionID: collectionID,
				PartitionIDs: []int64{1},
			})
		},
	} {
		t.Run(name, func(t *testing.T) {
			server := &Server{installGate: schemaevolution.NewGateManager()}
			server.UpdateStateCode(commonpb.StateCode_Healthy)
			server.installGate.Close(collectionID)

			status, err := load(server)
			require.NoError(t, err)
			require.ErrorIs(t, merr.Error(status), merr.ErrServiceNotReady)
			require.Zero(t, server.installGate.Active(collectionID))
		})
	}
}

func TestNormalLoadDoesNotHoldTopologyLeaseWhileBroadcasting(t *testing.T) {
	const collectionID int64 = 103

	mockey.PatchConvey("normal load leaves the topology lease free while waiting for broadcast", t, func() {
		server := &Server{installGate: schemaevolution.NewGateManager()}
		server.UpdateStateCode(commonpb.StateCode_Healthy)
		broadcastEntered := make(chan struct{})
		unblockBroadcast := make(chan struct{})
		mockey.Mock((*Server).broadcastAlterLoadConfigCollectionV2ForLoadCollection).
			To(func(_ *Server, _ context.Context, _ *querypb.LoadCollectionRequest) error {
				close(broadcastEntered)
				<-unblockBroadcast
				return nil
			}).Build()

		done := make(chan *commonpb.Status, 1)
		go func() {
			status, err := server.LoadCollection(context.Background(), &querypb.LoadCollectionRequest{
				CollectionID: collectionID,
			})
			require.NoError(t, err)
			done <- status
		}()
		<-broadcastEntered

		require.Zero(t, server.installGate.Active(collectionID))
		prepareCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, server.PrepareSchemaInstall(prepareCtx, collectionID))
		require.True(t, server.installGate.IsClosed(collectionID))

		close(unblockBroadcast)
		require.NoError(t, merr.Error(<-done))
	})
}

func TestRefreshLoadHoldsTopologyLeaseThroughMutation(t *testing.T) {
	const collectionID int64 = 104

	mockey.PatchConvey("refresh load owns a topology lease until refresh completes", t, func() {
		server := &Server{installGate: schemaevolution.NewGateManager()}
		server.UpdateStateCode(commonpb.StateCode_Healthy)
		refreshEntered := make(chan struct{})
		unblockRefresh := make(chan struct{})
		mockey.Mock((*Server).refreshCollection).
			To(func(_ *Server, _ context.Context, _ int64) error {
				close(refreshEntered)
				<-unblockRefresh
				return nil
			}).Build()

		done := make(chan *commonpb.Status, 1)
		go func() {
			status, err := server.LoadCollection(context.Background(), &querypb.LoadCollectionRequest{
				CollectionID: collectionID,
				Refresh:      true,
			})
			require.NoError(t, err)
			done <- status
		}()
		<-refreshEntered

		require.Equal(t, 1, server.installGate.Active(collectionID))
		server.installGate.Close(collectionID)
		waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		require.ErrorIs(t, server.installGate.WaitIdle(waitCtx, collectionID), context.DeadlineExceeded)

		close(unblockRefresh)
		require.NoError(t, merr.Error(<-done))
		require.NoError(t, server.installGate.WaitIdle(context.Background(), collectionID))
	})
}

func mapKeys(values map[int64]struct{}) []int64 {
	keys := make([]int64, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}
