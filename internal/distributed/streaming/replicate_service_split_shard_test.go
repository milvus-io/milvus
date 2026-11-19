// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package streaming

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming/internal/producer"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/mock_client"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/handler/mock_producer"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/mock_handler"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// splitShardGate is the StreamingCoord and the replicate checkpoint a
// replicate service of the secondary cluster "by-dev" sees in a test.
type splitShardGate struct {
	// wait answers WaitVChannelsAcked. The default acknowledges at once at
	// time tick 5000.
	wait func(ctx context.Context, typ message.MessageType, broadcastID uint64, vchannels []string) (uint64, error)
	// waits records the calls of WaitVChannelsAcked.
	waits [][]string
	// checkpoint is the replicate checkpoint of every pchannel; nil if none.
	checkpoint    *wal.ReplicateCheckpoint
	checkpointErr error
	// appended records what reached the WAL, in order.
	appended []message.MutableMessage
	// reports records the calls of ReportVChannelAppended; reportErr fails them.
	reports   []splitShardReport
	reportErr error
	// appendErr fails every append to the WAL.
	appendErr error
}

type splitShardReport struct {
	broadcastID uint64
	vchannel    string
	timeTick    uint64
}

// newSplitShardReplicateService builds a replicate service of the secondary
// cluster "by-dev" that records what reaches its WAL.
func newSplitShardReplicateService(t *testing.T, gate *splitShardGate) *replicateService {
	c := mock_client.NewMockClient(t)
	as := mock_client.NewMockAssignmentService(t)
	c.EXPECT().Assignment().Return(as).Maybe()
	bs := mock_client.NewMockBroadcastService(t)
	c.EXPECT().Broadcast().Return(bs).Maybe()
	bs.EXPECT().WaitVChannelsAcked(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, typ message.MessageType, broadcastID uint64, vchannels []string) (uint64, error) {
			gate.waits = append(gate.waits, vchannels)
			if gate.wait != nil {
				return gate.wait(ctx, typ, broadcastID, vchannels)
			}
			return 5000, nil
		}).Maybe()
	bs.EXPECT().ReportVChannelAppended(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, broadcastID uint64, vchannel string, timeTick uint64) error {
			gate.reports = append(gate.reports, splitShardReport{broadcastID: broadcastID, vchannel: vchannel, timeTick: timeTick})
			return gate.reportErr
		}).Maybe()

	h := mock_handler.NewMockHandlerClient(t)
	h.EXPECT().GetReplicateCheckpoint(mock.Anything, mock.Anything).RunAndReturn(func(context.Context, string) (*wal.ReplicateCheckpoint, error) {
		return gate.checkpoint, gate.checkpointErr
	}).Maybe()
	p := mock_producer.NewMockProducer(t)
	p.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, mm message.MutableMessage) (*types.AppendResult, error) {
		if gate.appendErr != nil {
			return nil, status.NewUnrecoverableError("%s", gate.appendErr.Error())
		}
		gate.appended = append(gate.appended, mm)
		// The local time tick of the n-th append is 2000+n.
		return &types.AppendResult{MessageID: walimplstest.NewTestMessageID(1), TimeTick: uint64(2000 + len(gate.appended))}, nil
	}).Maybe()
	p.EXPECT().IsAvailable().Return(true).Maybe()
	p.EXPECT().Available().Return(make(chan struct{})).Maybe()
	h.EXPECT().CreateProducer(mock.Anything, mock.Anything).Return(p, nil).Maybe()

	as.EXPECT().GetReplicateConfiguration(mock.Anything).Return(replicateutil.MustNewConfigHelper(
		"by-dev",
		&commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "primary", Pchannels: []string{"primary-rootcoord-dml_0", "primary-rootcoord-dml_1", "primary-rootcoord-dml_2", "primary-rootcoord-dml_3"}},
				{ClusterId: "by-dev", Pchannels: []string{"by-dev-rootcoord-dml_0", "by-dev-rootcoord-dml_1", "by-dev-rootcoord-dml_2", "by-dev-rootcoord-dml_3"}},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "primary", TargetClusterId: "by-dev"},
			},
		},
	), nil).Maybe()

	return &replicateService{
		walAccesserImpl: &walAccesserImpl{
			lifetime:             typeutil.NewLifetime(),
			clusterID:            "by-dev",
			streamingCoordClient: c,
			handlerClient:        h,
			producers:            make(map[string]*producer.ResumableProducer),
		},
	}
}

func hashShard(vchannel string, buckets ...uint64) *schemapb.CollectionShardInfo {
	return &schemapb.CollectionShardInfo{
		VchannelName: vchannel,
		Routing:      &schemapb.CollectionShardInfo_HashRouting{HashRouting: &schemapb.HashRouting{Buckets: buckets}},
	}
}

// newPrimarySplitShardCopies builds the copies of a SplitShard message as the
// primary cluster wrote them into its WAL, without the control channel.
func newPrimarySplitShardCopies(opts ...message.OptBuildBroadcast) []message.ImmutableMessage {
	source := "primary-rootcoord-dml_0_100v0"
	targets := []string{"primary-rootcoord-dml_2_100v2", "primary-rootcoord-dml_3_100v3"}
	opts = append([]message.OptBuildBroadcast{message.OptBuildBroadcastAppendFirst(source)}, opts...)
	broadcast := message.NewSplitShardMessageBuilderV2().
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId:    100,
			SplitTaskId:     7,
			SourceVchannel:  source,
			TargetVchannels: targets,
		}).
		WithBody(&message.SplitShardMessageBody{
			GenesisHeader: &message.CreateCollectionMessageHeader{CollectionId: 100, PartitionIds: []int64{1}},
			Genesis:       &message.CreateCollectionRequest{CollectionID: 100, CollectionSchema: &schemapb.CollectionSchema{Name: "c"}},
			Routing: &message.SplitShardRouting{
				RoutingModulus: 4,
				ShardBy:        "hash(pk)",
				Shards: []*schemapb.CollectionShardInfo{
					hashShard(targets[0], 0),
					hashShard(targets[1], 2),
					hashShard("primary-rootcoord-dml_1_100v1", 1, 3),
				},
			},
		}).
		WithBroadcast(append([]string{source}, targets...), opts...).
		MustBuildBroadcast().WithBroadcastID(55)

	copies := broadcast.SplitIntoMutableMessage()
	// The broadcast header does not keep the order of its vchannels. Give the
	// copies a fixed order, the source first, so that their time ticks are
	// fixed too: the source copy has the lowest, as on the primary.
	sort.Slice(copies, func(i, j int) bool { return copies[i].VChannel() < copies[j].VChannel() })
	immutables := make([]message.ImmutableMessage, 0, len(copies))
	for idx, c := range copies {
		id := walimplstest.NewTestMessageID(int64(100 + idx))
		if idx > 0 {
			// The broadcaster of the primary gave every later copy the time tick
			// of the source as its barrier.
			c = c.WithBarrierTimeTick(1000)
		}
		immutables = append(immutables, c.WithTimeTick(uint64(1000+idx)).WithLastConfirmed(id).IntoImmutableMessage(id))
	}
	return immutables
}

func TestReplicateServiceRenamesTheVChannelsOfASplitShardMessage(t *testing.T) {
	gate := &splitShardGate{}
	rs := newSplitShardReplicateService(t, gate)

	copies := newPrimarySplitShardCopies()
	for _, c := range copies {
		_, err := rs.Append(context.Background(), message.MustNewReplicateMessage("primary", c.IntoImmutableMessageProto()))
		require.NoError(t, err)
	}
	appended := gate.appended
	require.Len(t, appended, len(copies))

	wantSource := "by-dev-rootcoord-dml_0_100v0"
	wantTargets := []string{"by-dev-rootcoord-dml_2_100v2", "by-dev-rootcoord-dml_3_100v3"}
	wantVChannels := append([]string{wantSource}, wantTargets...)
	for idx, mm := range appended {
		// The copy lands on the vchannel of this cluster that corresponds to
		// the one it was written to.
		require.Contains(t, wantVChannels, mm.VChannel())
		require.NotNil(t, mm.ReplicateHeader())
		require.Equal(t, copies[idx].VChannel(), mm.ReplicateHeader().VChannel)

		// The broadcast header names the vchannels of this cluster, the
		// append-first one included.
		bh := mm.BroadcastHeader()
		require.ElementsMatch(t, wantVChannels, bh.VChannels)
		require.Equal(t, []string{wantSource}, bh.AppendFirstVChannels)
		require.Equal(t, uint64(55), bh.BroadcastID)

		// So does the message itself: header and routing.
		split := message.MustAsMutableSplitShardMessageV2(mm)
		header := split.Header()
		require.Equal(t, wantSource, header.GetSourceVchannel())
		require.Equal(t, wantTargets, header.GetTargetVchannels())
		routing := split.MustBody().GetRouting()
		require.Equal(t, uint64(4), routing.GetRoutingModulus())
		require.Equal(t, "hash(pk)", routing.GetShardBy())
		require.Equal(t, "by-dev-rootcoord-dml_2_100v2", routing.GetShards()[0].GetVchannelName())
		require.Equal(t, "by-dev-rootcoord-dml_3_100v3", routing.GetShards()[1].GetVchannelName())
		require.Equal(t, "by-dev-rootcoord-dml_1_100v1", routing.GetShards()[2].GetVchannelName())
		// What is the same fact in both clusters is untouched.
		require.Equal(t, []uint64{1, 3}, routing.GetShards()[2].GetHashRouting().GetBuckets())
		require.Equal(t, int64(7), header.GetSplitTaskId())
		require.Equal(t, int64(100), header.GetCollectionId())
		require.Equal(t, []int64{1}, split.MustBody().GetGenesisHeader().GetPartitionIds())

		// The barrier time tick of the primary does not travel: it means
		// something in the primary only. A target copy carries the barrier of
		// this cluster instead, and the source copy carries none.
		if mm.VChannel() == wantSource {
			require.Zero(t, mm.BarrierTimeTick())
		} else {
			require.Equal(t, uint64(5000), mm.BarrierTimeTick())
		}
		// The message is replicated as it is: it is not marked unreplicable.
		require.False(t, mm.IsUnreplicable())
	}
}

func TestReplicateServiceRefusesAForeignAppendFirstVChannel(t *testing.T) {
	gate := &splitShardGate{}
	rs := newSplitShardReplicateService(t, gate)
	// A header that names an append-first vchannel outside its own vchannels
	// cannot be built by this code base; another cluster could send one.
	foreign := func(bh *messagespb.BroadcastHeader) {
		bh.AppendFirstVchannels = []string{"primary-rootcoord-dml_1_100v9"}
	}
	c := newPrimarySplitShardCopies(foreign)[0]
	_, err := rs.Append(context.Background(), message.MustNewReplicateMessage("primary", c.IntoImmutableMessageProto()))
	require.Error(t, err)
	require.True(t, status.AsStreamingError(err).IsReplicateViolation())
	require.Empty(t, gate.appended)
}

func TestReplicateServiceRefusesASplitShardVChannelOutsideTheTopology(t *testing.T) {
	gate := &splitShardGate{}
	rs := newSplitShardReplicateService(t, gate)

	for name, mutate := range map[string]func(*message.SplitShardMessageHeader, *message.SplitShardMessageBody){
		"source": func(h *message.SplitShardMessageHeader, _ *message.SplitShardMessageBody) {
			h.SourceVchannel = "primary-rootcoord-dml_9_100v0"
		},
		"target": func(h *message.SplitShardMessageHeader, _ *message.SplitShardMessageBody) {
			h.TargetVchannels[1] = "primary-rootcoord-dml_9_100v3"
		},
		"routing": func(_ *message.SplitShardMessageHeader, b *message.SplitShardMessageBody) {
			b.Routing.Shards[2].VchannelName = "primary-rootcoord-dml_9_100v1"
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := newPrimarySplitShardCopies()[0]
			replicated := message.MustNewReplicateMessage("primary", c.IntoImmutableMessageProto())
			split := message.MustAsMutableSplitShardMessageV2(replicated)
			header, body := split.Header(), split.MustBody()
			mutate(header, body)
			split.OverwriteHeader(header)
			split.OverwriteBody(body)
			_, err := rs.Append(context.Background(), replicated)
			require.Error(t, err)
			require.True(t, status.AsStreamingError(err).IsReplicateViolation())
			require.Empty(t, gate.appended)
		})
	}
}

// splitShardCopyOn returns the replicated copy of the primary on one vchannel.
func splitShardCopyOn(t *testing.T, vchannel string) message.ReplicateMutableMessage {
	t.Helper()
	for _, c := range newPrimarySplitShardCopies() {
		if c.VChannel() == vchannel {
			return message.MustNewReplicateMessage("primary", c.IntoImmutableMessageProto())
		}
	}
	t.Fatalf("no copy on %s", vchannel)
	return nil
}

func TestReplicateServiceGatesATargetCopyBehindTheSourceCopy(t *testing.T) {
	const (
		primarySource = "primary-rootcoord-dml_0_100v0"
		primaryTarget = "primary-rootcoord-dml_2_100v2"
		localSource   = "by-dev-rootcoord-dml_0_100v0"
		localTarget   = "by-dev-rootcoord-dml_2_100v2"
	)

	t.Run("the target copy arrives first and waits for the source copy", func(t *testing.T) {
		gate := &splitShardGate{}
		sourceAcked := make(chan uint64)
		waiting := make(chan struct{})
		gate.wait = func(ctx context.Context, typ message.MessageType, broadcastID uint64, vchannels []string) (uint64, error) {
			require.Equal(t, message.MessageTypeSplitShard, typ)
			require.Equal(t, uint64(55), broadcastID)
			// The wait names the source by its name on this cluster.
			require.Equal(t, []string{localSource}, vchannels)
			close(waiting)
			select {
			case tick := <-sourceAcked:
				return tick, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
		rs := newSplitShardReplicateService(t, gate)

		done := make(chan error, 1)
		go func() {
			_, err := rs.Append(context.Background(), splitShardCopyOn(t, primaryTarget))
			done <- err
		}()
		<-waiting
		// The genesis of the target is not in the local WAL while the source
		// copy is not acknowledged here.
		select {
		case <-done:
			t.Fatal("the target copy must not be appended before the source copy is acknowledged")
		case <-time.After(50 * time.Millisecond):
		}
		require.Empty(t, gate.appended)

		// The source copy arrives on its own stream. It never waits.
		waitsBefore := len(gate.waits)
		_, err := rs.Append(context.Background(), splitShardCopyOn(t, primarySource))
		require.NoError(t, err)
		require.Len(t, gate.waits, waitsBefore)
		require.Len(t, gate.appended, 1)
		require.Equal(t, localSource, gate.appended[0].VChannel())
		require.Zero(t, gate.appended[0].BarrierTimeTick())

		// Its local acknowledgement, at local time tick 7000, opens the gate.
		// The target copy is appended with that time tick as its barrier, so
		// the time tick interceptor gives the genesis a time tick above it.
		sourceAcked <- 7000
		require.NoError(t, <-done)
		require.Len(t, gate.appended, 2)
		require.Equal(t, localTarget, gate.appended[1].VChannel())
		require.Equal(t, uint64(7000), gate.appended[1].BarrierTimeTick())
	})

	t.Run("the copy on the control channel and a broadcast without append-first never wait", func(t *testing.T) {
		gate := &splitShardGate{}
		gate.wait = func(context.Context, message.MessageType, uint64, []string) (uint64, error) {
			t.Fatal("no wait is expected")
			return 0, nil
		}
		rs := newSplitShardReplicateService(t, gate)

		control := message.NewSplitShardMessageBuilderV2().
			WithHeader(&message.SplitShardMessageHeader{CollectionId: 100, SplitTaskId: 7, SourceVchannel: primarySource, TargetVchannels: []string{primaryTarget, "primary-rootcoord-dml_3_100v3"}}).
			WithBody(&message.SplitShardMessageBody{Routing: &message.SplitShardRouting{}}).
			WithBroadcast([]string{primarySource, primaryTarget}, message.OptBuildBroadcastAppendFirst(primarySource)).
			MustBuildBroadcast().WithBroadcastID(55)
		withControl := message.WithBroadcastControlChannel(control, "primary-rootcoord-dml_0_vcchan")
		for _, c := range withControl.SplitIntoMutableMessage() {
			if c.VChannel() != "primary-rootcoord-dml_0_vcchan" {
				continue
			}
			id := walimplstest.NewTestMessageID(9)
			immutable := c.WithTimeTick(1001).WithLastConfirmed(id).IntoImmutableMessage(id)
			// The control channel check needs the assignment; the gate decision
			// is what this case is about, so call the gate itself.
			replicated := message.MustNewReplicateMessage("primary", immutable.IntoImmutableMessageProto())
			replicated.OverwriteReplicateVChannel("by-dev-rootcoord-dml_0_vcchan", []string{localSource, localTarget, "by-dev-rootcoord-dml_0_vcchan"})
			out, err := rs.waitAppendFirstCopies(context.Background(), replicated)
			require.NoError(t, err)
			require.Zero(t, out.BarrierTimeTick())
		}

		flush := message.NewManualFlushMessageBuilderV2().
			WithHeader(&message.ManualFlushMessageHeader{}).WithBody(&message.ManualFlushMessageBody{}).
			WithBroadcast([]string{primarySource, primaryTarget}).MustBuildBroadcast().WithBroadcastID(56).SplitIntoMutableMessage()[0]
		out, err := rs.waitAppendFirstCopies(context.Background(), flush)
		require.NoError(t, err)
		require.Zero(t, out.BarrierTimeTick())
		require.Empty(t, gate.waits)
	})

	t.Run("a copy this cluster already appended does not wait", func(t *testing.T) {
		// The stream reconnected and delivers the target copy again. Its
		// replicate time tick is covered by the replicate checkpoint, so the
		// replicate interceptor drops it; its broadcast task may be collected.
		gate := &splitShardGate{checkpoint: &wal.ReplicateCheckpoint{ClusterID: "primary", TimeTick: 1001}}
		gate.wait = func(context.Context, message.MessageType, uint64, []string) (uint64, error) {
			t.Fatal("a covered copy must not wait")
			return 0, nil
		}
		rs := newSplitShardReplicateService(t, gate)
		_, err := rs.Append(context.Background(), splitShardCopyOn(t, primaryTarget))
		require.NoError(t, err)
		require.Len(t, gate.appended, 1)
		require.Zero(t, gate.appended[0].BarrierTimeTick())

		// A checkpoint below the copy, or of another source cluster, does not cover it.
		for _, checkpoint := range []*wal.ReplicateCheckpoint{
			{ClusterID: "primary", TimeTick: 1000},
			{ClusterID: "other", TimeTick: 9999},
		} {
			gate := &splitShardGate{checkpoint: checkpoint}
			rs := newSplitShardReplicateService(t, gate)
			_, err := rs.Append(context.Background(), splitShardCopyOn(t, primaryTarget))
			require.NoError(t, err)
			require.Len(t, gate.waits, 1)
			require.Equal(t, uint64(5000), gate.appended[0].BarrierTimeTick())
		}
	})

	t.Run("a wait that fails appends nothing, and the redelivered copy waits again", func(t *testing.T) {
		// The stream is closed while the target waits (a reconnect, a
		// switchover), or the StreamingCoord restarts: the call fails.
		gate := &splitShardGate{}
		failure := errors.New("streaming coord is restarting")
		gate.wait = func(context.Context, message.MessageType, uint64, []string) (uint64, error) { return 0, failure }
		rs := newSplitShardReplicateService(t, gate)
		_, err := rs.Append(context.Background(), splitShardCopyOn(t, primaryTarget))
		require.ErrorIs(t, err, failure)
		require.Empty(t, gate.appended)

		ctx, cancel := context.WithCancel(context.Background())
		gate.wait = func(ctx context.Context, _ message.MessageType, _ uint64, _ []string) (uint64, error) {
			cancel()
			<-ctx.Done()
			return 0, ctx.Err()
		}
		_, err = rs.Append(ctx, splitShardCopyOn(t, primaryTarget))
		require.ErrorIs(t, err, context.Canceled)
		require.Empty(t, gate.appended)

		// The stream delivers the copy again after it reconnects; it waits
		// again and is appended once the source copy is acknowledged.
		gate.wait = nil
		_, err = rs.Append(context.Background(), splitShardCopyOn(t, primaryTarget))
		require.NoError(t, err)
		require.Len(t, gate.appended, 1)
		require.Equal(t, uint64(5000), gate.appended[0].BarrierTimeTick())
		require.Len(t, gate.waits, 3)

		// The replicate checkpoint cannot be read: nothing is appended either.
		gate.checkpointErr = failure
		_, err = rs.Append(context.Background(), splitShardCopyOn(t, primaryTarget))
		require.ErrorIs(t, err, failure)
		require.Len(t, gate.appended, 1)
	})
}

// The replicate stream of the source reports the local time tick of the source
// copy to the streaming coord as soon as it is appended, so that the target
// copies are released without waiting for the acknowledgement of the copy.
// No other copy is reported, and a failed report does not fail the append.
func TestReplicateServiceReportsTheAppendedSourceCopy(t *testing.T) {
	const localSource = "by-dev-rootcoord-dml_0_100v0"
	gate := &splitShardGate{}
	rs := newSplitShardReplicateService(t, gate)

	for _, c := range newPrimarySplitShardCopies() {
		_, err := rs.Append(context.Background(), message.MustNewReplicateMessage("primary", c.IntoImmutableMessageProto()))
		require.NoError(t, err)
	}
	require.Len(t, gate.appended, 3)
	require.Equal(t, localSource, gate.appended[0].VChannel())
	require.Equal(t, []splitShardReport{{broadcastID: 55, vchannel: localSource, timeTick: 2001}}, gate.reports)

	// A report that fails leaves the append as it was: the targets then wait
	// for the acknowledgement of the copy.
	gate = &splitShardGate{reportErr: errors.New("streaming coord is unavailable")}
	rs = newSplitShardReplicateService(t, gate)
	result, err := rs.Append(context.Background(), splitShardCopyOn(t, "primary-rootcoord-dml_0_100v0"))
	require.NoError(t, err)
	require.Equal(t, uint64(2001), result.TimeTick)
	require.Len(t, gate.reports, 1)

	// An append that fails reports nothing.
	gate = &splitShardGate{appendErr: errors.New("wal is fenced")}
	rs = newSplitShardReplicateService(t, gate)
	_, err = rs.Append(context.Background(), splitShardCopyOn(t, "primary-rootcoord-dml_0_100v0"))
	require.Error(t, err)
	require.Empty(t, gate.reports)
}
