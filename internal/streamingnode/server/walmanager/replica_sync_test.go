package walmanager

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/replica"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type replicaTestOpener struct{ wal.Opener }

func (*replicaTestOpener) Open(context.Context, *wal.OpenOption) (wal.WAL, error) { panic("mockey") }

type replicaTestWAL struct {
	wal.WAL
	channel types.PChannelInfo
	closed  atomic.Bool
}

func (w *replicaTestWAL) Channel() types.PChannelInfo { return w.channel }
func (w *replicaTestWAL) IsAvailable() bool           { return !w.closed.Load() }
func (w *replicaTestWAL) Close()                      { w.closed.Store(true) }

func TestReplicaSync(t *testing.T) {
	mockey.PatchConvey("reconnecting Coord while the writer is opening", t, func() {
		var opens atomic.Int32
		opening := make(chan struct{})
		resume := make(chan struct{})
		mockey.Mock((*replicaTestOpener).Open).To(func(_ *replicaTestOpener, ctx context.Context, opt *wal.OpenOption) (wal.WAL, error) {
			opens.Add(1)
			close(opening)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-resume:
			}
			return &replicaTestWAL{channel: opt.Channel}, nil
		}).Build()
		advertisements := make(chan *streamingpb.WALAdvertisement, 4)
		controller := newReplicaControl(15, &replicaTestOpener{}, func(_ context.Context, _ string, ad *streamingpb.WALAdvertisement) error {
			advertisements <- ad
			return nil
		})
		defer controller.Close()
		topology := &streamingpb.WALReplicaTopology{Pchannel: "p", Revision: 12, WriterReplicaId: 30, WriterTerm: 7, Members: []*streamingpb.WALReplicaPlacement{{NodeId: 15, WalReplicaId: 30}}}
		controller.authority = func(context.Context, string) (*streamingpb.WALReplicaTopology, error) { return topology, nil }
		controller.sessions = func(context.Context, string) (map[string]*sessionutil.Session, int64, error) {
			s := &sessionutil.Session{}
			s.ServerID = 15
			return map[string]*sessionutil.Session{"sn": s}, 1, nil
		}
		req := &streamingpb.SyncWALReplicasRequest{Topologies: []*streamingpb.WALReplicaTopology{topology}, Cchannel: &streamingpb.CChannelMeta{Pchannel: "p"}}
		a, err := controller.Connect()
		require.NoError(t, err)
		report, err := a.Sync(req)
		require.NoError(t, err)
		require.Equal(t, streamingpb.LocalWALReplica_PREPARING, report.Replicas[0].State)
		<-opening
		b, err := controller.Connect()
		require.NoError(t, err)
		<-a.Done()
		a.Close()
		_, err = a.Sync(req)
		require.Error(t, err)
		_, err = b.Sync(req)
		require.NoError(t, err)
		close(resume)
		ad := <-advertisements
		require.EqualValues(t, 7, ad.GetWriterTerm())
		require.True(t, ad.IsCchannel)
		select {
		case <-b.Changed():
		case <-time.After(time.Second):
			t.Fatal("open progress not reported")
		}
		report, err = b.Sync(req)
		require.NoError(t, err)
		require.Equal(t, streamingpb.LocalWALReplica_RW, report.Replicas[0].State)
		require.EqualValues(t, 7, report.Replicas[0].GetWriterTerm())
		require.EqualValues(t, 1, opens.Load())
		active := controller.lookup("p").(*replicaTestWAL)
		_, err = b.Sync(&streamingpb.SyncWALReplicasRequest{})
		require.Error(t, err)
		closed := &streamingpb.SyncWALReplicasRequest{Topologies: []*streamingpb.WALReplicaTopology{{Pchannel: "p", Revision: 13, WriterTerm: 7}}}
		report, err = b.Sync(closed)
		require.NoError(t, err)
		require.Equal(t, streamingpb.LocalWALReplica_PREPARING, report.Replicas[0].State)
		require.Nil(t, <-advertisements)
		<-b.Changed()
		report, err = b.Inventory()
		require.NoError(t, err)
		require.Empty(t, report.Replicas)
		require.True(t, active.closed.Load())
		b.Close()
		_, err = b.Inventory()
		require.Error(t, err)
	})
}

func TestReplicaSyncAbandonedWriter(t *testing.T) {
	mockey.PatchConvey("target failure survives Sync and control reconnect", t, func() {
		var opens atomic.Int32
		mockey.Mock((*replicaTestOpener).Open).To(func(_ *replicaTestOpener, ctx context.Context, opt *wal.OpenOption) (wal.WAL, error) {
			opens.Add(1)
			if opt.Channel.Term == 7 {
				return nil, wal.ErrWriterTermConsumed
			}
			return &replicaTestWAL{channel: opt.Channel}, nil
		}).Build()
		c := newReplicaControl(15, &replicaTestOpener{}, func(context.Context, string, *streamingpb.WALAdvertisement) error { return nil })
		defer c.Close()
		topo := &streamingpb.WALReplicaTopology{Pchannel: "p", Revision: 12, WriterReplicaId: 30, WriterTerm: 7, Members: []*streamingpb.WALReplicaPlacement{{NodeId: 15, WalReplicaId: 30}}}
		c.authority = func(context.Context, string) (*streamingpb.WALReplicaTopology, error) { return topo, nil }
		c.sessions = func(context.Context, string) (map[string]*sessionutil.Session, int64, error) {
			s := &sessionutil.Session{}
			s.ServerID = 15
			return map[string]*sessionutil.Session{"15": s}, 1, nil
		}
		s, err := c.Connect()
		require.NoError(t, err)
		req := &streamingpb.SyncWALReplicasRequest{Topologies: []*streamingpb.WALReplicaTopology{topo}}
		_, err = s.Sync(req)
		require.NoError(t, err)
		waitForChange := func() {
			t.Helper()
			select {
			case <-s.Changed():
			case <-time.After(5 * time.Second):
				t.Fatal("missing replica status change")
			}
		}
		waitForChange()
		s.Close()
		s, err = c.Connect()
		require.NoError(t, err)
		report, err := s.Sync(req)
		require.NoError(t, err)
		require.Equal(t, streamingpb.LocalWALReplica_CLOSED, report.Replicas[0].State)
		require.EqualValues(t, 1, opens.Load())
		topo = proto.CloneOf(topo)
		topo.Revision++
		topo.WriterTerm++
		req = &streamingpb.SyncWALReplicasRequest{Topologies: []*streamingpb.WALReplicaTopology{topo}}
		report, err = s.Sync(req)
		require.NoError(t, err)
		require.Equal(t, streamingpb.LocalWALReplica_CLOSED, report.Replicas[0].State)
		require.Nil(t, report.Replicas[0].WriterTerm)
		require.EqualValues(t, 1, opens.Load())

		topo = proto.CloneOf(topo)
		topo.Revision++
		topo.WriterReplicaId = 31
		topo.Members[0].WalReplicaId = 31
		req = &streamingpb.SyncWALReplicasRequest{Topologies: []*streamingpb.WALReplicaTopology{topo}}
		_, err = s.Sync(req)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			report, err = s.Inventory()
			return err == nil && len(report.Replicas) == 1 && report.Replicas[0].State == streamingpb.LocalWALReplica_RW
		}, 5*time.Second, time.Millisecond)
		require.EqualValues(t, 31, report.Replicas[0].WalReplicaId)
		require.EqualValues(t, 8, report.Replicas[0].GetWriterTerm())
		require.EqualValues(t, 2, opens.Load())
	})
}

func TestReplicaPromotionResults(t *testing.T) {
	mockey.PatchConvey("a failed promotion preserves the reader until the next writer grant", t, func() {
		reader := &handoffTestWAL{}
		reader.channel = types.PChannelInfo{Name: "p", WALReplicaID: 30, AccessMode: types.AccessModeRO}
		mockey.Mock((*replicaTestOpener).Open).Return(reader, nil).Build()

		var promotions atomic.Int32
		mockey.Mock((*handoffTestWAL).Promote).To(func(_ *handoffTestWAL, ctx context.Context, target types.PChannelInfo, _ bool, activate func(context.Context) error) (wal.WAL, error) {
			promotions.Add(1)
			if target.Term == 7 {
				return nil, wal.ErrWriterTermConsumed
			}
			if err := activate(ctx); err != nil {
				return nil, err
			}
			return &replicaTestWAL{channel: target}, nil
		}).Build()

		c := newReplicaControl(15, &replicaTestOpener{}, func(context.Context, string, *streamingpb.WALAdvertisement) error { return nil })
		defer c.Close()
		var topology atomic.Pointer[streamingpb.WALReplicaTopology]
		c.authority = func(context.Context, string) (*streamingpb.WALReplicaTopology, error) { return topology.Load(), nil }
		c.sessions = func(context.Context, string) (map[string]*sessionutil.Session, int64, error) {
			session := &sessionutil.Session{}
			session.ServerID = 15
			return map[string]*sessionutil.Session{"B": session}, 0, nil
		}
		s, err := c.Connect()
		require.NoError(t, err)
		sync := func(revision, writerID, term int64) *streamingpb.LocalWALReplica {
			t.Helper()
			current := &streamingpb.WALReplicaTopology{
				Pchannel: "p", Revision: revision, WriterReplicaId: writerID, WriterTerm: term,
				Members: []*streamingpb.WALReplicaPlacement{{NodeId: 15, WalReplicaId: 30}, {NodeId: 16, WalReplicaId: 31}},
			}
			topology.Store(current)
			report, err := s.Sync(&streamingpb.SyncWALReplicasRequest{Topologies: []*streamingpb.WALReplicaTopology{current}})
			require.NoError(t, err)
			return report.Replicas[0]
		}
		result := func() *streamingpb.LocalWALReplica {
			t.Helper()
			select {
			case <-s.Changed():
			case <-time.After(5 * time.Second):
				t.Fatal("missing replica state")
			}
			report, err := s.Inventory()
			require.NoError(t, err)
			return report.Replicas[0]
		}

		require.Equal(t, streamingpb.LocalWALReplica_PREPARING, sync(1, 31, 6).State)
		require.Equal(t, streamingpb.LocalWALReplica_RO, result().State)
		require.Equal(t, streamingpb.LocalWALReplica_PREPARING, sync(2, 30, 7).State)
		report := result()
		require.Equal(t, streamingpb.LocalWALReplica_RO, report.State)
		require.Nil(t, report.WriterTerm)
		require.Same(t, reader, c.lookup("p"))
		require.False(t, reader.closed.Load())
		require.Equal(t, streamingpb.LocalWALReplica_RO, sync(3, 30, 7).State)
		require.EqualValues(t, 1, promotions.Load())
		require.Equal(t, streamingpb.LocalWALReplica_PREPARING, sync(4, 30, 8).State)
		report = result()
		require.Equal(t, streamingpb.LocalWALReplica_RW, report.State)
		require.EqualValues(t, 8, report.GetWriterTerm())
		require.EqualValues(t, 2, promotions.Load())
	})
}

func TestReplicaWithdrawalBeforeReplacement(t *testing.T) {
	mockey.PatchConvey("a failed withdrawal cannot reopen the same retired ID", t, func() {
		var opens, withdrawals atomic.Int32
		reader := &replicaTestWAL{channel: types.PChannelInfo{Name: "p", AccessMode: types.AccessModeRO, WALReplicaID: 11}}
		mockey.Mock((*replicaTestOpener).Open).To(func(*replicaTestOpener, context.Context, *wal.OpenOption) (wal.WAL, error) {
			opens.Add(1)
			return reader, nil
		}).Build()
		c := newReplicaControl(12, &replicaTestOpener{}, func(_ context.Context, _ string, ad *streamingpb.WALAdvertisement) error {
			if ad == nil && withdrawals.Add(1) == 1 {
				return context.DeadlineExceeded
			}
			return nil
		})
		defer c.Close()
		s, err := c.Connect()
		require.NoError(t, err)
		member := &streamingpb.WALReplicaPlacement{NodeId: 12, WalReplicaId: 11}
		sync := func(revision int64, members ...*streamingpb.WALReplicaPlacement) {
			t.Helper()
			req := &streamingpb.SyncWALReplicasRequest{Topologies: []*streamingpb.WALReplicaTopology{{Revision: revision, Pchannel: "p", Members: members}}}
			_, err := s.Sync(req)
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				c.mu.Lock()
				defer c.mu.Unlock()
				return c.locals["p"] == nil || !c.locals["p"].running
			}, time.Second, time.Millisecond)
		}
		sync(1, member)
		sync(2)
		require.True(t, reader.closed.Load())
		sync(3, member)
		report, err := s.Inventory()
		require.NoError(t, err)
		require.Equal(t, streamingpb.LocalWALReplica_CLOSED, report.Replicas[0].State)
		require.EqualValues(t, 1, opens.Load())
		require.EqualValues(t, 2, withdrawals.Load())
		sync(4)
		report, err = s.Inventory()
		require.NoError(t, err)
		require.Empty(t, report.Replicas)
	})
}

type replicaTestWriter struct {
	handoffTestWAL
	publisher *replica.Publisher
}

func (w *replicaTestWriter) ReadReplica() wal.WAL                 { return w.reader }
func (w *replicaTestWriter) ReplicaPublisher() *replica.Publisher { return w.publisher }

func TestReplicaReconcileHandoffAndRuntimeFailures(t *testing.T) {
	ctx := context.Background()
	ro := types.PChannelInfo{Name: "p", WALReplicaID: 5, AccessMode: types.AccessModeRO}
	rw := types.PChannelInfo{Name: "p", WALReplicaID: 5, Term: 7, AccessMode: types.AccessModeRW}
	for _, tc := range []struct {
		name              string
		writerUnavailable bool
		readerUnavailable bool
		desired           types.PChannelInfo
		sealing           int64
		wantState         streamingpb.LocalWALReplica_State
	}{
		{name: "healthy writer waits for successor seal", desired: ro, wantState: streamingpb.LocalWALReplica_PREPARING},
		{name: "pending seal finishes before RO sync", desired: ro, sealing: 8, wantState: streamingpb.LocalWALReplica_RO},
		{name: "writer failed and retained its reader", writerUnavailable: true, desired: rw, wantState: streamingpb.LocalWALReplica_RO},
		{name: "reader independently failed", readerUnavailable: true, desired: rw, wantState: streamingpb.LocalWALReplica_CLOSED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := &handoffTestWAL{}
			reader.channel = ro
			reader.closed.Store(tc.readerUnavailable)
			writer := &replicaTestWriter{}
			writer.reader, writer.channel = reader, rw
			writer.closed.Store(tc.writerUnavailable)
			r := &localReplica{id: 5, instance: writer, opened: true, sealing: tc.sealing}
			var ads []*streamingpb.WALAdvertisement
			c := newReplicaControl(1, nil, func(_ context.Context, _ string, ad *streamingpb.WALAdvertisement) error {
				ads = append(ads, proto.CloneOf(ad))
				return nil
			})
			defer c.Close()
			w, err := c.reconcile(ctx, "p", r, writer, replicaTarget{channel: tc.desired})
			switch tc.wantState {
			case streamingpb.LocalWALReplica_PREPARING:
				require.Error(t, err)
				require.Same(t, writer, w)
				require.Empty(t, ads)
			case streamingpb.LocalWALReplica_RO:
				require.NoError(t, err)
				require.Same(t, reader, w)
				require.Nil(t, ads[len(ads)-1].WriterTerm)
				require.Equal(t, tc.sealing, r.sealed)
			case streamingpb.LocalWALReplica_CLOSED:
				require.NoError(t, err)
				require.Nil(t, w)
				require.True(t, writer.closed.Load())
				require.True(t, r.closing)
				require.Equal(t, []*streamingpb.WALAdvertisement{nil}, ads)
			}
		})
	}
}

func TestReplicaSupersededRemoval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reader := &replicaTestWAL{}
	r := &localReplica{id: 5, instance: reader, opened: true}
	c := newReplicaControl(1, nil, nil)
	defer c.Close()
	actual, err := c.reconcile(ctx, "p", r, reader, replicaTarget{})
	require.ErrorIs(t, err, context.Canceled)
	require.Same(t, reader, actual)
	require.False(t, r.closing)
	require.True(t, reader.IsAvailable())
}

func TestReplicaColdWriterFailure(t *testing.T) {
	mockey.PatchConvey("an unadvertised handoff reader belongs to its cold Open", t, func() {
		target := types.PChannelInfo{Name: "p", WALReplicaID: 5, AccessMode: types.AccessModeRW, Term: 8}
		var grants []types.PChannelInfo
		reader := &handoffTestWAL{}
		mockey.Mock((*replicaTestOpener).Open).To(func(_ *replicaTestOpener, _ context.Context, opt *wal.OpenOption) (wal.WAL, error) {
			grants = append(grants, opt.Channel)
			reader.channel = opt.Channel
			return reader, nil
		}).Build()
		var withdrawals int
		c := newReplicaControl(2, &replicaTestOpener{}, func(_ context.Context, name string, ad *streamingpb.WALAdvertisement) error {
			require.Equal(t, "p", name)
			require.Nil(t, ad)
			withdrawals++
			return nil
		})
		defer c.Close()
		mockey.Mock((*ReplicaControl).writerSource).Return(int64(1), nil).Build()
		failure := context.DeadlineExceeded
		mockey.Mock((*ReplicaControl).prepareSuccessor).To(func(*ReplicaControl, context.Context, *localReplica, wal.WAL, replicaTarget, int64) (wal.WAL, error) {
			return reader, failure
		}).Build()
		r := &localReplica{id: 5}
		w, err := c.reconcile(context.Background(), "p", r, nil, replicaTarget{channel: target})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Same(t, reader, w)
		require.False(t, reader.closed.Load())
		require.Equal(t, []types.PChannelInfo{{Name: "p", WALReplicaID: 5, AccessMode: types.AccessModeRO}}, grants)
		failure = merr.WrapErrDataIntegrityMsg("corrupt durable segment owner")
		w, err = c.reconcile(context.Background(), "p", r, reader, replicaTarget{channel: target})
		require.NoError(t, err)
		require.Nil(t, w)
		require.True(t, reader.closed.Load())
		require.Len(t, grants, 1)
		require.Equal(t, 1, withdrawals)

		reader.closed.Store(false)
		r = &localReplica{id: 5, opened: true}
		w, err = c.reconcile(context.Background(), "p", r, reader, replicaTarget{channel: target})
		require.NoError(t, err)
		require.Same(t, reader, w)
		require.False(t, reader.closed.Load())
		require.Equal(t, 1, withdrawals)
	})
}

func TestReplicaPublishRequiresMatchingWriterGrant(t *testing.T) {
	mockey.PatchConvey("membership for the next term cannot authorize the old publisher", t, func() {
		writer := &replicaTestWriter{publisher: replica.NewPublisher(nil)}
		defer writer.publisher.Close()
		writer.channel = types.PChannelInfo{Name: "p", WALReplicaID: 31, Term: 7, AccessMode: types.AccessModeRW}
		r := &localReplica{instance: writer, target: replicaTarget{channel: writer.channel, confirmed: true}}
		var confirmations []bool
		mockey.Mock((*replica.Publisher).SetMembers).To(func(_ *replica.Publisher, _ *streamingpb.WALReplicaTopology, confirmed bool) {
			confirmations = append(confirmations, confirmed)
		}).Build()
		updatePublisher(r)
		r.target.channel.Term++
		updatePublisher(r)
		r.target.channel = writer.channel
		r.target.channel.WALReplicaID++
		updatePublisher(r)
		r.target.channel = writer.channel
		r.target.confirmed = false
		updatePublisher(r)
		require.Equal(t, []bool{true, false, false, false}, confirmations)
	})
}

func TestReplicaLocallyReplacesWriterTerm(t *testing.T) {
	mockey.PatchConvey("an unsealed former writer is reelected after its successor exits", t, func() {
		original := types.PChannelInfo{Name: "p", WALReplicaID: 5, AccessMode: types.AccessModeRW, Term: 7}
		want := original
		want.Term = 9
		reader := &handoffTestWAL{}
		reader.channel = types.PChannelInfo{Name: "p", WALReplicaID: 5, AccessMode: types.AccessModeRO}
		previous := &replicaTestWriter{}
		previous.reader, previous.channel = reader, original
		next := &replicaTestWriter{}
		next.reader, next.channel = reader, want
		r := &localReplica{id: 5, opened: true}

		var roles []*streamingpb.WALAdvertisement
		c := newReplicaControl(1, nil, func(_ context.Context, _ string, ad *streamingpb.WALAdvertisement) error {
			roles = append(roles, proto.CloneOf(ad))
			return nil
		})
		defer c.Close()
		mockey.Mock((*ReplicaControl).writerSource).Return(int64(0), nil).Build()
		mockey.Mock((*handoffTestWAL).Promote).To(func(side *handoffTestWAL, ctx context.Context, grant types.PChannelInfo, cooperative bool, activate func(context.Context) error) (wal.WAL, error) {
			require.Same(t, reader, side)
			require.Equal(t, want, grant)
			require.False(t, cooperative)
			require.NoError(t, activate(ctx))
			return next, nil
		}).Build()
		w, err := c.reconcile(context.Background(), "p", r, previous, replicaTarget{channel: want})
		require.NoError(t, err)
		require.Same(t, next, w)
		require.Len(t, roles, 2)
		require.Nil(t, roles[0].WriterTerm)
		require.EqualValues(t, 9, roles[1].GetWriterTerm())

		// A newly initialized writer can fail before advertising. The
		// established reader used for this promotion is still healthy.
		next.closed.Store(true)
		roles = nil
		w, err = c.reconcile(context.Background(), "p", r, reader, replicaTarget{channel: want})
		require.NoError(t, err)
		require.Same(t, reader, w)
		require.Len(t, roles, 1)
		require.Nil(t, roles[0].WriterTerm)
		require.False(t, reader.closed.Load())
	})
}
