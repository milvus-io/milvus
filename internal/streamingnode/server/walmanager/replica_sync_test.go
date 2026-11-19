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
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
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
		require.Equal(t, streamingpb.LocalWALReplica_READY, report.Replicas[0].State)
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
		require.Equal(t, streamingpb.LocalWALReplica_UNRECOVERABLE, report.Replicas[0].State)
		require.EqualValues(t, 1, opens.Load())
		topo = proto.CloneOf(topo)
		topo.Revision++
		topo.WriterTerm++
		req = &streamingpb.SyncWALReplicasRequest{Topologies: []*streamingpb.WALReplicaTopology{topo}}
		report, err = s.Sync(req)
		require.NoError(t, err)
		require.Equal(t, streamingpb.LocalWALReplica_PREPARING, report.Replicas[0].State)
		waitForChange()
		report, err = s.Inventory()
		require.NoError(t, err)
		require.Equal(t, streamingpb.LocalWALReplica_READY, report.Replicas[0].State)
		require.EqualValues(t, 30, report.Replicas[0].WalReplicaId)
		require.EqualValues(t, 2, opens.Load())
	})
}
