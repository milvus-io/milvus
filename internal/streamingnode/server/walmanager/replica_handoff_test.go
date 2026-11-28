package walmanager

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	clientmanager "github.com/milvus-io/milvus/internal/streamingnode/client/manager"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type handoffTestWAL struct {
	replicaTestWAL
	reader *handoffTestWAL
	syncs  int
}

func (w *handoffTestWAL) SyncHandoffTimeTick(context.Context) (uint64, error) {
	w.syncs++
	if w.syncs == 1 {
		return 0, context.DeadlineExceeded
	}
	return 100, nil
}

func (w *handoffTestWAL) PrepareHandoff(context.Context) error { return nil }
func (w *handoffTestWAL) SealHandoff(context.Context) error    { return nil }
func (w *handoffTestWAL) Demote() wal.WAL                      { return w.reader }
func (w *handoffTestWAL) WaitThrough(context.Context, uint64) error {
	panic("unexpected catch-up")
}

func (w *handoffTestWAL) Promote(_ context.Context, _ types.PChannelInfo, _ bool, _ func(context.Context) error) (wal.WAL, error) {
	panic("unexpected promotion")
}

func TestHandoffSourceCompletesSealAfterCallerDisconnects(t *testing.T) {
	mockey.PatchConvey("the admitted Seal drains on the lifetime of the source", t, func() {
		reader := &handoffTestWAL{}
		writer := &handoffTestWAL{reader: reader}
		writer.channel = types.PChannelInfo{Name: "p", WALReplicaID: 10, AccessMode: types.AccessModeRW, Term: 7}
		authority := &streamingpb.WALReplicaTopology{
			Pchannel: "p", Revision: 2, WriterReplicaId: 20, WriterTerm: 8,
			Members: []*streamingpb.WALReplicaPlacement{{NodeId: 1, WalReplicaId: 10}, {NodeId: 2, WalReplicaId: 20}},
		}
		r := &localReplica{instance: writer, target: replicaTarget{topology: authority}}
		caller, disconnect := context.WithCancel(context.Background())
		defer disconnect()
		seals := 0
		mockey.Mock((*handoffTestWAL).SealHandoff).To(func(_ *handoffTestWAL, ctx context.Context) error {
			disconnect()
			seals++
			return ctx.Err()
		}).Build()
		publications := 0
		control := newReplicaControl(1, nil, func(ctx context.Context, name string, advertisement *streamingpb.WALAdvertisement) error {
			publications++
			require.NoError(t, ctx.Err())
			require.Equal(t, "p", name)
			require.EqualValues(t, 10, advertisement.WalReplicaId)
			require.Nil(t, advertisement.WriterTerm)
			return nil
		})
		defer control.Close()
		control.locals["p"] = r
		control.authority = func(context.Context, string) (*streamingpb.WALReplicaTopology, error) { return authority, nil }
		req := &streamingpb.WALHandoffRequest{Pchannel: "p", TargetTerm: 8}
		require.Error(t, control.SealHandoff(caller, req, 2))
		require.Error(t, control.CatchupHandoff(caller, req, 2))
		tick, err := control.PreparingHandoff(caller, req, 2)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Zero(t, tick)
		require.Error(t, control.CatchupHandoff(caller, req, 2))
		for range 2 {
			tick, err = control.PreparingHandoff(caller, req, 2)
			require.NoError(t, err)
			require.EqualValues(t, 100, tick)
		}
		require.Equal(t, 2, writer.syncs)
		require.Error(t, control.SealHandoff(caller, req, 2))
		require.NoError(t, control.CatchupHandoff(caller, req, 2))
		require.NoError(t, control.CatchupHandoff(caller, req, 2))
		require.NoError(t, control.SealHandoff(caller, req, 2))
		require.ErrorIs(t, caller.Err(), context.Canceled)
		require.NoError(t, control.SealHandoff(context.Background(), req, 2))
		require.Same(t, reader, r.instance)
		require.Equal(t, 1, seals)
		require.Equal(t, 1, publications)
	})
}

func TestHandoffSuccessorRetriesSealWithoutNewTimeTick(t *testing.T) {
	mockey.PatchConvey("a lost seal response does not restart tail confirmation", t, func() {
		target := replicaTarget{channel: types.PChannelInfo{Name: "p", WALReplicaID: 20, AccessMode: types.AccessModeRW, Term: 8}}
		control := newReplicaControl(2, nil, nil)
		defer control.Close()
		control.peers = &clientmanager.WALReplicaClient{}
		control.authority = func(context.Context, string) (*streamingpb.WALReplicaTopology, error) {
			return &streamingpb.WALReplicaTopology{WriterReplicaId: 20, WriterTerm: 8}, nil
		}
		control.sessions = func(context.Context, string) (map[string]*sessionutil.Session, int64, error) {
			member := &sessionutil.Session{}
			member.ServerID = 2
			return map[string]*sessionutil.Session{"B": member}, 1, nil
		}
		reader, writer := &handoffTestWAL{}, &handoffTestWAL{}
		var calls []string
		seals := 0
		mockey.Mock((*clientmanager.WALReplicaClient).Prepare).To(func(*clientmanager.WALReplicaClient, context.Context, int64, int64, *streamingpb.WALHandoffRequest) (uint64, error) {
			calls = append(calls, "prepare")
			return 102, nil
		}).Build()
		mockey.Mock((*handoffTestWAL).WaitThrough).To(func(_ *handoffTestWAL, _ context.Context, ts uint64) error {
			calls = append(calls, "observe")
			require.EqualValues(t, 102, ts)
			if len(calls) == 2 {
				return context.DeadlineExceeded
			}
			return nil
		}).Build()
		mockey.Mock((*clientmanager.WALReplicaClient).Catchup).To(func(*clientmanager.WALReplicaClient, context.Context, int64, int64, *streamingpb.WALHandoffRequest) error {
			calls = append(calls, "catchup")
			return nil
		}).Build()
		mockey.Mock((*clientmanager.WALReplicaClient).Seal).To(func(*clientmanager.WALReplicaClient, context.Context, int64, int64, *streamingpb.WALHandoffRequest) error {
			calls = append(calls, "seal")
			seals++
			if seals <= 2 {
				return context.DeadlineExceeded
			}
			return nil
		}).Build()
		mockey.Mock((*handoffTestWAL).Promote).To(func(_ *handoffTestWAL, ctx context.Context, _ types.PChannelInfo, cooperative bool, activate func(context.Context) error) (wal.WAL, error) {
			require.Equal(t, seals == 0, cooperative)
			if err := activate(ctx); err != nil {
				return nil, err
			}
			return writer, nil
		}).Build()
		r := &localReplica{}
		for range 3 {
			w, err := control.prepareSuccessor(context.Background(), r, reader, target, 1)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Same(t, reader, w)
		}
		w, err := control.prepareSuccessor(context.Background(), r, reader, target, 1)
		require.NoError(t, err)
		require.Same(t, writer, w)
		require.Equal(t, []string{"prepare", "observe", "prepare", "observe", "catchup", "seal", "seal", "seal"}, calls)
		require.Equal(t, 3, seals)
	})
}

func TestHandoffRejectsStaleAuthorityAndReplacedSource(t *testing.T) {
	ctx := context.Background()
	control := newReplicaControl(1, nil, nil)
	defer control.Close()
	req := &streamingpb.WALHandoffRequest{Pchannel: "p", TargetTerm: 8}
	require.Error(t, control.CatchupHandoff(ctx, nil, 2))
	require.Error(t, control.CatchupHandoff(ctx, req, 1))
	require.Error(t, control.CatchupHandoff(ctx, req, 2))

	authority := &streamingpb.WALReplicaTopology{
		Revision: 3, WriterReplicaId: 20, WriterTerm: 8,
		Members: []*streamingpb.WALReplicaPlacement{{NodeId: 2, WalReplicaId: 20}},
	}
	r := &localReplica{}
	control.locals["p"] = r
	control.authority = func(context.Context, string) (*streamingpb.WALReplicaTopology, error) {
		return nil, context.DeadlineExceeded
	}
	require.ErrorIs(t, control.CatchupHandoff(ctx, req, 2), context.DeadlineExceeded)
	control.authority = func(context.Context, string) (*streamingpb.WALReplicaTopology, error) { return authority, nil }
	require.Error(t, control.CatchupHandoff(ctx, &streamingpb.WALHandoffRequest{Pchannel: "p", TargetTerm: 7}, 2))
	require.Error(t, control.CatchupHandoff(ctx, req, 3))
	require.Error(t, control.CatchupHandoff(ctx, req, 2))
	r.target.topology = authority
	require.Error(t, control.CatchupHandoff(ctx, req, 2))
	control.authority = func(context.Context, string) (*streamingpb.WALReplicaTopology, error) {
		control.mu.Lock()
		control.locals["p"] = &localReplica{}
		control.mu.Unlock()
		return authority, nil
	}
	require.Error(t, control.SealHandoff(ctx, req, 2))
}

func TestHandoffSourceRetriesRetirementFromSync(t *testing.T) {
	mockey.PatchConvey("the successor can disappear once Seal enters the source", t, func() {
		reader, writer := &handoffTestWAL{}, &handoffTestWAL{}
		reader.channel = types.PChannelInfo{Name: "p", WALReplicaID: 10, AccessMode: types.AccessModeRO}
		writer.channel = types.PChannelInfo{Name: "p", WALReplicaID: 10, AccessMode: types.AccessModeRW, Term: 7}
		writer.reader = reader
		r := &localReplica{id: 10, instance: writer, opened: true, sealing: 8}
		ctx := context.Background()
		sealCalls := 0
		mockey.Mock((*handoffTestWAL).SealHandoff).To(func(*handoffTestWAL, context.Context) error {
			sealCalls++
			if sealCalls == 1 {
				return context.DeadlineExceeded
			}
			return nil
		}).Build()
		publications := 0
		control := newReplicaControl(1, nil, func(context.Context, string, *streamingpb.WALAdvertisement) error {
			publications++
			if publications == 1 {
				return context.DeadlineExceeded
			}
			return nil
		})
		defer control.Close()
		target := replicaTarget{channel: reader.channel}
		for range 2 {
			w, err := control.reconcile(ctx, "p", r, writer, target)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Same(t, writer, w)
			require.EqualValues(t, 8, r.sealing)
		}
		w, err := control.reconcile(ctx, "p", r, writer, target)
		require.NoError(t, err)
		require.Same(t, reader, w)
		require.EqualValues(t, 0, r.sealing)
		require.EqualValues(t, 8, r.sealed)
	})
}
