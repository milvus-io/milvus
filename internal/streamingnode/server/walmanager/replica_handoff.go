package walmanager

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

// finishSeal runs under the replica's operations lock. The source keeps the
// intent until both write admission and the leased writer advertisement are gone.
func (c *ReplicaControl) finishSeal(ctx context.Context, name string, r *localReplica, current wal.WAL) (wal.WAL, error) {
	reader, err := c.sealWriter(ctx, name, current)
	if err != nil {
		return current, err
	}
	c.mu.Lock()
	r.instance = reader
	c.mu.Unlock()
	r.sealed, r.sealing = r.sealing, 0
	return reader, nil
}

func (c *ReplicaControl) sealWriter(ctx context.Context, name string, current wal.WAL) (wal.WAL, error) {
	writer := current.(interface {
		SealHandoff(context.Context) error
		Demote() wal.WAL
	})
	if err := writer.SealHandoff(ctx); err != nil {
		return current, err
	}
	ad := &streamingpb.WALAdvertisement{Pchannel: name, WalReplicaId: current.Channel().WALReplicaID}
	if err := c.advertise(ctx, name, ad); err != nil {
		return current, err
	}
	return writer.Demote(), nil
}

// prepareSuccessor retains only the identity of an unconfirmed Seal. Its
// frozen snapshot and cursor belong to Promote and can be released on error.
func (c *ReplicaControl) prepareSuccessor(ctx context.Context, r *localReplica, current wal.WAL, target replicaTarget, source int64) (wal.WAL, error) {
	want := target.channel
	req := &streamingpb.WALHandoffRequest{Pchannel: want.Name, TargetTerm: want.Term}
	reader := current.(interface {
		WaitThrough(context.Context, uint64) error
		Promote(context.Context, types.PChannelInfo, bool, func(context.Context) error) (wal.WAL, error)
	})
	if source != 0 && r.successorSealTerm == want.Term && r.successorSealSource == source {
		// The source may already reject TimeTicks. Finish the idempotent RPC
		// before opening this term from the now-stable recovery baseline.
		if err := c.peers.Seal(ctx, source, c.nodeID, req); err != nil {
			return current, err
		}
		source = 0
	}
	if source != 0 {
		catchup, cancel := context.WithTimeout(ctx, 10*time.Second)
		ts, err := c.peers.Prepare(catchup, source, c.nodeID, req)
		if err == nil {
			err = reader.WaitThrough(catchup, ts)
		}
		cancel()
		if err != nil {
			return current, err
		}
		mlog.Info(ctx, "successor observed handoff TimeTick", mlog.String("channel", want.Name), mlog.Int64("targetTerm", want.Term), mlog.Uint64("timeTick", ts))
		if err := c.peers.Catchup(ctx, source, c.nodeID, req); err != nil {
			return current, err
		}
	}
	activate := func(ctx context.Context) error {
		if source != 0 {
			r.successorSealSource, r.successorSealTerm = source, want.Term
			if err := c.peers.Seal(ctx, source, c.nodeID, req); err != nil {
				return err
			}
		}
		return c.checkWriter(ctx, want)
	}
	writer, err := reader.Promote(ctx, want, source != 0, activate)
	if err != nil {
		return current, err
	}
	return writer, nil
}

func (c *ReplicaControl) PreparingHandoff(ctx context.Context, req *streamingpb.WALHandoffRequest, caller int64) (uint64, error) {
	var tick uint64
	err := c.withHandoff(ctx, req, caller, func(ctx context.Context, r *localReplica, current wal.WAL) error {
		if r.handoffTickTerm != req.TargetTerm {
			writer := current.(interface {
				SyncHandoffTimeTick(context.Context) (uint64, error)
			})
			persisted, err := writer.SyncHandoffTimeTick(ctx)
			if err != nil {
				return err
			}
			r.handoffTick, r.handoffTickTerm = persisted, req.TargetTerm
			mlog.Info(ctx, "writer persisted handoff TimeTick", mlog.String("channel", req.Pchannel), mlog.Int64("targetTerm", req.TargetTerm), mlog.Uint64("timeTick", persisted))
		}
		tick = r.handoffTick
		return nil
	})
	return tick, err
}

func (c *ReplicaControl) CatchupHandoff(ctx context.Context, req *streamingpb.WALHandoffRequest, caller int64) error {
	return c.withHandoff(ctx, req, caller, func(ctx context.Context, r *localReplica, current wal.WAL) error {
		if r.prepared == req.TargetTerm {
			return nil
		}
		if r.handoffTickTerm != req.TargetTerm {
			return status.NewIgnoreOperation("WAL handoff requires a persisted catch-up TimeTick")
		}
		writer := current.(interface{ PrepareHandoff(context.Context) error })
		if err := writer.PrepareHandoff(ctx); err != nil {
			return err
		}
		r.prepared = req.TargetTerm
		return nil
	})
}

func (c *ReplicaControl) SealHandoff(ctx context.Context, req *streamingpb.WALHandoffRequest, caller int64) error {
	return c.withHandoff(ctx, req, caller, func(_ context.Context, r *localReplica, current wal.WAL) error {
		if r.sealed == req.TargetTerm {
			return nil
		}
		if r.prepared != req.TargetTerm {
			return status.NewIgnoreOperation("WAL handoff requires a completed catch-up")
		}
		// Once admitted the source finishes draining even if the caller disconnects.
		r.sealing = req.TargetTerm
		_, err := c.finishSeal(c.ctx, req.Pchannel, r, current)
		return err
	})
}
