package walmanager

import (
	"context"
	"maps"
	"slices"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	clientmanager "github.com/milvus-io/milvus/internal/streamingnode/client/manager"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/replica"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ReplicaControl owns the node's inventory across Coord stream replacements.
// One operation at a time can change a PChannel's local WAL or its session route.
type ReplicaControl struct {
	peers      *clientmanager.WALReplicaClient
	authority  func(context.Context, string) (*streamingpb.WALReplicaTopology, error)
	sessions   func(context.Context, string) (map[string]*sessionutil.Session, int64, error)
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.Mutex
	wg         sync.WaitGroup
	closed     bool
	nodeID     int64
	revision   int64
	connection *ReplicaControlStream
	locals     map[string]*localReplica
	opener     wal.Opener
	advertise  func(context.Context, string, *streamingpb.WALAdvertisement) error
}

type replicaTarget struct {
	channel   types.PChannelInfo
	topology  *streamingpb.WALReplicaTopology
	confirmed bool
	control   bool
}

type localReplica struct {
	// opened distinguishes an established replica from a temporary reader owned
	// by a cold writer Open. Protected by operations.
	opened bool
	// Once retirement has started, a newer Sync cannot reopen this ID.
	closing                   bool
	operations                sync.Mutex
	prepared, sealing, sealed int64
	// Retain an unconfirmed outbound Seal across retries of this promotion.
	successorSealTerm, successorSealSource int64
	target                                 replicaTarget
	instance                               wal.WAL
	id                                     int64
	running                                bool
	state                                  streamingpb.LocalWALReplica_State
	cancel                                 context.CancelFunc
}

func newReplicaControl(nodeID int64, opener wal.Opener, advertise func(context.Context, string, *streamingpb.WALAdvertisement) error) *ReplicaControl {
	ctx, cancel := context.WithCancel(context.Background())
	return &ReplicaControl{ctx: ctx, cancel: cancel, nodeID: nodeID, opener: opener, advertise: advertise, locals: make(map[string]*localReplica)}
}

// Stream identity fences late disconnect callbacks and membership confirmations.
type ReplicaControlStream struct {
	owner   *ReplicaControl
	done    chan struct{}
	changed chan struct{}
}

func (c *ReplicaControl) Connect() (*ReplicaControlStream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errWALManagerClosed
	}
	if c.connection != nil {
		c.disconnectLocked()
	}
	s := &ReplicaControlStream{owner: c, done: make(chan struct{}), changed: make(chan struct{}, 1)}
	c.connection = s
	return s, nil
}

func (s *ReplicaControlStream) Done() <-chan struct{} { return s.done }

func (s *ReplicaControlStream) Close() {
	c := s.owner
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.connection == s {
		c.disconnectLocked()
	}
}

func (c *ReplicaControl) disconnectLocked() {
	close(c.connection.done)
	c.connection = nil
	for _, r := range c.locals {
		r.target.confirmed = false
		updatePublisher(r)
	}
}

func updatePublisher(r *localReplica) {
	if w, ok := r.instance.(interface{ ReplicaPublisher() *replica.Publisher }); ok && w.ReplicaPublisher() != nil {
		w.ReplicaPublisher().SetMembers(r.target.topology, r.target.confirmed && r.target.channel.AccessMode == types.AccessModeRW && r.instance.Channel() == r.target.channel)
	}
}

// Sync acknowledges the entire desired revision immediately. Repeated Sync
// retries failed I/O and samples progress without restarting in-flight opens.
func (s *ReplicaControlStream) Sync(req *streamingpb.SyncWALReplicasRequest) (*streamingpb.SyncWALReplicasResponse, error) {
	c := s.owner
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.connection != s {
		return nil, status.NewOnShutdownError("WAL replica control stream replaced")
	}
	var revision int64
	targets := make(map[string]replicaTarget)
	for _, t := range req.Topologies {
		revision = max(revision, t.Revision)
		for _, m := range t.Members {
			if m.NodeId != c.nodeID {
				continue
			}
			channel := types.PChannelInfo{Name: t.Pchannel, WALReplicaID: m.WalReplicaId, AccessMode: types.AccessModeRO}
			if m.WalReplicaId == t.WriterReplicaId {
				channel.AccessMode = types.AccessModeRW
				channel.Term = t.WriterTerm
			}
			targets[t.Pchannel] = replicaTarget{channel: channel, topology: t, confirmed: slices.Contains(req.ConfirmedQueryMemberships, t.Pchannel), control: req.Cchannel.GetPchannel() == t.Pchannel}
		}
	}
	if revision < c.revision {
		return nil, status.NewIgnoreOperation("expired WAL replica topology revision %d, current %d", revision, c.revision)
	}
	c.revision = revision
	for name, target := range targets {
		if c.locals[name] == nil {
			c.locals[name] = &localReplica{id: target.channel.WALReplicaID}
		}
	}
	for name, r := range c.locals {
		next := targets[name]
		// An ID that has closed can only be removed or replaced. Even a new
		// term for this ID must replay its Closed result.
		if r.state == streamingpb.LocalWALReplica_CLOSED && next.channel.WALReplicaID == r.id {
			r.target = next
			continue
		}
		if r.target.channel != next.channel {
			r.state = streamingpb.LocalWALReplica_PREPARING
			if r.running {
				r.cancel()
			}
		} else if r.target.control != next.control && r.state == streamingpb.LocalWALReplica_RW {
			r.state = streamingpb.LocalWALReplica_PREPARING
		}
		r.target = next
		updatePublisher(r)
		if (r.state == streamingpb.LocalWALReplica_RW || r.state == streamingpb.LocalWALReplica_RO) && !replicaAvailable(r.instance) {
			r.state = streamingpb.LocalWALReplica_PREPARING
		}
		if r.state == streamingpb.LocalWALReplica_PREPARING && !r.running {
			c.startLocked(name, r)
		}
	}
	return c.inventoryLocked(), nil
}

func (s *ReplicaControlStream) Changed() <-chan struct{} { return s.changed }

func (s *ReplicaControlStream) Inventory() (*streamingpb.SyncWALReplicasResponse, error) {
	c := s.owner
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.connection != s {
		return nil, status.NewOnShutdownError("WAL replica control stream replaced")
	}
	return c.inventoryLocked(), nil
}

func (c *ReplicaControl) inventoryLocked() *streamingpb.SyncWALReplicasResponse {
	response := &streamingpb.SyncWALReplicasResponse{ObservedRevision: c.revision}
	for _, name := range slices.Sorted(maps.Keys(c.locals)) {
		r := c.locals[name]
		local := &streamingpb.LocalWALReplica{Pchannel: name, WalReplicaId: r.id, State: r.state}
		if r.state == streamingpb.LocalWALReplica_RW {
			local.WriterTerm = proto.Int64(r.instance.Channel().Term)
		}
		response.Replicas = append(response.Replicas, local)
	}
	return response
}

func (c *ReplicaControl) startLocked(name string, r *localReplica) {
	ctx, cancel := context.WithCancel(c.ctx)
	r.running = true
	r.cancel = cancel
	target := r.target
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer cancel()
		r.operations.Lock()
		defer r.operations.Unlock()
		c.mu.Lock()
		instance := r.instance
		c.mu.Unlock()
		w, err := c.reconcile(ctx, name, r, instance, target)
		c.mu.Lock()
		defer c.mu.Unlock()
		r.instance = w
		r.running = false
		if c.closed {
			return
		}
		updatePublisher(r)
		if err != nil {
			mlog.Warn(ctx, "failed to reconcile WAL replica", mlog.String("channel", name), mlog.Int64("replicaID", r.id), mlog.Err(err))
			if target.channel != r.target.channel {
				c.startLocked(name, r)
			}
			return
		}

		switch {
		case w == nil:
			r.state = streamingpb.LocalWALReplica_CLOSED
			if nextID := r.target.channel.WALReplicaID; nextID != r.id {
				delete(c.locals, name)
				if nextID != 0 {
					// The replacement has its own lifetime and handoff state.
					next := &localReplica{id: nextID, target: r.target}
					c.locals[name] = next
					c.startLocked(name, next)
				}
			}
		case target.channel != r.target.channel || (w.Channel().AccessMode == types.AccessModeRW && target.control != r.target.control):
			c.startLocked(name, r)
		case w.Channel().AccessMode == types.AccessModeRW:
			r.state = streamingpb.LocalWALReplica_RW
		default:
			r.state = streamingpb.LocalWALReplica_RO
		}
		if c.connection != nil {
			select {
			case c.connection.changed <- struct{}{}:
			default:
			}
		}
	}()
}

func (c *ReplicaControl) retire(ctx context.Context, name string, r *localReplica, current wal.WAL) (wal.WAL, error) {
	r.closing = true
	if current != nil {
		current.Close()
	}
	return nil, c.advertise(ctx, name, nil)
}

func replicaAvailable(w wal.WAL) bool {
	if !w.IsAvailable() {
		return false
	}
	if writer, ok := w.(interface{ ReadReplica() wal.WAL }); ok {
		return writer.ReadReplica().IsAvailable()
	}
	return true
}

func (c *ReplicaControl) demote(ctx context.Context, name string, current wal.WAL) (wal.WAL, error) {
	writer := current.(interface{ PrepareHandoff(context.Context) error })
	if err := writer.PrepareHandoff(ctx); err != nil {
		return current, err
	}
	return c.sealWriter(ctx, name, current)
}

func (c *ReplicaControl) handleUnavailable(ctx context.Context, name string, r *localReplica, current wal.WAL) (wal.WAL, error) {
	if writer, ok := current.(interface{ ReadReplica() wal.WAL }); ok && r.opened && writer.ReadReplica().IsAvailable() {
		return c.demote(ctx, name, current)
	}
	return c.retire(ctx, name, r, current)
}

func (c *ReplicaControl) reconcile(ctx context.Context, name string, r *localReplica, current wal.WAL, target replicaTarget) (wal.WAL, error) {
	// A newer Sync may have superseded this operation while it waited for a
	// local handoff. Do not begin an obsolete retirement or open.
	if err := ctx.Err(); err != nil {
		return current, err
	}
	want := target.channel
	if r.closing || want.WALReplicaID != r.id {
		return c.retire(ctx, name, r, current)
	}
	if r.sealing != 0 {
		reader, err := c.finishSeal(ctx, name, r, current)
		if err != nil {
			return current, err
		}
		current = reader
	}
	if current != nil && !replicaAvailable(current) {
		return c.handleUnavailable(ctx, name, r, current)
	}
	if current != nil && current.Channel().AccessMode == types.AccessModeRW && current.Channel() != want {
		if want.AccessMode == types.AccessModeRO {
			current.(interface{ ConfirmReplicaTail() }).ConfirmReplicaTail()
			return current, status.NewIgnoreOperation("awaiting seal of WAL %s", name)
		}
		// A successor may have lost its lease before sealing this writer.
		reader, err := c.demote(ctx, name, current)
		if err != nil {
			return current, err
		}
		current = reader
	}

	if want.AccessMode == types.AccessModeRW && (current == nil || current.Channel() != want) {
		source, err := c.writerSource(ctx, want)
		if err != nil {
			return current, err
		}
		if source != 0 && current == nil {
			read := want
			read.Term, read.AccessMode = 0, types.AccessModeRO
			current, err = c.opener.Open(ctx, &wal.OpenOption{Channel: read})
			if err != nil {
				return c.openFailure(ctx, name, r, nil, err)
			}
		}
		if current != nil {
			writer, err := c.prepareSuccessor(ctx, r, current, target, source)
			if err != nil {
				return c.openFailure(ctx, name, r, current, err)
			}
			current = writer
		}
	}
	if current == nil {
		var err error
		current, err = c.opener.Open(ctx, &wal.OpenOption{Channel: want})
		if err != nil {
			return c.openFailure(ctx, name, r, nil, err)
		}
	}
	if !replicaAvailable(current) {
		return c.handleUnavailable(ctx, name, r, current)
	}
	ad := &streamingpb.WALAdvertisement{Pchannel: name, WalReplicaId: r.id}
	if current.Channel().AccessMode == types.AccessModeRW {
		ad.WriterTerm = proto.Int64(current.Channel().Term)
		ad.IsCchannel = target.control
	}
	c.mu.Lock()
	if ctx.Err() != nil {
		c.mu.Unlock()
		return current, ctx.Err()
	}
	r.instance = current
	updatePublisher(r)
	c.mu.Unlock()
	if err := c.advertise(ctx, name, ad); err != nil {
		return current, err
	}
	r.opened = true
	return current, nil
}

// openFailure reports what remains after an abandoned cold Open or promotion.
// Recoverable I/O leaves the attempt Preparing and preserves its recovery reader.
func (c *ReplicaControl) openFailure(ctx context.Context, name string, r *localReplica, reader wal.WAL, err error) (wal.WAL, error) {
	if !errors.IsAny(err, wal.ErrWriterTermConsumed, merr.ErrDataIntegrity) && !status.AsStreamingError(err).IsUnrecoverable() {
		return reader, err
	}
	mlog.Warn(ctx, "WAL replica open attempt ended", mlog.Int64("replicaID", r.id), mlog.Err(err))
	if !r.opened || !replicaAvailable(reader) {
		// The previous role advertisement may have been written despite a
		// lost reply. Finish withdrawing it before reporting Closed.
		return c.retire(ctx, name, r, reader)
	}
	return reader, nil
}

func (c *ReplicaControl) checkWriter(ctx context.Context, want types.PChannelInfo) error {
	source, err := c.writerSource(ctx, want)
	if err != nil {
		return err
	}
	if source != 0 {
		return status.NewIgnoreOperation("waiting for writer on node %d to seal %s", source, want.Name)
	}
	return nil
}

func (c *ReplicaControl) writerSource(ctx context.Context, want types.PChannelInfo) (int64, error) {
	t, err := c.authority(ctx, want.Name)
	if err != nil {
		return 0, err
	}
	if t.WriterReplicaId != want.WALReplicaID || t.WriterTerm != want.Term {
		return 0, status.NewUnmatchedChannelTerm(want.Name, want.Term, t.WriterTerm)
	}
	sessions, _, err := c.sessions(ctx, typeutil.StreamingNodeRole)
	if err != nil {
		return 0, err
	}
	registered := false
	var source int64
	for _, session := range sessions {
		if session.ServerID == c.nodeID {
			registered = !session.Stopping
		}
		// Stopping removes a node from client routing, but its writer can
		// still be draining. Only a sealed route or lease loss permits takeover.
		for _, ad := range session.WALReplicas {
			if ad.Pchannel == want.Name && ad.WriterTerm != nil && (ad.WalReplicaId != want.WALReplicaID || ad.GetWriterTerm() != want.Term) {
				if source != 0 && source != session.ServerID {
					return 0, status.NewIgnoreOperation("multiple writers draining WAL %s", want.Name)
				}
				source = session.ServerID
			}
		}
	}
	if !registered {
		return 0, status.NewOnShutdownError("target writer session is no longer active")
	}
	return source, nil
}

func (c *ReplicaControl) instances() []wal.WAL {
	c.mu.Lock()
	defer c.mu.Unlock()
	instances := make([]wal.WAL, 0, len(c.locals))
	for _, r := range c.locals {
		if r.instance != nil && r.instance.IsAvailable() {
			instances = append(instances, r.instance)
		}
	}
	return instances
}

func (c *ReplicaControl) lookup(name string) wal.WAL {
	c.mu.Lock()
	defer c.mu.Unlock()
	if r := c.locals[name]; r != nil {
		return r.instance
	}
	return nil
}

func (c *ReplicaControl) Close() {
	c.mu.Lock()
	c.closed = true
	c.cancel()
	if c.connection != nil {
		c.disconnectLocked()
	}
	c.mu.Unlock()
	c.wg.Wait()

	for _, r := range c.locals {
		if r.instance != nil {
			r.instance.Close()
		}
	}
}

// Handoff executes on the source. The same ordered pair of calls can be
// retried after a lost reply. Durable authority is checked on every invocation.
func (c *ReplicaControl) Handoff(ctx context.Context, req *streamingpb.WALHandoffRequest, caller int64, seal bool) error {
	if req.GetPchannel() == "" || req.GetTargetTerm() <= 0 || caller <= 0 || caller == c.nodeID {
		return status.NewInvalidArgument("missing or invalid WAL handoff endpoint")
	}
	c.mu.Lock()
	r := c.locals[req.Pchannel]
	if r == nil || c.closed {
		c.mu.Unlock()
		return status.NewChannelNotExist(req.Pchannel)
	}
	c.wg.Add(1)
	c.mu.Unlock()
	defer c.wg.Done()
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.ctx, cancel)
	defer stop()
	defer cancel()
	r.operations.Lock()
	defer r.operations.Unlock()
	t, err := c.authority(ctx, req.Pchannel)
	if err != nil {
		return err
	}
	if req.TargetTerm != t.WriterTerm {
		return status.NewUnmatchedChannelTerm(req.Pchannel, req.TargetTerm, t.WriterTerm)
	}
	if !slices.ContainsFunc(t.Members, func(m *streamingpb.WALReplicaPlacement) bool {
		return m.NodeId == caller && m.WalReplicaId == t.WriterReplicaId
	}) {
		return status.NewInvalidArgument("caller is not the authorized WAL handoff successor")
	}
	c.mu.Lock()
	if c.locals[req.Pchannel] != r {
		c.mu.Unlock()
		return status.NewChannelNotExist(req.Pchannel)
	}
	current := r.instance
	accepted := r.target.topology
	c.mu.Unlock()
	if accepted == nil || accepted.Revision != t.Revision {
		return status.NewIgnoreOperation("waiting for local WAL handoff topology")
	}
	if r.sealed == req.TargetTerm {
		return nil
	}
	if current == nil || current.Channel().AccessMode != types.AccessModeRW || current.Channel().Term >= req.TargetTerm {
		return status.NewChannelNotExist(req.Pchannel)
	}
	if !seal {
		writer := current.(interface{ PrepareHandoff(context.Context) error })
		if err := writer.PrepareHandoff(ctx); err != nil {
			return err
		}
		r.prepared = req.TargetTerm
		return nil
	}
	if r.prepared != req.TargetTerm {
		return status.NewIgnoreOperation("WAL handoff requires a completed prepare")
	}
	// Seal owns the drain once admitted. A lost RPC must not leave a live
	// writer permanently rejecting appends while still advertising its old term.
	r.sealing = req.TargetTerm
	_, err = c.finishSeal(c.ctx, req.Pchannel, r, current)
	return err
}
