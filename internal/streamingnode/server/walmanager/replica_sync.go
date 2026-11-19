package walmanager

import (
	"context"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	clientmanager "github.com/milvus-io/milvus/internal/streamingnode/client/manager"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/replica"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
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
	operations       sync.Mutex
	prepared, sealed int64
	target           replicaTarget
	instance         wal.WAL
	id               int64
	running          bool
	state            streamingpb.LocalWALReplica_State
	cancel           context.CancelFunc
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
		w.ReplicaPublisher().SetMembers(r.target.topology, r.target.confirmed && r.target.channel.AccessMode == types.AccessModeRW)
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
		if r.target.channel != next.channel {
			r.state = streamingpb.LocalWALReplica_PREPARING
			if r.running {
				r.cancel()
			}
		} else if r.target.control != next.control && r.state == streamingpb.LocalWALReplica_READY {
			// Only the advertisement changed; keep any ongoing WAL recovery.
			r.state = streamingpb.LocalWALReplica_PREPARING
		} else if next.channel.AccessMode == types.AccessModeRO && next.topology.GetRevision() != r.target.topology.GetRevision() && r.state == streamingpb.LocalWALReplica_UNRECOVERABLE {
			r.state = streamingpb.LocalWALReplica_PREPARING
		}
		r.target = next
		updatePublisher(r)
		if r.state == streamingpb.LocalWALReplica_READY && !r.instance.IsAvailable() {
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
		response.Replicas = append(response.Replicas, &streamingpb.LocalWALReplica{Pchannel: name, WalReplicaId: r.id, State: r.state})
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
		w, err := c.reconcile(ctx, name, instance, target)
		c.mu.Lock()
		defer c.mu.Unlock()
		r.instance = w
		r.running = false
		if w != nil {
			r.id = w.Channel().WALReplicaID
		}
		if c.closed {
			return
		}
		if target.channel == r.target.channel && err != nil && (errors.Is(err, wal.ErrWriterTermConsumed) ||
			(target.channel.AccessMode == types.AccessModeRO && target.topology.GetRevision() == r.target.topology.GetRevision() && status.AsStreamingError(err).IsUnrecoverable())) {
			r.state = streamingpb.LocalWALReplica_UNRECOVERABLE
		}
		if target.channel != r.target.channel || (target.control != r.target.control && r.state != streamingpb.LocalWALReplica_UNRECOVERABLE) {
			c.startLocked(name, r)
			return
		}
		if c.connection != nil && (err == nil || r.state == streamingpb.LocalWALReplica_UNRECOVERABLE) {
			select {
			case c.connection.changed <- struct{}{}:
			default:
			}
		}
		if err != nil {
			mlog.Warn(ctx, "failed to reconcile WAL replica", mlog.String("channel", name), mlog.Int64("replicaID", r.id), mlog.Err(err))
			return
		}
		if w == nil {
			delete(c.locals, name)
			return
		}
		updatePublisher(r)
		r.state = streamingpb.LocalWALReplica_READY
	}()
}

func (c *ReplicaControl) reconcile(ctx context.Context, name string, current wal.WAL, target replicaTarget) (wal.WAL, error) {
	want := target.channel
	if want.WALReplicaID == 0 || (current != nil && current.Channel().WALReplicaID != want.WALReplicaID) {
		if current != nil {
			current.Close()
			current = nil
		}
		if err := c.advertise(ctx, name, nil); err != nil {
			return nil, err
		}
		if want.WALReplicaID == 0 {
			return nil, nil
		}
	}
	if current != nil && current.Channel().AccessMode == types.AccessModeRW && current.Channel() != want {
		if want.AccessMode == types.AccessModeRO {
			current.(interface{ ConfirmReplicaTail() }).ConfirmReplicaTail()
			return current, status.NewIgnoreOperation("awaiting seal of WAL %s", name)
		}
		// A successor may have lost its lease while handing off from this node.
		// Reuse its local read side before claiming the newer authorization.
		writer := current.(interface {
			PrepareHandoff(context.Context) error
			SealHandoff(context.Context) error
			Demote() wal.WAL
		})
		if err := writer.PrepareHandoff(ctx); err != nil {
			return current, err
		}
		if err := writer.SealHandoff(ctx); err != nil {
			return current, err
		}
		if err := c.advertise(ctx, name, &streamingpb.WALAdvertisement{Pchannel: name, WalReplicaId: want.WALReplicaID}); err != nil {
			return current, err
		}
		current = writer.Demote()
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
				return nil, err
			}
		}
		if current != nil {
			req := &streamingpb.WALHandoffRequest{Pchannel: name, TargetTerm: want.Term}
			reader := current.(interface {
				WaitThrough(context.Context, uint64) error
				Promote(context.Context, types.PChannelInfo, bool, func(context.Context) error) (wal.WAL, error)
			})
			if source != 0 {
				// Old RW forces a physical confirmation on every Sync until seal.
				// On lease loss or missed ticks return to discovery on the next Sync.
				ts, err := resource.Resource().TSOAllocator().Allocate(ctx)
				if err != nil {
					return current, err
				}
				catchup, cancel := context.WithTimeout(ctx, 10*time.Second)
				err = reader.WaitThrough(catchup, ts)
				cancel()
				if err != nil {
					return current, err
				}
				if err = c.peers.Prepare(ctx, source, c.nodeID, req); err != nil {
					return current, err
				}
			}
			activate := func(ctx context.Context) error {
				if source != 0 {
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
			current = writer
		}
	}
	if current == nil {
		var err error
		current, err = c.opener.Open(ctx, &wal.OpenOption{Channel: want})
		if err != nil {
			return nil, err
		}
	}
	if !current.IsAvailable() {
		if err := c.advertise(ctx, name, nil); err != nil {
			return current, err
		}
		current.Close()
		if want.AccessMode == types.AccessModeRW {
			return nil, wal.ErrWriterTermConsumed
		}
		return nil, status.NewChannelNotExist(name)
	}
	ad := &streamingpb.WALAdvertisement{Pchannel: name, WalReplicaId: want.WALReplicaID}
	if want.AccessMode == types.AccessModeRW {
		ad.WriterTerm = proto.Int64(want.Term)
		ad.IsCchannel = target.control
	}
	c.mu.Lock()
	if ctx.Err() != nil {
		c.mu.Unlock()
		return current, ctx.Err()
	}
	r := c.locals[name]
	r.instance = current
	r.id = want.WALReplicaID
	updatePublisher(r)
	c.mu.Unlock()
	return current, c.advertise(ctx, name, ad)
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
	writer := current.(interface {
		PrepareHandoff(context.Context) error
		SealHandoff(context.Context) error
	})
	if !seal {
		if err := writer.PrepareHandoff(ctx); err != nil {
			return err
		}
		r.prepared = req.TargetTerm
		return nil
	}
	if r.prepared != req.TargetTerm {
		return status.NewIgnoreOperation("WAL handoff requires a completed prepare")
	}
	if err := writer.SealHandoff(ctx); err != nil {
		return err
	}
	ad := &streamingpb.WALAdvertisement{Pchannel: req.Pchannel, WalReplicaId: current.Channel().WALReplicaID}
	if err := c.advertise(ctx, req.Pchannel, ad); err != nil {
		return err
	}
	reader := current.(interface{ Demote() wal.WAL }).Demote()
	c.mu.Lock()
	r.instance = reader
	c.mu.Unlock()
	r.sealed = req.TargetTerm
	return nil
}
