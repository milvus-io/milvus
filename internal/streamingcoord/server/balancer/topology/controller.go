package topology

import (
	"context"
	"maps"
	"slices"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type Catalog interface {
	List(context.Context) (map[string]*streamingpb.WALReplicaTopology, error)
	Save(context.Context, *streamingpb.WALReplicaTopology, *streamingpb.WALReplicaTopology) error
}

// Controller is owned by the serialized balance loop. Desired records and RPC
// responses are immutable after publication. Each pass gets a new session view.
type Controller struct {
	catalog    Catalog
	allocate   func(context.Context) (uint64, error)
	desired    map[string]*streamingpb.WALReplicaTopology
	retryAfter map[string]time.Time
	retryTimer <-chan time.Time
	retryAt    time.Time
}

func Recover(ctx context.Context, catalog Catalog, allocate func(context.Context) (uint64, error)) (*Controller, error) {
	desired, err := catalog.List(ctx)
	if err != nil {
		return nil, err
	}
	return &Controller{catalog: catalog, allocate: allocate, desired: desired, retryAfter: make(map[string]time.Time)}, nil
}

func (c *Controller) Channels() []string {
	return slices.Sorted(maps.Keys(c.desired))
}

// RetryTimer wakes the balance loop while the last broadcast is still converging.
func (c *Controller) RetryTimer() <-chan time.Time {
	return c.retryTimer
}

// Plan persists changes one PChannel at a time. An uncertain write invalidates the cached catalog.
func (c *Controller) Plan(ctx context.Context, writers map[string]int64, eligible, live map[int64]*types.StreamingNodeInfoWithResourceGroup, reports map[int64]*streamingpb.SyncWALReplicasResponse, advertised map[string]types.PChannelInfoAssigned) error {
	if c.desired == nil {
		desired, err := c.catalog.List(ctx)
		if err != nil {
			return err
		}
		c.desired = desired
	}
	load := map[int64]int{}
	for _, t := range c.desired {
		for _, m := range t.Members {
			load[m.NodeId]++
		}
	}
	observed := c.observe(live, reports)
	for _, name := range slices.Sorted(maps.Keys(writers)) {
		current := c.desired[name]
		if current == nil {
			current = &streamingpb.WALReplicaTopology{Pchannel: name}
		}
		writerNode := writers[name]
		// A persisted empty topology is an explicit close. Reopening waits for
		// the complete inventories, including nodes outside the new placement.
		if writerNode != 0 && current.Revision != 0 && len(current.Members) == 0 &&
			(!observed.complete || observed.retiring[name]) {
			continue
		}
		writerResult := observed.members[current.WriterReplicaId].GetState()
		writerEnded := writerResult == streamingpb.LocalWALReplica_CLOSED || writerResult == streamingpb.LocalWALReplica_RO
		if writerNode != 0 && !writerEnded && deferWriterChange(current, writerNode, live, advertised[name]) {
			continue
		}
		retrying := writerNode != 0 && (writerEnded || observed.closed[name])
		if after := c.retryAfter[name]; retrying && time.Now().Before(after) {
			continue
		}
		next, err := Next(ctx, current, writerNode, eligible, live, load, observed.members, c.allocate)
		if err != nil {
			return err
		}
		if next == current {
			continue
		}
		if err := c.catalog.Save(ctx, current, next); err != nil {
			c.desired = nil
			return err
		}
		if retrying {
			// A permanently failing open otherwise drives a tight report/replan loop.
			delay := paramtable.Get().StreamingCfg.WALBalancerBackoffMaxInterval.GetAsDurationByParse()
			c.retryAfter[name] = time.Now().Add(delay)
		} else {
			delete(c.retryAfter, name)
		}
		c.desired[name] = next
		for _, m := range current.Members {
			load[m.NodeId]--
		}
		for _, m := range next.Members {
			load[m.NodeId]++
		}
	}
	return nil
}

func deferWriterChange(current *streamingpb.WALReplicaTopology, nextNode int64, live map[int64]*types.StreamingNodeInfoWithResourceGroup, advertised types.PChannelInfoAssigned) bool {
	for _, member := range current.Members {
		if member.WalReplicaId != current.WriterReplicaId {
			continue
		}
		if member.NodeId == nextNode || live[member.NodeId] == nil {
			return false
		}
		return advertised.Node.ServerID != member.NodeId || advertised.Channel.WALReplicaID != member.WalReplicaId || advertised.Channel.Term != current.WriterTerm
	}
	return false
}

func (c *Controller) revision() int64 {
	var revision int64
	for _, t := range c.desired {
		revision = max(revision, t.Revision)
	}
	return revision
}

// inventory is a snapshot of reports from current registrations for one full Sync revision.
// WALReplicaIDs are globally allocated; match the placement before indexing by ID.
type inventory struct {
	complete bool
	members  map[int64]*streamingpb.LocalWALReplica
	retiring map[string]bool
	closed   map[string]bool
}

func (i inventory) converged(topologies []*streamingpb.WALReplicaTopology) bool {
	if !i.complete || len(i.retiring) != 0 {
		return false
	}
	for _, t := range topologies {
		for _, m := range t.Members {
			r := i.members[m.WalReplicaId]
			if m.WalReplicaId == t.WriterReplicaId {
				if r.GetState() != streamingpb.LocalWALReplica_RW || r.GetWriterTerm() != t.WriterTerm {
					return false
				}
			} else if r.GetState() != streamingpb.LocalWALReplica_RO {
				return false
			}
		}
	}
	return true
}

func (c *Controller) observe(live map[int64]*types.StreamingNodeInfoWithResourceGroup, reports map[int64]*streamingpb.SyncWALReplicasResponse) inventory {
	observed := inventory{complete: true, members: make(map[int64]*streamingpb.LocalWALReplica), retiring: make(map[string]bool), closed: make(map[string]bool)}
	revision := c.revision()
	for id := range live {
		response := reports[id]
		if response == nil || response.ObservedRevision != revision {
			observed.complete = false
			continue
		}
		for _, local := range response.Replicas {
			topology := c.desired[local.Pchannel]
			if slices.ContainsFunc(topology.GetMembers(), func(m *streamingpb.WALReplicaPlacement) bool {
				return m.NodeId == id && m.WalReplicaId == local.WalReplicaId
			}) {
				observed.members[local.WalReplicaId] = local
				if local.State == streamingpb.LocalWALReplica_CLOSED {
					observed.closed[local.Pchannel] = true
				}
			} else {
				observed.retiring[local.Pchannel] = true
			}
		}
	}
	return observed
}

// Request confirms membership once every current registration has accepted this
// full revision and its inventory has no retiring instance of the PChannel.
func (c *Controller) Request(control *streamingpb.CChannelMeta, live map[int64]*types.StreamingNodeInfoWithResourceGroup, reports map[int64]*streamingpb.SyncWALReplicasResponse) *streamingpb.SyncWALReplicasRequest {
	if c.desired == nil {
		return nil
	}
	req := &streamingpb.SyncWALReplicasRequest{Cchannel: control}
	for _, name := range slices.Sorted(maps.Keys(c.desired)) {
		req.Topologies = append(req.Topologies, c.desired[name])
	}
	observed := c.observe(live, reports)
	// A retryable SN operation can finish without changing its Preparing
	// result. Resend the full topology even on an otherwise idle PChannel.
	// Preserve the deadline across other balance events so they cannot delay retries.
	if len(live) != 0 && !observed.converged(req.Topologies) {
		if now := time.Now(); c.retryTimer == nil || !now.Before(c.retryAt) {
			delay := paramtable.Get().StreamingCfg.WALBalancerBackoffMaxInterval.GetAsDurationByParse()
			c.retryAt = now.Add(delay)
			c.retryTimer = time.After(delay)
		}
	} else {
		c.retryTimer = nil
	}
	for _, t := range req.Topologies {
		if len(t.Members) > 0 && observed.complete && !observed.retiring[t.Pchannel] {
			req.ConfirmedQueryMemberships = append(req.ConfirmedQueryMemberships, t.Pchannel)
		}
	}
	return req
}
