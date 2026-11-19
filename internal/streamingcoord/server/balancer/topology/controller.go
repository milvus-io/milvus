package topology

import (
	"context"
	"maps"
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type Catalog interface {
	List(context.Context) (map[string]*streamingpb.WALReplicaTopology, error)
	Save(context.Context, *streamingpb.WALReplicaTopology, *streamingpb.WALReplicaTopology) error
}

// Controller is owned by the serialized balance loop. Desired records and RPC
// responses are immutable after publication. Each pass gets a new session view.
type Controller struct {
	catalog  Catalog
	allocate func(context.Context) (uint64, error)
	desired  map[string]*streamingpb.WALReplicaTopology
}

func Recover(ctx context.Context, catalog Catalog, allocate func(context.Context) (uint64, error)) (*Controller, error) {
	desired, err := catalog.List(ctx)
	if err != nil {
		return nil, err
	}
	return &Controller{catalog: catalog, allocate: allocate, desired: desired}, nil
}

// Plan persists changes one PChannel at a time. A write with an unknown result
// invalidates the cached catalog, including records from earlier successes.
func (c *Controller) Channels() []string {
	return slices.Sorted(maps.Keys(c.desired))
}

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
	baseRevision := c.revision()
	for _, name := range slices.Sorted(maps.Keys(writers)) {
		current := c.desired[name]
		if current == nil {
			current = &streamingpb.WALReplicaTopology{Pchannel: name}
		}
		ready := map[int64]bool{}
		retryWriter := false
		for _, m := range current.Members {
			report := reports[m.NodeId]
			if report == nil || report.ObservedRevision != baseRevision {
				continue
			}
			for _, r := range report.Replicas {
				if r.Pchannel == name && r.WalReplicaId == m.WalReplicaId {
					ready[m.WalReplicaId] = r.State == streamingpb.LocalWALReplica_READY
					if r.WalReplicaId == current.WriterReplicaId && r.State == streamingpb.LocalWALReplica_UNRECOVERABLE && live[m.NodeId] != nil {
						retryWriter = true
					}
					break
				}
			}
		}
		var next *streamingpb.WALReplicaTopology
		var err error
		if retryWriter && writers[name] != 0 {
			// Keep placement and replica identity. This report consumes exactly
			// one revision, including when several Sync replies were in flight.
			next = proto.CloneOf(current)
			next.WriterTerm++
			var revision uint64
			revision, err = c.allocate(ctx)
			next.Revision = int64(revision)
		} else {
			// A still-recovering writer may already own the checkpoint while no
			// other replica can discover it for Prepare. Reissue its current
			// topology until its first advertisement or the loss of its session.
			if writers[name] != 0 && deferWriterChange(current, writers[name], live, advertised[name]) {
				continue
			}
			next, err = Next(ctx, current, writers[name], eligible, live, load, ready, c.allocate)
		}
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

// Request includes every topology. A removal can be confirmed once each live
// node has acknowledged this revision and its inventory contains no retiring
// instance of the PChannel. New members can prepare Views independently.
func (c *Controller) Request(control *streamingpb.CChannelMeta, live map[int64]*types.StreamingNodeInfoWithResourceGroup, reports map[int64]*streamingpb.SyncWALReplicasResponse) *streamingpb.SyncWALReplicasRequest {
	if c.desired == nil {
		return nil
	}
	req := &streamingpb.SyncWALReplicasRequest{Cchannel: control}
	for _, name := range slices.Sorted(maps.Keys(c.desired)) {
		req.Topologies = append(req.Topologies, c.desired[name])
	}
	revision := c.revision()
	retiring := map[string]bool{}
	for id := range live {
		response := reports[id]
		if response == nil || response.ObservedRevision != revision {
			return req
		}
		for _, local := range response.Replicas {
			topology := c.desired[local.Pchannel]
			if !slices.ContainsFunc(topology.GetMembers(), func(m *streamingpb.WALReplicaPlacement) bool {
				return m.NodeId == id && m.WalReplicaId == local.WalReplicaId
			}) {
				retiring[local.Pchannel] = true
			}
		}
	}
	for _, t := range req.Topologies {
		if len(t.Members) > 0 && !retiring[t.Pchannel] {
			req.ConfirmedQueryMemberships = append(req.ConfirmedQueryMemberships, t.Pchannel)
		}
	}
	return req
}
