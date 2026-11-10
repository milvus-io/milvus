//go:build test && dynamic

package integration

import (
	"context"
	"path"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// QueryViewSegment is a Coordinator-confirmed assignment, not a native resource
// sample. MinimumManifestVersion is the View requirement, not the loaded revision.
type QueryViewSegment struct {
	NodeID                 int64
	PartitionID            int64
	MinimumManifestVersion int64
	LoadInfoVersion        uint64
}

// QueryViewDistribution retains all persisted states for release checks, and
// projects the highest Up version of each shard/replica for serving checks.
// Up is persisted after QNs report Ready and SN confirms Up. Preparing is only
// intent, and ready_segment_ids are deliberately omitted from persistence.
type QueryViewDistribution struct {
	Views    []*viewpb.QueryViewOfShard
	UpViews  []*viewpb.QueryViewOfShard
	Segments map[int64][]QueryViewSegment
}

// GetQueryViewDistribution reads this collection's Coordinator snapshots in one
// linearizable etcd operation. Callers independently verify worker resources and
// live serving; persisted state alone cannot prove current node availability.
func (s *MiniClusterSuite) GetQueryViewDistribution(ctx context.Context, collectionID int64) QueryViewDistribution {
	s.T().Helper()
	response, err := s.Cluster.EtcdCli.Get(ctx, path.Join(s.Cluster.RootPath(), "meta/coord/qv", strconv.FormatInt(collectionID, 10))+"/", clientv3.WithPrefix())
	require.NoError(s.T(), err)
	views := make([]*viewpb.QueryViewOfShard, 0, len(response.Kvs))
	for _, kv := range response.Kvs {
		view := &viewpb.QueryViewOfShard{}
		require.NoError(s.T(), proto.Unmarshal(kv.Value, view))
		require.Equal(s.T(), collectionID, view.GetMeta().GetCollectionId())
		views = append(views, view)
	}
	return queryViewDistribution(s.T(), views)
}

func queryViewDistribution(t testing.TB, views []*viewpb.QueryViewOfShard) QueryViewDistribution {
	t.Helper()
	result := QueryViewDistribution{Views: views, Segments: make(map[int64][]QueryViewSegment)}
	latest := make(map[qviews.ShardID]*viewpb.QueryViewOfShard)
	for _, view := range views {
		meta := view.GetMeta()
		if meta.GetState() != viewpb.QueryViewState_QueryViewStateUp {
			continue
		}
		id := qviews.ShardID{VChannel: meta.GetVchannel(), ReplicaID: meta.GetReplicaId()}
		previous := latest[id]
		if previous == nil || qviews.FromProtoQueryViewVersion(meta.GetVersion()).GT(qviews.FromProtoQueryViewVersion(previous.GetMeta().GetVersion())) {
			latest[id] = view
		}
	}
	for _, view := range latest {
		result.UpViews = append(result.UpViews, view)
	}
	sort.Slice(result.UpViews, func(i, j int) bool {
		a, b := result.UpViews[i].GetMeta(), result.UpViews[j].GetMeta()
		if a.GetVchannel() != b.GetVchannel() {
			return a.GetVchannel() < b.GetVchannel()
		}
		return a.GetReplicaId() < b.GetReplicaId()
	})
	for _, view := range result.UpViews {
		for _, node := range view.GetQueryNode() {
			require.Positive(t, node.GetNodeId())
			for _, partition := range node.GetPartitions() {
				versions := partition.GetSegmentManifestVersions()
				require.True(t, len(versions) == 0 || len(versions) == len(partition.GetSegmentIds()), "manifest versions must align with segment IDs")
				for i, segmentID := range partition.GetSegmentIds() {
					var version int64
					if len(versions) > 0 {
						version = versions[i]
					}
					result.Segments[segmentID] = append(result.Segments[segmentID], QueryViewSegment{NodeID: node.GetNodeId(), PartitionID: partition.GetPartitionId(), MinimumManifestVersion: version, LoadInfoVersion: view.GetMeta().GetLoadInfoVersion()})
				}
			}
		}
	}
	for _, nodes := range result.Segments {
		sort.Slice(nodes, func(i, j int) bool { return nodes[i].NodeID < nodes[j].NodeID })
	}
	return result
}
