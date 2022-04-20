package querynode

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"sync"

	grpcquerynodeclient "github.com/milvus-io/milvus/internal/distributed/querynode/client"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	ReplicaMetaPrefix = "queryCoord-ReplicaMeta"
)

// shardQueryNodeWrapper wraps a querynode to shardQueryNode and preventing it been closed
type shardQueryNodeWrapper struct {
	*QueryNode
}

// Stop overrides default close method
func (w *shardQueryNodeWrapper) Stop() error { return nil }

// ShardClusterService maintains the online ShardCluster(leader) in this querynode.
type ShardClusterService struct {
	client  *clientv3.Client // etcd client for detectors
	session *sessionutil.Session
	node    *QueryNode

	clusters sync.Map // channel name => *shardCluster
}

// newShardClusterService returns a new shardClusterService
func newShardClusterService(client *clientv3.Client, session *sessionutil.Session, node *QueryNode) *ShardClusterService {
	return &ShardClusterService{
		node:     node,
		session:  session,
		client:   client,
		clusters: sync.Map{},
	}
}

// addShardCluster adds shardCluster into service.
func (s *ShardClusterService) addShardCluster(collectionID, replicaID int64, vchannelName string) {
	nodeDetector := NewEtcdShardNodeDetector(s.client, path.Join(Params.EtcdCfg.MetaRootPath, ReplicaMetaPrefix),
		func() (map[int64]string, error) {
			result := make(map[int64]string)
			sessions, _, err := s.session.GetSessions(typeutil.QueryNodeRole)
			if err != nil {
				return nil, err
			}
			for _, session := range sessions {
				result[session.ServerID] = session.Address
			}
			return result, nil
		})

	segmentDetector := NewEtcdShardSegmentDetector(s.client, path.Join(Params.EtcdCfg.MetaRootPath, util.SegmentMetaPrefix, strconv.FormatInt(collectionID, 10)))

	cs := NewShardCluster(collectionID, replicaID, vchannelName, nodeDetector, segmentDetector,
		func(nodeID int64, addr string) shardQueryNode {
			if nodeID == s.session.ServerID {
				// wrap node itself
				return &shardQueryNodeWrapper{QueryNode: s.node}
			}
			ctx := context.Background()
			qn, _ := grpcquerynodeclient.NewClient(ctx, addr)
			return qn
		})

	s.clusters.Store(vchannelName, cs)
}

// getShardCluster gets shardCluster of specified vchannel if exists.
func (s *ShardClusterService) getShardCluster(vchannelName string) (*ShardCluster, bool) {
	raw, ok := s.clusters.Load(vchannelName)
	if !ok {
		return nil, false
	}
	return raw.(*ShardCluster), true
}

// releaseShardCluster removes shardCluster from service and stops it.
func (s *ShardClusterService) releaseShardCluster(vchannelName string) error {
	raw, ok := s.clusters.LoadAndDelete(vchannelName)
	if !ok {
		return fmt.Errorf("ShardCluster of channel: %s does not exists", vchannelName)
	}

	cs := raw.(*ShardCluster)
	cs.Close()
	return nil
}

// releaseCollection removes all shardCluster matching specified collectionID
func (s *ShardClusterService) releaseCollection(collectionID int64) {
	s.clusters.Range(func(k, v interface{}) bool {
		cs := v.(*ShardCluster)
		if cs.collectionID == collectionID {
			s.releaseShardCluster(k.(string))
		}
		return true
	})
}
