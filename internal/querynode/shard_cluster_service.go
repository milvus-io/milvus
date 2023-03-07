package querynode

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"sync"

	"github.com/cockroachdb/errors"

	grpcquerynodeclient "github.com/milvus-io/milvus/internal/distributed/querynode/client"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	ReplicaMetaPrefix = "querycoord-replica"
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
func (s *ShardClusterService) addShardCluster(collectionID, replicaID int64, vchannelName string, version int64) {
	nodeDetector := NewEtcdShardNodeDetector(s.client, path.Join(Params.EtcdCfg.MetaRootPath.GetValue(), ReplicaMetaPrefix),
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

	segmentDetector := NewEtcdShardSegmentDetector(s.client, path.Join(Params.EtcdCfg.MetaRootPath.GetValue(), util.SegmentMetaPrefix, strconv.FormatInt(collectionID, 10)))

	cs := NewShardCluster(collectionID, replicaID, vchannelName, version, nodeDetector, segmentDetector,
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
	log.Info("successfully add shard cluster", zap.Int64("collectionID", collectionID), zap.Int64("replica", replicaID), zap.String("vchan", vchannelName))
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
func (s *ShardClusterService) releaseShardCluster(vchannelName string) {
	if raw, ok := s.clusters.LoadAndDelete(vchannelName); ok {
		raw.(*ShardCluster).Close()
	}
}

func (s *ShardClusterService) close() error {
	log.Debug("start to close shard cluster service")

	isFinish := true
	s.clusters.Range(func(key, value any) bool {
		cs, ok := value.(*ShardCluster)
		if !ok {
			log.Error("convert to ShardCluster fail, close shard cluster is interrupted", zap.Any("key", key))
			isFinish = false
			return false
		}

		cs.Close()
		return true
	})

	if isFinish {
		return nil
	}

	return errors.New("close shard cluster failed")
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
	log.Info("successfully release collection", zap.Int64("collectionID", collectionID))
}

// SyncReplicaSegments dispatches nodeID segments distribution to ShardCluster.
func (s *ShardClusterService) SyncReplicaSegments(vchannelName string, distribution []*querypb.ReplicaSegmentsInfo) error {
	sc, ok := s.getShardCluster(vchannelName)
	if !ok {
		return fmt.Errorf("Leader of VChannel %s is not this QueryNode %d", vchannelName, s.session.ServerID)
	}

	sc.SyncSegments(distribution, segmentStateLoaded)
	log.Info("successfully sync segments", zap.String("channel", vchannelName), zap.Any("distribution", distribution))
	return nil
}

func (s *ShardClusterService) GetShardClusters() []*ShardCluster {
	ret := make([]*ShardCluster, 0)
	s.clusters.Range(func(key, value any) bool {
		ret = append(ret, value.(*ShardCluster))
		return true
	})
	return ret
}
