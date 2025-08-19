package replicatemanager

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type ClusterReplicator interface {
	// StartReplicateCluster starts the replication for the target cluster.
	StartReplicateCluster()

	// StopReplicateCluster stops the replication for the target cluster
	// and wait for the replication to exit.
	StopReplicateCluster()
}

var _ ClusterReplicator = (*clusterReplicator)(nil)

// clusterReplicator is the implementation of ClusterReplicator.
type clusterReplicator struct {
	cluster            *milvuspb.MilvusCluster
	channelReplicators *typeutil.ConcurrentMap[string, ChannelReplicator]

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewClusterReplicator creates a new ClusterReplicator.
func NewClusterReplicator(cluster *milvuspb.MilvusCluster) ClusterReplicator {
	ctx, cancel := context.WithCancel(context.Background())
	return &clusterReplicator{
		ctx:                ctx,
		cancel:             cancel,
		cluster:            cluster,
		channelReplicators: typeutil.NewConcurrentMap[string, ChannelReplicator](),
	}
}

func (r *clusterReplicator) StartReplicateCluster() {
	for _, channel := range r.cluster.GetPchannels() {
		channelReplicator := NewChannelReplicator(channel, r.cluster)
		channelReplicator.StartReplicateChannel()
		r.channelReplicators.Insert(channel, channelReplicator)
	}
	r.wg.Add(1)
	go r.updateMetricsLoop()
}

func (r *clusterReplicator) StopReplicateCluster() {
	r.channelReplicators.Range(func(key string, value ChannelReplicator) bool {
		value.StopReplicateChannel()
		return true
	})
	r.cancel()
	r.wg.Wait()
}

func (r *clusterReplicator) updateMetricsLoop() {
	defer r.wg.Done()
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.updateMetrics()
		}
	}
}

func (r *clusterReplicator) updateMetrics() {
	// TODO: sheep, update metrics
}
