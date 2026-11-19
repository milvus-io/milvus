package querycoordv2

import (
	"context"
	"slices"

	"github.com/milvus-io/milvus/internal/datacoord/split"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (r *qviewsRuntime) splitTargetAdmission(ctx context.Context, request *querypb.GetQueryViewLoadInfoRequest) (bool, error) {
	id := request.GetCollectionID()
	if r.balanceCache == nil || r.resources == nil || r.shardViewRegistry == nil || r.splitHandover == nil {
		return false, merr.WrapErrServiceNotReadyMsg("split serving metadata unavailable")
	}
	if !hasSplitSourceFence(r.splitHandover.backend.ListShardSplits(), request) {
		return false, nil
	}
	entry := r.balanceCache.GetCollection(id)
	if entry == nil || entry.DataView() == nil {
		return false, nil
	}
	native := entry.DataView()
	if native.ShardSetEpoch < 2 || native.Shard(request.GetSplitTarget()) == nil {
		return false, nil
	}
	if source := native.Shard(request.GetSplitSource()); source != nil {
		known := false
		for _, target := range source.Lineage.GetVchannels() {
			known = known || target == request.GetSplitTarget()
		}
		if !known {
			return false, nil
		}
	} else if native.ShardSetEpoch < 3 {
		return false, nil
	}
	cfg := r.loadConfigStore.GetConfig(id)
	if cfg == nil {
		// Release may occur during the window. Once all old views are confirmed
		// out of service, absence of a read generation must not fence writes forever.
		return r.shardViewRegistry.ServingTransitionComplete(id, 0, true), nil
	}
	ref, err := r.resources.DataViews.Get(ctx, id, native.DataVersion.IntoProto())
	if err != nil {
		return false, err
	}
	if ref == nil {
		return false, merr.WrapErrServiceNotReadyMsg("split DataView unavailable")
	}
	defer ref.Deref()
	var replicas []int64
	for _, replica := range cfg.Replicas {
		replicas = append(replicas, replica.ReplicaID)
	}
	published, err := r.shardViewRegistry.ResolveServingGeneration(ctx, ref.DataView(), replicas)
	if err != nil {
		return false, err
	}
	targetPublished := false
	for _, shard := range published.GetShards() {
		targetPublished = targetPublished || shard.GetVchannel() == request.GetSplitTarget()
	}
	return targetPublished && r.shardViewRegistry.ServingTransitionComplete(id, published.GetShardSetEpoch(), false), nil
}

func hasSplitSourceFence(tasks []*datapb.SplitShardTask, request *querypb.GetQueryViewLoadInfoRequest) bool {
	for _, task := range tasks {
		if task.GetCollectionId() == request.GetCollectionID() && task.GetSourceVchannel() == request.GetSplitSource() &&
			slices.ContainsFunc(task.GetTargets(), func(target *datapb.SplitShardTaskTarget) bool {
				return target.GetVchannel() == request.GetSplitTarget()
			}) {
			return split.HasSourceFenceProof(task)
		}
	}
	return false
}
