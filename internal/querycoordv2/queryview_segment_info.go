package querycoordv2

import (
	"cmp"
	"context"
	"maps"
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (s *Server) queryViewSegmentInfo(ctx context.Context, req *querypb.GetSegmentInfoRequest) ([]*querypb.SegmentInfo, error) {
	collections := []int64{req.GetCollectionID()}
	// Preserve the internal, deprecated segment-ID-only request as well as the
	// public collection-scoped API. No QueryNode resource lookup is necessary.
	if req.GetCollectionID() == 0 && len(req.GetSegmentIDs()) > 0 {
		collections = slices.Collect(maps.Keys(s.qviewsRuntime.loadConfigStore.Snapshot().ConfigsMap()))
	}
	requested := make(map[int64]struct{}, len(req.GetSegmentIDs()))
	for _, id := range req.GetSegmentIDs() {
		requested[id] = struct{}{}
	}
	var infos []*querypb.SegmentInfo
	found := make(map[int64]struct{})
	for _, collectionID := range collections {
		collectionInfos, err := s.queryViewCollectionSegmentInfo(ctx, collectionID, requested)
		if err != nil {
			return nil, err
		}
		for _, info := range collectionInfos {
			found[info.GetSegmentID()] = struct{}{}
		}
		infos = append(infos, collectionInfos...)
	}
	for _, id := range req.GetSegmentIDs() {
		if _, ok := found[id]; !ok {
			return nil, merr.WrapErrSegmentNotLoaded(id)
		}
	}
	slices.SortFunc(infos, func(a, b *querypb.SegmentInfo) int { return cmp.Compare(a.GetSegmentID(), b.GetSegmentID()) })
	return infos, nil
}

func (s *Server) queryViewCollectionSegmentInfo(ctx context.Context, collectionID int64, requested map[int64]struct{}) ([]*querypb.SegmentInfo, error) {
	store, registry := s.qviewsRuntime.loadConfigStore, s.qviewsRuntime.shardViewRegistry
	snapshot := func() map[qviews.ShardID]*coordview.ShardStats {
		return registry.SnapshotForCollection(collectionID).StatsMap()
	}
	for range 3 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cfg, version := store.GetConfigWithVersion(collectionID)
		if cfg == nil {
			return nil, nil
		}
		stats := snapshot()
		placements := queryViewUpSegments(cfg, stats, requested)
		ids := slices.Sorted(maps.Keys(placements))
		var metadata []*querypb.SegmentLoadInfo
		var err error
		if len(ids) > 0 {
			// MixCoord dispatches this directly to its local DataCoord metadata.
			metadata, _, err = s.mixCoord.GetQueryViewSegmentLoadInfos(ctx, collectionID, ids)
		}
		_, currentVersion := store.GetConfigWithVersion(collectionID)
		// Stats are immutable publications. Compare scoped pointers, not the
		// registry's global version, so unrelated collections cannot starve us.
		if currentVersion != version || !maps.Equal(stats, snapshot()) {
			continue
		}
		if err != nil {
			return nil, err
		}
		infos := make([]*querypb.SegmentInfo, 0, len(metadata))
		for _, meta := range metadata {
			info := placements[meta.GetSegmentID()]
			info.NumRows = meta.GetNumOfRows()
			info.Level = meta.GetLevel()
			info.IsSorted = meta.GetIsSorted()
			info.StorageVersion = meta.GetStorageVersion()
			info.IndexInfos = meta.GetIndexInfos()
			if len(info.IndexInfos) > 0 {
				info.IndexName = info.IndexInfos[0].GetIndexName()
				info.IndexID = info.IndexInfos[0].GetIndexID()
			}
			// MemSize retains the existing Coord API's zero/default semantics.
			infos = append(infos, info)
		}
		return infos, nil
	}
	return nil, merr.WrapErrServiceUnavailableMsg("query view changed while reading segment information for collection %d", collectionID)
}

func queryViewUpSegments(cfg *loadmgr.LoadConfig, stats map[qviews.ShardID]*coordview.ShardStats, requested map[int64]struct{}) map[int64]*querypb.SegmentInfo {
	replicas := make(map[int64]struct{}, len(cfg.Replicas))
	for _, replica := range cfg.Replicas {
		replicas[replica.ReplicaID] = struct{}{}
	}
	latest := make(map[string]qviews.DataVersion)
	for sid, stat := range stats {
		if _, ok := replicas[sid.ReplicaID]; !ok || stat.UpVersion == nil {
			continue
		}
		version := stat.UpVersion.DataVersion
		if previous, ok := latest[sid.VChannel]; !ok || version.GT(previous) {
			latest[sid.VChannel] = version
		}
	}
	infos := make(map[int64]*querypb.SegmentInfo)
	for sid, stat := range stats {
		if _, ok := replicas[sid.ReplicaID]; !ok || stat.UpVersion == nil || !stat.UpVersion.DataVersion.EQ(latest[sid.VChannel]) {
			continue
		}
		for id, segment := range stat.Segments {
			if _, ok := requested[id]; len(requested) > 0 && !ok {
				continue
			}
			for node, state := range segment.Nodes {
				if state != coordview.SegmentStateUp {
					continue
				}
				info := infos[id]
				if info == nil {
					info = &querypb.SegmentInfo{
						SegmentID: id, CollectionID: cfg.CollectionID, PartitionID: segment.PartitionID,
						DmChannel: sid.VChannel, SegmentState: commonpb.SegmentState_Sealed,
					}
					infos[id] = info
				}
				info.NodeIds = append(info.NodeIds, node)
				info.ReplicaIds = append(info.ReplicaIds, sid.ReplicaID)
			}
		}
	}
	for _, info := range infos {
		slices.Sort(info.NodeIds)
		info.NodeIds = slices.Compact(info.NodeIds)
		slices.Sort(info.ReplicaIds)
		info.ReplicaIds = slices.Compact(info.ReplicaIds)
	}
	return infos
}
