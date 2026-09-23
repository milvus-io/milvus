package datacoord

import (
	"cmp"
	"context"
	"slices"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (s *Server) GetStreamingNodeQueryViewResources(ctx context.Context, req *datapb.GetStreamingNodeQueryViewResourcesRequest) (*datapb.GetStreamingNodeQueryViewResourcesResponse, error) {
	resp := &datapb.GetStreamingNodeQueryViewResourcesResponse{
		Status:       merr.Success(),
		CollectionId: req.GetCollectionId(),
		Vchannel:     req.GetVchannel(),
		DataVersion:  req.GetDataVersion(),
	}
	if err := merr.CheckHealthy(s.GetStateCode()); err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	if req.GetCollectionId() == 0 {
		resp.Status = merr.Status(merr.WrapErrParameterInvalidMsg("collection id is zero"))
		return resp, nil
	}
	if req.GetVchannel() == "" {
		resp.Status = merr.Status(merr.WrapErrParameterInvalidMsg("vchannel is empty"))
		return resp, nil
	}
	if req.GetDataVersion() == nil {
		resp.Status = merr.Status(merr.WrapErrParameterInvalidMsg("data version is nil"))
		return resp, nil
	}
	if s.dataViewManager == nil {
		resp.Status = merr.Status(merr.WrapErrServiceInternalMsg("data view manager is nil"))
		return resp, nil
	}
	ref, err := s.dataViewManager.Get(ctx, req.GetCollectionId(), req.GetDataVersion())
	if err != nil {
		resp.Status = merr.Status(err)
		return resp, nil
	}
	if ref == nil {
		resp.Status = merr.Status(merr.WrapErrServiceNotReadyMsg("requested data view is unavailable"))
		return resp, nil
	}
	defer ref.Deref()
	shard := dataViewShard(ref.DataView(), req.GetVchannel())
	if shard == nil {
		resp.Status = merr.Status(merr.WrapErrServiceInternalMsg(
			"data view shard not found, collectionID=%d, vchannel=%s, dataVersion=(%d,%d)",
			req.GetCollectionId(),
			req.GetVchannel(),
			req.GetDataVersion().GetStreamingVersion(),
			req.GetDataVersion().GetCompactVersion(),
		))
		return resp, nil
	}

	segmentIDs := dataViewShardSegmentIDs(shard, req.GetPartitionIds())
	manifestVersions := make(map[int64]int64)
	for _, partition := range shard.GetPartitions() {
		for i, version := range partition.GetSegmentManifestVersions() {
			manifestVersions[partition.GetSegmentIds()[i]] = version
		}
	}
	if len(segmentIDs) == 0 {
		return resp, nil
	}
	byID := make(map[int64]*datapb.StreamingNodeBM25Resource, len(segmentIDs))
	for _, segmentID := range segmentIDs {
		segment := s.meta.GetSegment(ctx, segmentID)
		if segment == nil {
			resp.Status = merr.Status(merr.WrapErrSegmentNotFound(segmentID, "missing segment info for data view"))
			return resp, nil
		}
		if segment.GetInsertChannel() != "" && segment.GetInsertChannel() != req.GetVchannel() {
			resp.Status = merr.Status(merr.WrapErrServiceInternalMsg(
				"segment channel mismatch, segmentID=%d, expected=%s, actual=%s",
				segment.GetID(),
				req.GetVchannel(),
				segment.GetInsertChannel(),
			))
			return resp, nil
		}
		manifestPath := segment.GetManifestPath()
		if version, ok := manifestVersions[segmentID]; ok && version > 0 && manifestPath != "" {
			base, _, err := packed.UnmarshalManifestPath(manifestPath)
			if err != nil {
				resp.Status = merr.Status(merr.Wrap(err, "decode query view segment manifest"))
				return resp, nil
			}
			manifestPath = packed.MarshalManifestPath(base, version)
		}
		byID[segment.GetID()] = &datapb.StreamingNodeBM25Resource{
			SegmentId:      segment.GetID(),
			PartitionId:    segment.GetPartitionID(),
			Bm25Binlogs:    segment.GetBm25Statslogs(),
			StorageVersion: segment.GetStorageVersion(),
			ManifestPath:   manifestPath,
		}
	}
	for _, segmentID := range segmentIDs {
		resource, ok := byID[segmentID]
		if !ok {
			resp.Status = merr.Status(merr.WrapErrSegmentNotFound(segmentID, "missing segment info for data view"))
			return resp, nil
		}
		resp.Bm25Resources = append(resp.Bm25Resources, resource)
	}
	return resp, nil
}

func dataViewShard(dataView *viewpb.DataViewOfCollection, vchannel string) *viewpb.DataViewOfShard {
	if dataView == nil {
		return nil
	}
	for _, shard := range dataView.GetShards() {
		if shard.GetVchannel() == vchannel {
			return shard
		}
	}
	return nil
}

func dataViewShardSegmentIDs(shard *viewpb.DataViewOfShard, partitionIDs []int64) []int64 {
	if shard == nil {
		return nil
	}
	requiredPartitions := typeutil.NewSet(partitionIDs...)
	loadsAllPartitions := len(requiredPartitions) == 0
	segmentIDs := make([]int64, 0)
	for _, partition := range shard.GetPartitions() {
		if !loadsAllPartitions && !requiredPartitions.Contain(partition.GetPartitionId()) {
			continue
		}
		segmentIDs = append(segmentIDs, partition.GetSegmentIds()...)
	}
	return segmentIDs
}

func (s *Server) GetQueryViewCollectionIndexInfos(collectionID int64) []*indexpb.IndexInfo {
	return s.queryViewCollectionIndexInfos(collectionID)
}

func (s *Server) queryViewCollectionIndexInfos(collectionID int64) []*indexpb.IndexInfo {
	indexes := s.meta.indexMeta.GetIndexesForCollection(collectionID, "")
	return packQueryViewCollectionIndexInfos(indexes)
}

func packQueryViewCollectionIndexInfos(indexes []*model.Index) []*indexpb.IndexInfo {
	infos := lo.Map(indexes, func(index *model.Index, _ int) *indexpb.IndexInfo {
		return &indexpb.IndexInfo{
			CollectionID:    index.CollectionID,
			FieldID:         index.FieldID,
			IndexName:       index.IndexName,
			IndexID:         index.IndexID,
			TypeParams:      index.TypeParams,
			IndexParams:     index.IndexParams,
			IsAutoIndex:     index.IsAutoIndex,
			UserIndexParams: index.UserIndexParams,
		}
	})
	slices.SortFunc(infos, func(left, right *indexpb.IndexInfo) int {
		return cmp.Compare(left.GetIndexID(), right.GetIndexID())
	})
	return infos
}
