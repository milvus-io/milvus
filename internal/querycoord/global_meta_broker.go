package querycoord

import (
	"context"
	"errors"
	"fmt"
	"path"

	"github.com/milvus-io/milvus/internal/util/retry"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

type globalMetaBroker struct {
	ctx    context.Context
	cancel context.CancelFunc

	rootCoord  types.RootCoord
	dataCoord  types.DataCoord
	indexCoord types.IndexCoord

	cm storage.ChunkManager
}

func newGlobalMetaBroker(ctx context.Context, rootCoord types.RootCoord, dataCoord types.DataCoord, indexCoord types.IndexCoord, cm storage.ChunkManager) (*globalMetaBroker, error) {
	childCtx, cancel := context.WithCancel(ctx)
	parser := &globalMetaBroker{
		ctx:        childCtx,
		cancel:     cancel,
		rootCoord:  rootCoord,
		dataCoord:  dataCoord,
		indexCoord: indexCoord,
		cm:         cm,
	}
	return parser, nil
}

// invalidateCollectionMetaCache notifies RootCoord to remove all the collection meta cache with the specified collectionID in Proxies
func (broker *globalMetaBroker) invalidateCollectionMetaCache(ctx context.Context, collectionID UniqueID) error {
	ctx1, cancel1 := context.WithTimeout(ctx, timeoutForRPC)
	defer cancel1()
	req := &proxypb.InvalidateCollMetaCacheRequest{
		Base: &commonpb.MsgBase{
			MsgType: 0, // TODO: msg type?
		},
		CollectionID: collectionID,
	}

	res, err := broker.rootCoord.InvalidateCollectionMetaCache(ctx1, req)
	if err != nil {
		log.Error("InvalidateCollMetaCacheRequest failed", zap.Int64("collectionID", collectionID), zap.Error(err))
		return err
	}
	if res.ErrorCode != commonpb.ErrorCode_Success {
		err = errors.New(res.Reason)
		log.Error("InvalidateCollMetaCacheRequest failed", zap.Int64("collectionID", collectionID), zap.Error(err))
		return err
	}
	log.Info("InvalidateCollMetaCacheRequest successfully", zap.Int64("collectionID", collectionID))

	return nil
}

func (broker *globalMetaBroker) showPartitionIDs(ctx context.Context, collectionID UniqueID) ([]UniqueID, error) {
	ctx2, cancel2 := context.WithTimeout(ctx, timeoutForRPC)
	defer cancel2()
	showPartitionRequest := &milvuspb.ShowPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ShowPartitions,
		},
		CollectionID: collectionID,
	}
	showPartitionResponse, err := broker.rootCoord.ShowPartitions(ctx2, showPartitionRequest)
	if err != nil {
		log.Error("showPartition failed", zap.Int64("collectionID", collectionID), zap.Error(err))
		return nil, err
	}

	if showPartitionResponse.Status.ErrorCode != commonpb.ErrorCode_Success {
		err = errors.New(showPartitionResponse.Status.Reason)
		log.Error("showPartition failed", zap.Int64("collectionID", collectionID), zap.Error(err))
		return nil, err
	}
	log.Info("show partition successfully", zap.Int64("collectionID", collectionID), zap.Int64s("partitionIDs", showPartitionResponse.PartitionIDs))

	return showPartitionResponse.PartitionIDs, nil
}

func (broker *globalMetaBroker) getRecoveryInfo(ctx context.Context, collectionID UniqueID, partitionID UniqueID) ([]*datapb.VchannelInfo, []*datapb.SegmentBinlogs, error) {
	ctx2, cancel2 := context.WithTimeout(ctx, timeoutForRPC)
	defer cancel2()
	getRecoveryInfoRequest := &datapb.GetRecoveryInfoRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_GetRecoveryInfo,
		},
		CollectionID: collectionID,
		PartitionID:  partitionID,
	}
	recoveryInfo, err := broker.dataCoord.GetRecoveryInfo(ctx2, getRecoveryInfoRequest)
	if err != nil {
		log.Error("get recovery info failed", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID), zap.Error(err))
		return nil, nil, err
	}

	if recoveryInfo.Status.ErrorCode != commonpb.ErrorCode_Success {
		err = errors.New(recoveryInfo.Status.Reason)
		log.Error("get recovery info failed", zap.Int64("collectionID", collectionID), zap.Int64("partitionID", partitionID), zap.Error(err))
		return nil, nil, err
	}
	log.Info("get recovery info successfully",
		zap.Int64("collectionID", collectionID),
		zap.Int64("partitionID", partitionID),
		zap.Int("num channels", len(recoveryInfo.Channels)),
		zap.Int("num segments", len(recoveryInfo.Binlogs)))

	return recoveryInfo.Channels, recoveryInfo.Binlogs, nil
}

//func (broker *globalMetaBroker) getIndexBuildID(ctx context.Context, collectionID UniqueID, segmentID UniqueID) (bool, int64, error) {
//	req := &milvuspb.DescribeSegmentRequest{
//		Base: &commonpb.MsgBase{
//			MsgType: commonpb.MsgType_DescribeSegment,
//		},
//		CollectionID: collectionID,
//		SegmentID:    segmentID,
//	}
//	ctx2, cancel2 := context.WithTimeout(ctx, timeoutForRPC)
//	defer cancel2()
//	response, err := broker.rootCoord.DescribeSegment(ctx2, req)
//	if err != nil {
//		log.Error("describe segment from rootCoord failed",
//			zap.Int64("collectionID", collectionID),
//			zap.Int64("segmentID", segmentID),
//			zap.Error(err))
//		return false, 0, err
//	}
//	if response.Status.ErrorCode != commonpb.ErrorCode_Success {
//		err = errors.New(response.Status.Reason)
//		log.Error("describe segment from rootCoord failed",
//			zap.Int64("collectionID", collectionID),
//			zap.Int64("segmentID", segmentID),
//			zap.Error(err))
//		return false, 0, err
//	}
//
//	if !response.EnableIndex {
//		log.Info("describe segment from rootCoord successfully",
//			zap.Int64("collectionID", collectionID),
//			zap.Int64("segmentID", segmentID),
//			zap.Bool("enableIndex", false))
//		return false, 0, nil
//	}
//
//	log.Info("describe segment from rootCoord successfully",
//		zap.Int64("collectionID", collectionID),
//		zap.Int64("segmentID", segmentID),
//		zap.Bool("enableIndex", true),
//		zap.Int64("buildID", response.BuildID))
//	return true, response.BuildID, nil
//}

func (broker *globalMetaBroker) getIndexFilePaths(ctx context.Context, collID UniqueID, indexName string, segmentIDs []int64) (*indexpb.GetIndexInfoResponse, error) {
	indexFilePathRequest := &indexpb.GetIndexInfoRequest{
		CollectionID: collID,
		SegmentIDs:   segmentIDs,
		IndexName:    indexName,
	}

	ctx3, cancel3 := context.WithTimeout(ctx, timeoutForRPC)
	defer cancel3()
	pathResponse, err := broker.indexCoord.GetIndexInfos(ctx3, indexFilePathRequest)
	if err != nil {
		log.Error("get index info from indexCoord failed", zap.Int64s("segmentIDs", segmentIDs),
			zap.String("indexName", indexName), zap.Error(err))
		return nil, err
	}

	if pathResponse.Status.ErrorCode != commonpb.ErrorCode_Success {
		err = fmt.Errorf("get index info from indexCoord failed, reason = %s", pathResponse.Status.Reason)
		log.Error(err.Error())
		return nil, err
	}
	log.Info("get index info from indexCoord successfully", zap.Int64s("segmentIDs", segmentIDs))

	return pathResponse, nil
}

//func (broker *globalMetaBroker) parseIndexInfo(ctx context.Context, segmentID UniqueID, indexInfo *querypb.FieldIndexInfo) error {
//	resp, err := broker.getIndexFilePaths(ctx, 0, indexInfo.IndexName, []UniqueID{segmentID})
//	if err != nil {
//		return err
//	}
//	if !resp.EnableIndex {
//		log.Debug(fmt.Sprintf("fieldID %d of segment %d don't has index", indexInfo.FieldID, segmentID))
//		return nil
//	}
//
//	if len(resp.FilePaths) != 1 {
//		err = fmt.Errorf("illegal index file paths, there should be only one vector column,  segmentID = %d, fieldID = %d", segmentID, indexInfo.FieldID)
//		log.Error(err.Error())
//		return err
//	}
//
//	fieldPathInfo := resp.FilePaths[0]
//	if len(fieldPathInfo.IndexFilePaths) == 0 {
//		err = fmt.Errorf("empty index paths, segmentID = %d, fieldID = %d", segmentID, indexInfo.FieldID)
//		log.Error(err.Error())
//		return err
//	}
//
//	indexInfo.IndexFilePaths = fieldPathInfo.IndexFilePaths
//	indexInfo.IndexSize = int64(fieldPathInfo.SerializedSize)
//
//	log.Debug("get indexFilePath info from indexCoord success", zap.Int64("segmentID", segmentID),
//		zap.Int64("fieldID", indexInfo.FieldID), zap.Strings("indexPaths", fieldPathInfo.IndexFilePaths))
//
//	indexCodec := storage.NewIndexFileBinlogCodec()
//	for _, indexFilePath := range fieldPathInfo.IndexFilePaths {
//		// get index params when detecting indexParamPrefix
//		if path.Base(indexFilePath) == storage.IndexParamsKey {
//			indexPiece, err := broker.cm.Read(indexFilePath)
//			if err != nil {
//				log.Error("load index params file failed",
//					zap.Int64("segmentID", segmentID),
//					zap.Int64("fieldID", indexInfo.FieldID),
//					zap.String("index params filePath", indexFilePath),
//					zap.Error(err))
//				return err
//			}
//			_, indexParams, indexName, indexID, err := indexCodec.Deserialize([]*storage.Blob{{Key: storage.IndexParamsKey, Value: indexPiece}})
//			if err != nil {
//				log.Error("deserialize index params file failed",
//					zap.Int64("segmentID", segmentID),
//					zap.Int64("fieldID", indexInfo.FieldID),
//					zap.String("index params filePath", indexFilePath),
//					zap.Error(err))
//				return err
//			}
//			if len(indexParams) <= 0 {
//				err = fmt.Errorf("cannot find index param, segmentID = %d, fieldID = %d, indexFilePath = %s", segmentID, indexInfo.FieldID, indexFilePath)
//				log.Error(err.Error())
//				return err
//			}
//			indexInfo.IndexName = indexName
//			indexInfo.IndexID = indexID
//			indexInfo.IndexParams = funcutil.Map2KeyValuePair(indexParams)
//			break
//		}
//	}
//
//	if len(indexInfo.IndexParams) == 0 {
//		err = fmt.Errorf("no index params in Index file, segmentID = %d, fieldID = %d, indexPaths = %v", segmentID, indexInfo.FieldID, fieldPathInfo.IndexFilePaths)
//		log.Error(err.Error())
//		return err
//	}
//
//	log.Info("set index info  success", zap.Int64("segmentID", segmentID), zap.Int64("fieldID", indexInfo.FieldID))
//
//	return nil
//}

// Better to let index params key appear in the file paths first.
func (broker *globalMetaBroker) loadIndexExtraInfo(ctx context.Context, fieldPathInfo *indexpb.IndexFilePathInfo) (*extraIndexInfo, error) {
	indexCodec := storage.NewIndexFileBinlogCodec()
	for _, indexFilePath := range fieldPathInfo.IndexFilePaths {
		// get index params when detecting indexParamPrefix
		if path.Base(indexFilePath) == storage.IndexParamsKey {
			content, err := broker.cm.MultiRead([]string{indexFilePath})
			if err != nil {
				return nil, err
			}

			if len(content) <= 0 {
				return nil, fmt.Errorf("failed to read index file binlog, path: %s", indexFilePath)
			}

			indexPiece := content[0]
			_, indexParams, indexName, _, err := indexCodec.Deserialize([]*storage.Blob{{Key: storage.IndexParamsKey, Value: indexPiece}})
			if err != nil {
				return nil, err
			}

			return &extraIndexInfo{
				indexName:   indexName,
				indexParams: funcutil.Map2KeyValuePair(indexParams),
			}, nil
		}
	}
	return nil, errors.New("failed to load index extra info")
}

//func (broker *globalMetaBroker) describeSegments(ctx context.Context, collectionID UniqueID, segmentIDs []UniqueID) (*rootcoordpb.DescribeSegmentsResponse, error) {
//	resp, err := broker.rootCoord.DescribeSegments(ctx, &rootcoordpb.DescribeSegmentsRequest{
//		Base: &commonpb.MsgBase{
//			MsgType: commonpb.MsgType_DescribeSegments,
//		},
//		CollectionID: collectionID,
//		SegmentIDs:   segmentIDs,
//	})
//	if err != nil {
//		log.Error("failed to describe segments",
//			zap.Int64("collection", collectionID),
//			zap.Int64s("segments", segmentIDs),
//			zap.Error(err))
//		return nil, err
//	}
//
//	log.Info("describe segments successfully",
//		zap.Int64("collection", collectionID),
//		zap.Int64s("segments", segmentIDs))
//
//	return resp, nil
//}

// return: segment_id -> segment_index_infos
//func (broker *globalMetaBroker) getFullIndexInfos(ctx context.Context, collectionID UniqueID, segmentID UniqueID) ([]*querypb.FieldIndexInfo, error) {
//	ret := make([]*querypb.FieldIndexInfo, 0)
//	resp, err := broker.getIndexFilePaths(ctx, collectionID, "", []UniqueID{segmentID})
//	if err != nil {
//		log.Warn("failed to get index file paths", zap.Int64("collection", collectionID),
//			zap.Int64("segmentID", segmentID), zap.Error(err))
//		return nil, err
//	}
//	if resp.EnableIndex {
//		for _, indexInfo := range resp.FilePaths {
//			ret = append(ret, &querypb.FieldIndexInfo{
//				FieldID:        indexInfo.FieldID,
//				EnableIndex:    true,
//				IndexName:      indexInfo.IndexName,
//				IndexID:        indexInfo.IndexID,
//				BuildID:        indexInfo.BuildID,
//				IndexParams:    indexInfo.IndexParams,
//				IndexFilePaths: indexInfo.IndexFilePaths,
//				IndexSize:      int64(indexInfo.SerializedSize),
//			})
//		}
//	}
//
//	return ret, nil
//}

// return: segment_id -> segment_index_infos
func (broker *globalMetaBroker) getFullIndexInfos(ctx context.Context, collectionID UniqueID, segmentIDs []UniqueID) (map[UniqueID][]*querypb.FieldIndexInfo, error) {
	resp, err := broker.getIndexFilePaths(ctx, collectionID, "", segmentIDs)
	if err != nil {
		log.Warn("failed to get index file paths", zap.Int64("collection", collectionID),
			zap.Int64s("segmentIDs", segmentIDs), zap.Error(err))
		return nil, err
	}

	ret := make(map[UniqueID][]*querypb.FieldIndexInfo)
	for _, segmentID := range segmentIDs {
		infos, ok := resp.GetSegmentInfo()[segmentID]
		if !ok {
			log.Warn("segment not found",
				zap.Int64("collection", collectionID),
				zap.Int64("segment", segmentID))
			return nil, fmt.Errorf("segment not found, collection: %d, segment: %d", collectionID, segmentID)
		}

		if _, ok := ret[segmentID]; !ok {
			ret[segmentID] = make([]*querypb.FieldIndexInfo, 0, len(infos.IndexInfos))
		}

		for _, info := range infos.IndexInfos {
			//extraInfo, ok := infos.GetExtraIndexInfos()[info.IndexID]
			indexInfo := &querypb.FieldIndexInfo{
				FieldID:        info.FieldID,
				EnableIndex:    true,
				IndexName:      info.IndexName,
				IndexID:        info.IndexID,
				BuildID:        info.BuildID,
				IndexParams:    info.IndexParams,
				IndexFilePaths: info.IndexFilePaths,
				IndexSize:      int64(info.SerializedSize),
			}

			if len(info.IndexFilePaths) <= 0 {
				log.Warn("index not ready", zap.Int64("index_build_id", info.BuildID))
				return nil, fmt.Errorf("index not ready, index build id: %d", info.BuildID)
			}

			ret[segmentID] = append(ret[segmentID], indexInfo)
		}
	}

	return ret, nil
}

func (broker *globalMetaBroker) getIndexInfo(ctx context.Context, collectionID UniqueID, segmentID UniqueID, schema *schemapb.CollectionSchema) ([]*querypb.FieldIndexInfo, error) {
	segmentIndexInfos, err := broker.getFullIndexInfos(ctx, collectionID, []UniqueID{segmentID})
	if err != nil {
		return nil, err
	}
	if infos, ok := segmentIndexInfos[segmentID]; ok {
		return infos, nil
	}
	return nil, fmt.Errorf("failed to get segment index infos, collection: %d, segment: %d", collectionID, segmentID)
}

func (broker *globalMetaBroker) generateSegmentLoadInfo(ctx context.Context,
	collectionID UniqueID,
	partitionID UniqueID,
	segmentBinlog *datapb.SegmentBinlogs,
	setIndex bool,
	schema *schemapb.CollectionSchema) *querypb.SegmentLoadInfo {
	segmentID := segmentBinlog.SegmentID
	segmentLoadInfo := &querypb.SegmentLoadInfo{
		SegmentID:     segmentID,
		PartitionID:   partitionID,
		CollectionID:  collectionID,
		BinlogPaths:   segmentBinlog.FieldBinlogs,
		NumOfRows:     segmentBinlog.NumOfRows,
		Statslogs:     segmentBinlog.Statslogs,
		Deltalogs:     segmentBinlog.Deltalogs,
		InsertChannel: segmentBinlog.InsertChannel,
	}
	if setIndex {
		// if index not exist, load binlog to query node
		indexInfo, err := broker.getIndexInfo(ctx, collectionID, segmentID, schema)
		if err == nil {
			segmentLoadInfo.IndexInfos = indexInfo
		}
		log.Warn("querycoord debug generateSegmentLoadInfo", zap.Any("indexInfo", indexInfo))
	}

	// set the estimate segment size to segmentLoadInfo
	segmentLoadInfo.SegmentSize = estimateSegmentSize(segmentLoadInfo)

	return segmentLoadInfo
}

func (broker *globalMetaBroker) getSegmentStates(ctx context.Context, segmentID UniqueID) (*datapb.SegmentStateInfo, error) {
	ctx2, cancel2 := context.WithTimeout(ctx, timeoutForRPC)
	defer cancel2()

	req := &datapb.GetSegmentStatesRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_GetSegmentState,
		},
		SegmentIDs: []UniqueID{segmentID},
	}
	resp, err := broker.dataCoord.GetSegmentStates(ctx2, req)
	if err != nil {
		log.Error("get segment states failed from dataCoord,", zap.Int64("segmentID", segmentID), zap.Error(err))
		return nil, err
	}

	if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
		err = errors.New(resp.Status.Reason)
		log.Error("get segment states failed from dataCoord,", zap.Int64("segmentID", segmentID), zap.Error(err))
		return nil, err
	}

	if len(resp.States) != 1 {
		err = fmt.Errorf("the length of segmentStates result should be 1, segmentID = %d", segmentID)
		log.Error(err.Error())
		return nil, err
	}

	return resp.States[0], nil
}

func (broker *globalMetaBroker) acquireSegmentsReferLock(ctx context.Context, taskID int64, segmentIDs []UniqueID) error {
	ctx, cancel := context.WithTimeout(ctx, timeoutForRPC)
	defer cancel()
	acquireSegLockReq := &datapb.AcquireSegmentLockRequest{
		TaskID:     taskID,
		SegmentIDs: segmentIDs,
		NodeID:     Params.QueryCoordCfg.GetNodeID(),
	}
	status, err := broker.dataCoord.AcquireSegmentLock(ctx, acquireSegLockReq)
	if err != nil {
		log.Error("QueryCoord acquire the segment reference lock error", zap.Int64s("segIDs", segmentIDs),
			zap.Error(err))
		return err
	}
	if status.ErrorCode != commonpb.ErrorCode_Success {
		log.Error("QueryCoord acquire the segment reference lock error", zap.Int64s("segIDs", segmentIDs),
			zap.String("failed reason", status.Reason))
		return fmt.Errorf(status.Reason)
	}

	return nil
}

func (broker *globalMetaBroker) releaseSegmentReferLock(ctx context.Context, taskID int64, segmentIDs []UniqueID) error {
	ctx, cancel := context.WithTimeout(ctx, timeoutForRPC)
	defer cancel()

	releaseSegReferLockReq := &datapb.ReleaseSegmentLockRequest{
		TaskID:     taskID,
		NodeID:     Params.QueryCoordCfg.GetNodeID(),
		SegmentIDs: segmentIDs,
	}

	if err := retry.Do(ctx, func() error {
		status, err := broker.dataCoord.ReleaseSegmentLock(ctx, releaseSegReferLockReq)
		if err != nil {
			log.Error("QueryCoord release reference lock on segments failed", zap.Int64s("segmentIDs", segmentIDs),
				zap.Error(err))
			return err
		}

		if status.ErrorCode != commonpb.ErrorCode_Success {
			log.Error("QueryCoord release reference lock on segments failed", zap.Int64s("segmentIDs", segmentIDs),
				zap.String("failed reason", status.Reason))
			return errors.New(status.Reason)
		}
		return nil
	}, retry.Attempts(100)); err != nil {
		return err
	}

	return nil
}

// getDataSegmentInfosByIDs return the SegmentInfo details according to the given ids through RPC to datacoord
func (broker *globalMetaBroker) getDataSegmentInfosByIDs(segmentIds []int64) ([]*datapb.SegmentInfo, error) {
	var segmentInfos []*datapb.SegmentInfo
	infoResp, err := broker.dataCoord.GetSegmentInfo(broker.ctx, &datapb.GetSegmentInfoRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_SegmentInfo,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  Params.ProxyCfg.GetNodeID(),
		},
		SegmentIDs:       segmentIds,
		IncludeUnHealthy: true,
	})
	if err != nil {
		log.Error("Fail to get datapb.SegmentInfo by ids from datacoord", zap.Error(err))
		return nil, err
	}
	if infoResp.GetStatus().ErrorCode != commonpb.ErrorCode_Success {
		err = errors.New(infoResp.GetStatus().Reason)
		log.Error("Fail to get datapb.SegmentInfo by ids from datacoord", zap.Error(err))
		return nil, err
	}
	segmentInfos = infoResp.Infos
	return segmentInfos, nil
}
