// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package meta

import (
	"context"
	"math"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
	. "github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type Broker interface {
	DescribeCollection(ctx context.Context, collectionID UniqueID) (*milvuspb.DescribeCollectionResponse, error)
	GetPartitions(ctx context.Context, collectionID UniqueID) ([]UniqueID, error)
	GetRecoveryInfo(ctx context.Context, collectionID UniqueID, partitionID UniqueID) ([]*datapb.VchannelInfo, []*datapb.SegmentBinlogs, error)
	ListIndexes(ctx context.Context, collectionID UniqueID) ([]*indexpb.IndexInfo, error)
	GetSegmentInfo(ctx context.Context, segmentID ...UniqueID) ([]*datapb.SegmentInfo, error)
	GetIndexInfo(ctx context.Context, collectionID UniqueID, segmentIDs ...UniqueID) (map[int64][]*querypb.FieldIndexInfo, error)
	GetRecoveryInfoV2(ctx context.Context, collectionID UniqueID, partitionIDs ...UniqueID) ([]*datapb.VchannelInfo, []*datapb.SegmentInfo, error)
	DescribeDatabase(ctx context.Context, dbName string) (*rootcoordpb.DescribeDatabaseResponse, error)
	GetCollectionLoadInfo(ctx context.Context, collectionID UniqueID) ([]string, int64, error)
}

type CoordinatorBroker struct {
	mixCoord types.MixCoord
}

func NewCoordinatorBroker(
	mixCoord types.MixCoord,
) *CoordinatorBroker {
	return &CoordinatorBroker{
		mixCoord,
	}
}

func (broker *CoordinatorBroker) DescribeCollection(ctx context.Context, collectionID UniqueID) (*milvuspb.DescribeCollectionResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()

	req := &milvuspb.DescribeCollectionRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_DescribeCollection),
		),
		// please do not specify the collection name alone after database feature.
		CollectionID: collectionID,
	}
	resp, err := broker.mixCoord.DescribeCollection(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Ctx(ctx).Warn("failed to get collection schema", zap.Error(err))
		return nil, err
	}
	return resp, nil
}

func (broker *CoordinatorBroker) DescribeDatabase(ctx context.Context, dbName string) (*rootcoordpb.DescribeDatabaseResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()

	req := &rootcoordpb.DescribeDatabaseRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_DescribeCollection),
		),
		DbName: dbName,
	}
	resp, err := broker.mixCoord.DescribeDatabase(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Ctx(ctx).Warn("failed to describe database", zap.Error(err))
		return nil, err
	}
	return resp, nil
}

// try to get database level replica_num and resource groups, return (resource_groups, replica_num, error)
func (broker *CoordinatorBroker) GetCollectionLoadInfo(ctx context.Context, collectionID UniqueID) ([]string, int64, error) {
	collectionInfo, err := broker.DescribeCollection(ctx, collectionID)
	if err != nil {
		return nil, 0, err
	}

	log := log.Ctx(ctx)
	replicaNum, err := common.CollectionLevelReplicaNumber(collectionInfo.GetProperties())
	if err != nil {
		log.Debug("failed to get collection level load info", zap.Int64("collectionID", collectionID), zap.Error(err))
	} else if replicaNum > 0 {
		log.Info("get collection level load info", zap.Int64("collectionID", collectionID), zap.Int64("replica_num", replicaNum))
	}

	rgs, err := common.CollectionLevelResourceGroups(collectionInfo.GetProperties())
	if err != nil {
		log.Debug("failed to get collection level load info", zap.Int64("collectionID", collectionID), zap.Error(err))
	} else if len(rgs) > 0 {
		log.Info("get collection level load info", zap.Int64("collectionID", collectionID), zap.Strings("resource_groups", rgs))
	}

	if replicaNum <= 0 || len(rgs) == 0 {
		dbInfo, err := broker.DescribeDatabase(ctx, collectionInfo.GetDbName())
		if err != nil {
			return nil, 0, err
		}

		if replicaNum <= 0 {
			replicaNum, err = common.DatabaseLevelReplicaNumber(dbInfo.GetProperties())
			if err != nil {
				log.Debug("failed to get database level load info", zap.Int64("collectionID", collectionID), zap.Error(err))
			} else if replicaNum > 0 {
				log.Info("get database level load info", zap.Int64("collectionID", collectionID), zap.Int64("replica_num", replicaNum))
			}
		}

		if len(rgs) == 0 {
			rgs, err = common.DatabaseLevelResourceGroups(dbInfo.GetProperties())
			if err != nil {
				log.Debug("failed to get database level load info", zap.Int64("collectionID", collectionID), zap.Error(err))
			} else if len(rgs) > 0 {
				log.Info("get database level load info", zap.Int64("collectionID", collectionID), zap.Strings("resource_groups", rgs))
			}
		}
	}

	if replicaNum <= 0 || len(rgs) == 0 {
		if replicaNum <= 0 {
			replicaNum = paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.GetAsInt64()
			if replicaNum > 0 {
				log.Info("get cluster level load info", zap.Int64("collectionID", collectionID), zap.Int64("replica_num", replicaNum))
			}
		}

		if len(rgs) == 0 {
			rgs = paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.GetAsStrings()
			if len(rgs) > 0 {
				log.Info("get cluster level load info", zap.Int64("collectionID", collectionID), zap.Strings("resource_groups", rgs))
			}
		}
	}

	return rgs, replicaNum, nil
}

func (broker *CoordinatorBroker) GetPartitions(ctx context.Context, collectionID UniqueID) ([]UniqueID, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	log := log.Ctx(ctx).With(zap.Int64("collectionID", collectionID))
	req := &milvuspb.ShowPartitionsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ShowPartitions),
		),
		// please do not specify the collection name alone after database feature.
		CollectionID: collectionID,
	}
	resp, err := broker.mixCoord.ShowPartitions(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn("failed to get partitions", zap.Error(err))
		return nil, err
	}

	return resp.PartitionIDs, nil
}

func (broker *CoordinatorBroker) GetRecoveryInfo(ctx context.Context, collectionID UniqueID, partitionID UniqueID) ([]*datapb.VchannelInfo, []*datapb.SegmentBinlogs, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", collectionID),
		zap.Int64("partitionID", partitionID),
	)

	getRecoveryInfoRequest := &datapb.GetRecoveryInfoRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_GetRecoveryInfo),
		),
		CollectionID: collectionID,
		PartitionID:  partitionID,
	}
	recoveryInfo, err := broker.mixCoord.GetRecoveryInfo(ctx, getRecoveryInfoRequest)
	if err := merr.CheckRPCCall(recoveryInfo, err); err != nil {
		log.Warn("get recovery info failed", zap.Error(err))
		return nil, nil, err
	}

	// fallback binlog memory size to log size when it is zero
	fallbackBinlogMemorySize := func(binlogs []*datapb.FieldBinlog) {
		for _, insertBinlogs := range binlogs {
			for _, b := range insertBinlogs.GetBinlogs() {
				if b.GetMemorySize() == 0 {
					b.MemorySize = b.GetLogSize()
				}
			}
		}
	}
	for _, segBinlogs := range recoveryInfo.GetBinlogs() {
		fallbackBinlogMemorySize(segBinlogs.GetFieldBinlogs())
		fallbackBinlogMemorySize(segBinlogs.GetStatslogs())
		fallbackBinlogMemorySize(segBinlogs.GetDeltalogs())
	}

	return recoveryInfo.Channels, recoveryInfo.Binlogs, nil
}

func (broker *CoordinatorBroker) GetRecoveryInfoV2(ctx context.Context, collectionID UniqueID, partitionIDs ...UniqueID) ([]*datapb.VchannelInfo, []*datapb.SegmentInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", collectionID),
		zap.Int64s("partitionIDis", partitionIDs),
	)

	getRecoveryInfoRequest := &datapb.GetRecoveryInfoRequestV2{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_GetRecoveryInfo),
		),
		CollectionID: collectionID,
		PartitionIDs: partitionIDs,
	}
	recoveryInfo, err := broker.mixCoord.GetRecoveryInfoV2(ctx, getRecoveryInfoRequest)

	if err := merr.CheckRPCCall(recoveryInfo, err); err != nil {
		log.Warn("get recovery info failed", zap.Error(err))
		return nil, nil, err
	}

	return recoveryInfo.Channels, recoveryInfo.Segments, nil
}

func (broker *CoordinatorBroker) GetSegmentInfo(ctx context.Context, ids ...UniqueID) ([]*datapb.SegmentInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()

	log := log.Ctx(ctx).With(
		zap.Int64s("segments", ids),
	)

	getSegmentInfo := func(ids []UniqueID) (*datapb.GetSegmentInfoResponse, error) {
		req := &datapb.GetSegmentInfoRequest{
			SegmentIDs:       ids,
			IncludeUnHealthy: true,
		}
		resp, err := broker.mixCoord.GetSegmentInfo(ctx, req)
		if err := merr.CheckRPCCall(resp, err); err != nil {
			log.Warn("failed to get segment info from DataCoord", zap.Error(err))
			return nil, err
		}

		if len(resp.Infos) == 0 {
			log.Warn("No such segment in DataCoord")
			return nil, errors.New("no such segment in DataCoord")
		}

		err = binlog.DecompressMultiBinLogs(resp.GetInfos())
		if err != nil {
			log.Warn("failed to DecompressMultiBinLogs", zap.Error(err))
			return nil, err
		}

		return resp, nil
	}

	ret := make([]*datapb.SegmentInfo, 0, len(ids))
	batchSize := 1000
	startIdx := 0
	for startIdx < len(ids) {
		endIdx := int(math.Min(float64(startIdx+batchSize), float64(len(ids))))

		resp, err := getSegmentInfo(ids[startIdx:endIdx])
		if err != nil {
			return nil, err
		}
		ret = append(ret, resp.GetInfos()...)
		startIdx += batchSize
	}

	return ret, nil
}

func (broker *CoordinatorBroker) GetIndexInfo(ctx context.Context, collectionID UniqueID, segmentIDs ...UniqueID) (map[int64][]*querypb.FieldIndexInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()

	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", collectionID),
		zap.Int64s("segmentIDs", segmentIDs),
	)

	// during rolling upgrade, query coord may connect to datacoord with version 2.2, which will return merr.ErrServiceUnimplemented
	// we add retry here to retry the request until context done, and if new data coord start up, it will success
	var resp *indexpb.GetIndexInfoResponse
	var err error
	retry.Do(ctx, func() error {
		resp, err = broker.mixCoord.GetIndexInfos(ctx, &indexpb.GetIndexInfoRequest{
			CollectionID: collectionID,
			SegmentIDs:   segmentIDs,
		})

		if errors.Is(err, merr.ErrServiceUnimplemented) {
			return err
		}
		return nil
	})

	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Warn("failed to get segment index info", zap.Error(err))
		return nil, err
	}

	if resp.GetSegmentInfo() == nil {
		err = merr.WrapErrIndexNotFoundForSegments(segmentIDs)
		log.Warn("failed to get segments index info",
			zap.Error(err))
		return nil, err
	}

	indexes := make(map[int64][]*querypb.FieldIndexInfo, 0)
	for segmentID, segmentInfo := range resp.GetSegmentInfo() {
		indexes[segmentID] = make([]*querypb.FieldIndexInfo, 0)
		for _, info := range segmentInfo.GetIndexInfos() {
			indexes[segmentID] = append(indexes[segmentID], &querypb.FieldIndexInfo{
				FieldID:             info.GetFieldID(),
				EnableIndex:         true, // deprecated, but keep it for compatibility
				IndexName:           info.GetIndexName(),
				IndexID:             info.GetIndexID(),
				BuildID:             info.GetBuildID(),
				IndexParams:         info.GetIndexParams(),
				IndexFilePaths:      info.GetIndexFilePaths(),
				IndexSize:           int64(info.GetMemSize()),
				IndexVersion:        info.GetIndexVersion(),
				NumRows:             info.GetNumRows(),
				CurrentIndexVersion: info.GetCurrentIndexVersion(),
			})
		}
	}

	return indexes, nil
}

func (broker *CoordinatorBroker) describeIndex(ctx context.Context, collectionID UniqueID) ([]*indexpb.IndexInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()

	// during rolling upgrade, query coord may connect to datacoord with version 2.2, which will return merr.ErrServiceUnimplemented
	// we add retry here to retry the request until context done, and if new data coord start up, it will success
	var resp *indexpb.DescribeIndexResponse
	var err error
	retry.Do(ctx, func() error {
		resp, err = broker.mixCoord.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{
			CollectionID: collectionID,
		})
		if errors.Is(err, merr.ErrServiceUnimplemented) {
			return err
		}
		return nil
	})

	if err := merr.CheckRPCCall(resp, err); err != nil {
		log.Error("failed to fetch index meta",
			zap.Int64("collection", collectionID),
			zap.Error(err))
		return nil, err
	}
	return resp.GetIndexInfos(), nil
}

func (broker *CoordinatorBroker) ListIndexes(ctx context.Context, collectionID UniqueID) ([]*indexpb.IndexInfo, error) {
	log := log.Ctx(ctx).With(zap.Int64("collectionID", collectionID))
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Millisecond))
	defer cancel()

	resp, err := broker.mixCoord.ListIndexes(ctx, &indexpb.ListIndexesRequest{
		CollectionID: collectionID,
	})

	err = merr.CheckRPCCall(resp, err)
	if err != nil {
		if errors.Is(err, merr.ErrServiceUnimplemented) {
			log.Warn("datacoord does not implement ListIndex API fallback to DescribeIndex")
			return broker.describeIndex(ctx, collectionID)
		}
		log.Warn("failed to fetch index meta", zap.Error(err))
		return nil, err
	}

	return resp.GetIndexInfos(), nil
}
