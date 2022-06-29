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

package datanode

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/concurrency"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
)

// dataSyncService controls a flowgraph for a specific collection
type dataSyncService struct {
	ctx          context.Context
	cancelFn     context.CancelFunc
	fg           *flowgraph.TimeTickedFlowGraph // internal flowgraph processes insert/delta messages
	flushCh      chan flushMsg                  // chan to notify flush
	resendTTCh   chan resendTTMsg               // chan to ask for resending DataNode time tick message.
	replica      Replica                        // segment replica stores meta
	idAllocator  allocatorInterface             // id/timestamp allocator
	msFactory    msgstream.Factory
	collectionID UniqueID // collection id of vchan for which this data sync service serves
	vchannelName string
	dataCoord    types.DataCoord // DataCoord instance to interact with
	clearSignal  chan<- string   // signal channel to notify flowgraph close for collection/partition drop msg consumed

	flushingSegCache *Cache       // a guarding cache stores currently flushing segment ids
	flushManager     flushManager // flush manager handles flush process
	chunkManager     storage.ChunkManager
	compactor        *compactionExecutor // reference to compaction executor

	// concurrent add segments, reduce time to load delta log from oss
	ioPool *concurrency.Pool
}

func newDataSyncService(ctx context.Context,
	flushCh chan flushMsg,
	resendTTCh chan resendTTMsg,
	replica Replica,
	alloc allocatorInterface,
	factory msgstream.Factory,
	vchan *datapb.VchannelInfo,
	clearSignal chan<- string,
	dataCoord types.DataCoord,
	flushingSegCache *Cache,
	chunkManager storage.ChunkManager,
	compactor *compactionExecutor,
) (*dataSyncService, error) {

	if replica == nil {
		return nil, errors.New("Nil input")
	}

	// Initialize io cocurrency pool
	log.Info("initialize io concurrency pool", zap.String("vchannel", vchan.GetChannelName()), zap.Int("ioConcurrency", Params.DataNodeCfg.IOConcurrency))
	ioPool, err := concurrency.NewPool(Params.DataNodeCfg.IOConcurrency)
	if err != nil {
		log.Error("failed to create goroutine pool for dataSyncService", zap.Error(err))
		return nil, err
	}

	ctx1, cancel := context.WithCancel(ctx)

	service := &dataSyncService{
		ctx:              ctx1,
		cancelFn:         cancel,
		fg:               nil,
		flushCh:          flushCh,
		resendTTCh:       resendTTCh,
		replica:          replica,
		idAllocator:      alloc,
		msFactory:        factory,
		collectionID:     vchan.GetCollectionID(),
		vchannelName:     vchan.GetChannelName(),
		dataCoord:        dataCoord,
		clearSignal:      clearSignal,
		flushingSegCache: flushingSegCache,
		chunkManager:     chunkManager,
		compactor:        compactor,
		ioPool:           ioPool,
	}

	if err := service.initNodes(vchan); err != nil {
		return nil, err
	}
	return service, nil
}

type parallelConfig struct {
	maxQueueLength int32
	maxParallelism int32
}

type nodeConfig struct {
	msFactory    msgstream.Factory // msgStream factory
	collectionID UniqueID
	vChannelName string
	replica      Replica // Segment replica
	allocator    allocatorInterface

	// defaults
	parallelConfig
}

func newParallelConfig() parallelConfig {
	return parallelConfig{Params.DataNodeCfg.FlowGraphMaxQueueLength, Params.DataNodeCfg.FlowGraphMaxParallelism}
}

// start starts the flowgraph in datasyncservice
func (dsService *dataSyncService) start() {
	if dsService.fg != nil {
		log.Info("dataSyncService starting flowgraph", zap.Int64("collectionID", dsService.collectionID),
			zap.String("vChanName", dsService.vchannelName))
		dsService.fg.Start()
	} else {
		log.Warn("dataSyncService starting flowgraph is nil", zap.Int64("collectionID", dsService.collectionID),
			zap.String("vChanName", dsService.vchannelName))
	}
}

func (dsService *dataSyncService) close() {
	if dsService.fg != nil {
		log.Info("dataSyncService closing flowgraph", zap.Int64("collectionID", dsService.collectionID),
			zap.String("vChanName", dsService.vchannelName))
		dsService.fg.Close()
		metrics.DataNodeNumConsumers.WithLabelValues(fmt.Sprint(Params.DataNodeCfg.GetNodeID())).Dec()
		metrics.DataNodeNumProducers.WithLabelValues(fmt.Sprint(Params.DataNodeCfg.GetNodeID())).Sub(2) // timeTickChannel + deltaChannel
	}

	dsService.clearGlobalFlushingCache()

	dsService.cancelFn()
	dsService.flushManager.close()
}

func (dsService *dataSyncService) clearGlobalFlushingCache() {
	segments := dsService.replica.listAllSegmentIDs()
	dsService.flushingSegCache.Remove(segments...)
}

// initNodes inits a TimetickedFlowGraph
func (dsService *dataSyncService) initNodes(vchanInfo *datapb.VchannelInfo) error {
	dsService.fg = flowgraph.NewTimeTickedFlowGraph(dsService.ctx)
	// initialize flush manager for DataSync Service
	dsService.flushManager = NewRendezvousFlushManager(dsService.idAllocator, dsService.chunkManager, dsService.replica,
		flushNotifyFunc(dsService), dropVirtualChannelFunc(dsService))

	var err error
	// recover segment checkpoints
	unflushedSegmentInfos, err := dsService.getSegmentInfos(vchanInfo.GetUnflushedSegmentIds())
	if err != nil {
		return err
	}
	flushedSegmentInfos, err := dsService.getSegmentInfos(vchanInfo.GetFlushedSegmentIds())
	if err != nil {
		return err
	}

	futures := make([]*concurrency.Future, 0, len(unflushedSegmentInfos)+len(flushedSegmentInfos))

	for _, us := range unflushedSegmentInfos {
		if us.CollectionID != dsService.collectionID ||
			us.GetInsertChannel() != vchanInfo.ChannelName {
			log.Warn("Collection ID or ChannelName not compact",
				zap.Int64("Wanted ID", dsService.collectionID),
				zap.Int64("Actual ID", us.CollectionID),
				zap.String("Wanted Channel Name", vchanInfo.ChannelName),
				zap.String("Actual Channel Name", us.GetInsertChannel()),
			)
			continue
		}

		log.Info("recover growing segments form checkpoints",
			zap.String("vChannelName", us.GetInsertChannel()),
			zap.Int64("segmentID", us.GetID()),
			zap.Int64("numRows", us.GetNumOfRows()),
		)
		var cp *segmentCheckPoint
		if us.GetDmlPosition() != nil {
			cp = &segmentCheckPoint{
				numRows: us.GetNumOfRows(),
				pos:     *us.GetDmlPosition(),
			}
		}
		// avoid closure capture iteration variable
		segment := us
		future := dsService.ioPool.Submit(func() (interface{}, error) {
			if err := dsService.replica.addSegment(addSegmentReq{
				segType:      datapb.SegmentType_Normal,
				segID:        segment.GetID(),
				collID:       segment.CollectionID,
				partitionID:  segment.PartitionID,
				channelName:  segment.GetInsertChannel(),
				numOfRows:    segment.GetNumOfRows(),
				statsBinLogs: segment.Statslogs,
				cp:           cp,
				recoverTs:    vchanInfo.GetSeekPosition().GetTimestamp()}); err != nil {
				return nil, err
			}
			return nil, nil
		})
		futures = append(futures, future)
	}

	for _, fs := range flushedSegmentInfos {
		if fs.CollectionID != dsService.collectionID ||
			fs.GetInsertChannel() != vchanInfo.ChannelName {
			log.Warn("Collection ID or ChannelName not compact",
				zap.Int64("Wanted ID", dsService.collectionID),
				zap.Int64("Actual ID", fs.CollectionID),
				zap.String("Wanted Channel Name", vchanInfo.ChannelName),
				zap.String("Actual Channel Name", fs.GetInsertChannel()),
			)
			continue
		}
		log.Info("recover sealed segments form checkpoints",
			zap.String("vChannelName", fs.GetInsertChannel()),
			zap.Int64("segmentID", fs.GetID()),
			zap.Int64("numRows", fs.GetNumOfRows()),
		)
		// avoid closure capture iteration variable
		segment := fs
		future := dsService.ioPool.Submit(func() (interface{}, error) {
			if err := dsService.replica.addSegment(addSegmentReq{
				segType:      datapb.SegmentType_Flushed,
				segID:        segment.GetID(),
				collID:       segment.CollectionID,
				partitionID:  segment.PartitionID,
				channelName:  segment.GetInsertChannel(),
				numOfRows:    segment.GetNumOfRows(),
				statsBinLogs: segment.Statslogs,
				recoverTs:    vchanInfo.GetSeekPosition().GetTimestamp(),
			}); err != nil {
				return nil, err
			}
			return nil, nil
		})
		futures = append(futures, future)
	}

	err = concurrency.AwaitAll(futures...)
	if err != nil {
		return err
	}

	c := &nodeConfig{
		msFactory:    dsService.msFactory,
		collectionID: vchanInfo.GetCollectionID(),
		vChannelName: vchanInfo.GetChannelName(),
		replica:      dsService.replica,
		allocator:    dsService.idAllocator,

		parallelConfig: newParallelConfig(),
	}

	var dmStreamNode Node
	dmStreamNode, err = newDmInputNode(dsService.ctx, vchanInfo.GetSeekPosition(), c)
	if err != nil {
		return err
	}

	var ddNode Node
	ddNode, err = newDDNode(
		dsService.ctx,
		dsService.collectionID,
		vchanInfo.GetChannelName(),
		vchanInfo.GetDroppedSegmentIds(),
		flushedSegmentInfos,
		unflushedSegmentInfos,
		dsService.msFactory,
		dsService.compactor)
	if err != nil {
		return err
	}

	var insertBufferNode Node
	insertBufferNode, err = newInsertBufferNode(
		dsService.ctx,
		dsService.collectionID,
		dsService.flushCh,
		dsService.resendTTCh,
		dsService.flushManager,
		dsService.flushingSegCache,
		c,
	)
	if err != nil {
		return err
	}

	var deleteNode Node
	deleteNode, err = newDeleteNode(dsService.ctx, dsService.flushManager, dsService.clearSignal, c)
	if err != nil {
		return err
	}

	dsService.fg.AddNode(dmStreamNode)
	dsService.fg.AddNode(ddNode)
	dsService.fg.AddNode(insertBufferNode)
	dsService.fg.AddNode(deleteNode)

	// ddStreamNode
	err = dsService.fg.SetEdges(dmStreamNode.Name(),
		[]string{},
		[]string{ddNode.Name()},
	)
	if err != nil {
		log.Error("set edges failed in node", zap.String("name", dmStreamNode.Name()), zap.Error(err))
		return err
	}

	// ddNode
	err = dsService.fg.SetEdges(ddNode.Name(),
		[]string{dmStreamNode.Name()},
		[]string{insertBufferNode.Name()},
	)
	if err != nil {
		log.Error("set edges failed in node", zap.String("name", ddNode.Name()), zap.Error(err))
		return err
	}

	// insertBufferNode
	err = dsService.fg.SetEdges(insertBufferNode.Name(),
		[]string{ddNode.Name()},
		[]string{deleteNode.Name()},
	)
	if err != nil {
		log.Error("set edges failed in node", zap.String("name", insertBufferNode.Name()), zap.Error(err))
		return err
	}

	//deleteNode
	err = dsService.fg.SetEdges(deleteNode.Name(),
		[]string{insertBufferNode.Name()},
		[]string{},
	)
	if err != nil {
		log.Error("set edges failed in node", zap.String("name", deleteNode.Name()), zap.Error(err))
		return err
	}
	return nil
}

// getSegmentInfos return the SegmentInfo details according to the given ids through RPC to datacoord
func (dsService *dataSyncService) getSegmentInfos(segmentIDs []int64) ([]*datapb.SegmentInfo, error) {
	var segmentInfos []*datapb.SegmentInfo
	infoResp, err := dsService.dataCoord.GetSegmentInfo(dsService.ctx, &datapb.GetSegmentInfoRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_SegmentInfo,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  Params.ProxyCfg.GetNodeID(),
		},
		SegmentIDs:       segmentIDs,
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
