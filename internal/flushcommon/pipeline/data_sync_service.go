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

package pipeline

import (
	"context"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datanode/compaction"
	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/flushcommon/util"
	"github.com/milvus-io/milvus/internal/flushcommon/writebuffer"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// DataSyncService controls a flowgraph for a specific collection
type DataSyncService struct {
	ctx          context.Context
	cancelFn     context.CancelFunc
	metacache    metacache.MetaCache
	opID         int64
	collectionID typeutil.UniqueID // collection id of vchan for which this data sync service serves
	vchannelName string

	// TODO: should be equal to paramtable.GetNodeID(), but intergrationtest has 1 paramtable for a minicluster, the NodeID
	// varies, will cause savebinglogpath check fail. So we pass ServerID into DataSyncService to aviod it failure.
	serverID typeutil.UniqueID

	fg *flowgraph.TimeTickedFlowGraph // internal flowgraph processes insert/delta messages

	broker  broker.Broker
	syncMgr syncmgr.SyncManager

	timetickSender util.StatsUpdater   // reference to TimeTickSender
	compactor      compaction.Executor // reference to compaction executor

	dispClient   msgdispatcher.Client
	chunkManager storage.ChunkManager

	stopOnce sync.Once
}

type nodeConfig struct {
	msFactory    msgstream.Factory // msgStream factory
	collectionID typeutil.UniqueID
	vChannelName string
	metacache    metacache.MetaCache
	serverID     typeutil.UniqueID
}

// Start the flow graph in dataSyncService
func (dsService *DataSyncService) Start() {
	if dsService.fg != nil {
		log.Info("dataSyncService starting flow graph", zap.Int64("collectionID", dsService.collectionID),
			zap.String("vChanName", dsService.vchannelName))
		dsService.fg.Start()
	} else {
		log.Warn("dataSyncService starting flow graph is nil", zap.Int64("collectionID", dsService.collectionID),
			zap.String("vChanName", dsService.vchannelName))
	}
}

func (dsService *DataSyncService) GracefullyClose() {
	if dsService.fg != nil {
		log.Info("dataSyncService gracefully closing flowgraph")
		dsService.fg.SetCloseMethod(flowgraph.CloseGracefully)
		dsService.close()
	}
}

func (dsService *DataSyncService) GetOpID() int64 {
	return dsService.opID
}

func (dsService *DataSyncService) close() {
	dsService.stopOnce.Do(func() {
		log := log.Ctx(dsService.ctx).With(
			zap.Int64("collectionID", dsService.collectionID),
			zap.String("vChanName", dsService.vchannelName),
		)
		if dsService.fg != nil {
			log.Info("dataSyncService closing flowgraph")
			if dsService.dispClient != nil {
				dsService.dispClient.Deregister(dsService.vchannelName)
			}
			dsService.fg.Close()
			log.Info("dataSyncService flowgraph closed")
		}

		dsService.cancelFn()

		// clean up metrics
		pChan := funcutil.ToPhysicalChannel(dsService.vchannelName)
		metrics.CleanupDataNodeCollectionMetrics(paramtable.GetNodeID(), dsService.collectionID, pChan)

		log.Info("dataSyncService closed")
	})
}

func (dsService *DataSyncService) GetMetaCache() metacache.MetaCache {
	return dsService.metacache
}

func getMetaCacheWithTickler(initCtx context.Context, params *util.PipelineParams, info *datapb.ChannelWatchInfo, tickler *util.Tickler, unflushed, flushed []*datapb.SegmentInfo) (metacache.MetaCache, error) {
	tickler.SetTotal(int32(len(unflushed) + len(flushed)))
	return initMetaCache(initCtx, params.ChunkManager, info, tickler, unflushed, flushed)
}

func initMetaCache(initCtx context.Context, chunkManager storage.ChunkManager, info *datapb.ChannelWatchInfo, tickler interface{ Inc() }, unflushed, flushed []*datapb.SegmentInfo) (metacache.MetaCache, error) {
	// tickler will update addSegment progress to watchInfo
	futures := make([]*conc.Future[any], 0, len(unflushed)+len(flushed))
	// segmentPks := typeutil.NewConcurrentMap[int64, []*storage.PkStatistics]()
	segmentPks := typeutil.NewConcurrentMap[int64, pkoracle.PkStat]()

	loadSegmentStats := func(segType string, segments []*datapb.SegmentInfo) {
		for _, item := range segments {
			log.Info("recover segments from checkpoints",
				zap.String("vChannelName", item.GetInsertChannel()),
				zap.Int64("segmentID", item.GetID()),
				zap.Int64("numRows", item.GetNumOfRows()),
				zap.String("segmentType", segType),
			)
			segment := item
			future := io.GetOrCreateStatsPool().Submit(func() (any, error) {
				var stats []*storage.PkStatistics
				var err error
				stats, err = compaction.LoadStats(initCtx, chunkManager, info.GetSchema(), segment.GetID(), segment.GetStatslogs())
				if err != nil {
					return nil, err
				}
				segmentPks.Insert(segment.GetID(), pkoracle.NewBloomFilterSet(stats...))
				if !streamingutil.IsStreamingServiceEnabled() {
					tickler.Inc()
				}

				return struct{}{}, nil
			})

			futures = append(futures, future)
		}
	}
	lazyLoadSegmentStats := func(segType string, segments []*datapb.SegmentInfo) {
		for _, item := range segments {
			log.Info("lazy load pk stats for segment",
				zap.String("vChannelName", item.GetInsertChannel()),
				zap.Int64("segmentID", item.GetID()),
				zap.Int64("numRows", item.GetNumOfRows()),
				zap.String("segmentType", segType),
			)
			segment := item

			lazy := pkoracle.NewLazyPkstats()

			// ignore lazy load future
			_ = io.GetOrCreateStatsPool().Submit(func() (any, error) {
				var stats []*storage.PkStatistics
				var err error
				stats, err = compaction.LoadStats(context.Background(), chunkManager, info.GetSchema(), segment.GetID(), segment.GetStatslogs())
				if err != nil {
					return nil, err
				}
				pkStats := pkoracle.NewBloomFilterSet(stats...)
				lazy.SetPkStats(pkStats)
				return struct{}{}, nil
			})
			segmentPks.Insert(segment.GetID(), lazy)
			if tickler != nil {
				tickler.Inc()
			}
		}
	}

	// growing segment cannot use lazy mode
	loadSegmentStats("growing", unflushed)
	lazy := paramtable.Get().DataNodeCfg.SkipBFStatsLoad.GetAsBool()
	// check paramtable to decide whether skip load BF stage when initializing
	if lazy {
		lazyLoadSegmentStats("sealed", flushed)
	} else {
		loadSegmentStats("sealed", flushed)
	}

	// use fetched segment info
	info.Vchan.FlushedSegments = flushed
	info.Vchan.UnflushedSegments = unflushed

	if err := conc.AwaitAll(futures...); err != nil {
		return nil, err
	}

	// return channel, nil
	metacache := metacache.NewMetaCache(info, func(segment *datapb.SegmentInfo) pkoracle.PkStat {
		pkStat, _ := segmentPks.Get(segment.GetID())
		return pkStat
	})

	return metacache, nil
}

func getServiceWithChannel(initCtx context.Context, params *util.PipelineParams,
	info *datapb.ChannelWatchInfo, metacache metacache.MetaCache,
	unflushed, flushed []*datapb.SegmentInfo, input <-chan *msgstream.MsgPack,
) (*DataSyncService, error) {
	var (
		channelName  = info.GetVchan().GetChannelName()
		collectionID = info.GetVchan().GetCollectionID()
	)
	serverID := paramtable.GetNodeID()
	if params.Session != nil {
		serverID = params.Session.ServerID
	}

	config := &nodeConfig{
		msFactory:    params.MsgStreamFactory,
		collectionID: collectionID,
		vChannelName: channelName,
		metacache:    metacache,
		serverID:     serverID,
	}

	err := params.WriteBufferManager.Register(channelName, metacache,
		writebuffer.WithMetaWriter(syncmgr.BrokerMetaWriter(params.Broker, config.serverID)),
		writebuffer.WithIDAllocator(params.Allocator))
	if err != nil {
		log.Warn("failed to register channel buffer", zap.Error(err))
		return nil, err
	}
	defer func() {
		if err != nil {
			defer params.WriteBufferManager.RemoveChannel(channelName)
		}
	}()

	ctx, cancel := context.WithCancel(params.Ctx)
	ds := &DataSyncService{
		ctx:      ctx,
		cancelFn: cancel,
		opID:     info.GetOpID(),

		dispClient: params.DispClient,
		broker:     params.Broker,

		metacache:    config.metacache,
		collectionID: config.collectionID,
		vchannelName: config.vChannelName,
		serverID:     config.serverID,

		chunkManager:   params.ChunkManager,
		compactor:      params.CompactionExecutor,
		timetickSender: params.TimeTickSender,
		syncMgr:        params.SyncMgr,

		fg: nil,
	}

	// init flowgraph
	fg := flowgraph.NewTimeTickedFlowGraph(params.Ctx)

	var dmStreamNode *flowgraph.InputNode
	dmStreamNode, err = newDmInputNode(initCtx, params.DispClient, info.GetVchan().GetSeekPosition(), config, input)
	if err != nil {
		return nil, err
	}

	var ddNode *ddNode
	ddNode, err = newDDNode(
		params.Ctx,
		collectionID,
		channelName,
		info.GetVchan().GetDroppedSegmentIds(),
		flushed,
		unflushed,
		params.CompactionExecutor,
		params.FlushMsgHandler,
	)
	if err != nil {
		return nil, err
	}

	writeNode := newWriteNode(params.Ctx, params.WriteBufferManager, ds.timetickSender, config)
	var ttNode *ttNode
	ttNode, err = newTTNode(config, params.WriteBufferManager, params.CheckpointUpdater)
	if err != nil {
		return nil, err
	}

	if err := fg.AssembleNodes(dmStreamNode, ddNode, writeNode, ttNode); err != nil {
		return nil, err
	}
	ds.fg = fg

	return ds, nil
}

// NewDataSyncService gets a dataSyncService, but flowgraphs are not running
// initCtx is used to init the dataSyncService only, if initCtx.Canceled or initCtx.Timeout
// NewDataSyncService stops and returns the initCtx.Err()
func NewDataSyncService(initCtx context.Context, pipelineParams *util.PipelineParams, info *datapb.ChannelWatchInfo, tickler *util.Tickler) (*DataSyncService, error) {
	// recover segment checkpoints
	unflushedSegmentInfos, err := pipelineParams.Broker.GetSegmentInfo(initCtx, info.GetVchan().GetUnflushedSegmentIds())
	if err != nil {
		return nil, err
	}
	flushedSegmentInfos, err := pipelineParams.Broker.GetSegmentInfo(initCtx, info.GetVchan().GetFlushedSegmentIds())
	if err != nil {
		return nil, err
	}

	// init metaCache meta
	metaCache, err := getMetaCacheWithTickler(initCtx, pipelineParams, info, tickler, unflushedSegmentInfos, flushedSegmentInfos)
	if err != nil {
		return nil, err
	}

	return getServiceWithChannel(initCtx, pipelineParams, info, metaCache, unflushedSegmentInfos, flushedSegmentInfos, nil)
}

func NewStreamingNodeDataSyncService(initCtx context.Context, pipelineParams *util.PipelineParams, info *datapb.ChannelWatchInfo, input <-chan *msgstream.MsgPack) (*DataSyncService, error) {
	// recover segment checkpoints
	var (
		err                   error
		unflushedSegmentInfos []*datapb.SegmentInfo
		flushedSegmentInfos   []*datapb.SegmentInfo
	)
	if len(info.GetVchan().GetUnflushedSegmentIds()) > 0 {
		unflushedSegmentInfos, err = pipelineParams.Broker.GetSegmentInfo(initCtx, info.GetVchan().GetUnflushedSegmentIds())
		if err != nil {
			return nil, err
		}
	}
	if len(info.GetVchan().GetFlushedSegmentIds()) > 0 {
		flushedSegmentInfos, err = pipelineParams.Broker.GetSegmentInfo(initCtx, info.GetVchan().GetFlushedSegmentIds())
		if err != nil {
			return nil, err
		}
	}

	// init metaCache meta
	metaCache, err := initMetaCache(initCtx, pipelineParams.ChunkManager, info, nil, unflushedSegmentInfos, flushedSegmentInfos)
	if err != nil {
		return nil, err
	}

	return getServiceWithChannel(initCtx, pipelineParams, info, metaCache, unflushedSegmentInfos, flushedSegmentInfos, input)
}

func NewDataSyncServiceWithMetaCache(metaCache metacache.MetaCache) *DataSyncService {
	return &DataSyncService{metacache: metaCache}
}
