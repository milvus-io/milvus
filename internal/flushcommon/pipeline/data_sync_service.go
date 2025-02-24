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
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
	dropCallback func()
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

func getMetaCacheForStreaming(initCtx context.Context, params *util.PipelineParams, info *datapb.ChannelWatchInfo, unflushed, flushed []*datapb.SegmentInfo) (metacache.MetaCache, error) {
	return initMetaCache(initCtx, params.ChunkManager, info, nil, unflushed, flushed)
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
	segmentBm25 := typeutil.NewConcurrentMap[int64, map[int64]*storage.BM25Stats]()

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
				if tickler != nil {
					tickler.Inc()
				}

				if segType == "growing" && len(segment.GetBm25Statslogs()) > 0 {
					bm25stats, err := compaction.LoadBM25Stats(initCtx, chunkManager, info.GetSchema(), segment.GetID(), segment.GetBm25Statslogs())
					if err != nil {
						return nil, err
					}
					segmentBm25.Insert(segment.GetID(), bm25stats)
				}

				return struct{}{}, nil
			})

			futures = append(futures, future)
		}
	}

	// growing segments's stats should always be loaded, for generating merged pk bf.
	loadSegmentStats("growing", unflushed)
	if !(streamingutil.IsStreamingServiceEnabled() || paramtable.Get().DataNodeCfg.SkipBFStatsLoad.GetAsBool()) {
		loadSegmentStats("sealed", flushed)
	}

	// use fetched segment info
	info.Vchan.FlushedSegments = flushed
	info.Vchan.UnflushedSegments = unflushed

	if err := conc.AwaitAll(futures...); err != nil {
		return nil, err
	}

	// return channel, nil
	pkStatsFactory := func(segment *datapb.SegmentInfo) pkoracle.PkStat {
		pkStat, _ := segmentPks.Get(segment.GetID())
		return pkStat
	}

	bm25StatsFactor := func(segment *datapb.SegmentInfo) *metacache.SegmentBM25Stats {
		stats, ok := segmentBm25.Get(segment.GetID())
		if !ok {
			return nil
		}
		segmentStats := metacache.NewSegmentBM25Stats(stats)
		return segmentStats
	}
	// return channel, nil
	metacache := metacache.NewMetaCache(info, pkStatsFactory, bm25StatsFactor)

	return metacache, nil
}

func getServiceWithChannel(initCtx context.Context, params *util.PipelineParams,
	info *datapb.ChannelWatchInfo, metacache metacache.MetaCache,
	unflushed, flushed []*datapb.SegmentInfo, input <-chan *msgstream.MsgPack,
	wbTaskObserverCallback writebuffer.TaskObserverCallback,
	dropCallback func(),
) (dss *DataSyncService, err error) {
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
		dropCallback: dropCallback,
	}

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
	nodeList := []flowgraph.Node{}

	dmStreamNode := newDmInputNode(config, input)
	nodeList = append(nodeList, dmStreamNode)

	ddNode := newDDNode(
		params.Ctx,
		collectionID,
		channelName,
		info.GetVchan().GetDroppedSegmentIds(),
		flushed,
		unflushed,
		params.CompactionExecutor,
		params.MsgHandler,
	)
	nodeList = append(nodeList, ddNode)

	if len(info.GetSchema().GetFunctions()) > 0 {
		emNode, err := newEmbeddingNode(channelName, info.GetSchema())
		if err != nil {
			return nil, err
		}
		nodeList = append(nodeList, emNode)
	}

	writeNode, err := newWriteNode(params.Ctx, params.WriteBufferManager, ds.timetickSender, config)
	if err != nil {
		return nil, err
	}
	nodeList = append(nodeList, writeNode)

	ttNode := newTTNode(config, params.WriteBufferManager, params.CheckpointUpdater)
	nodeList = append(nodeList, ttNode)

	if err := fg.AssembleNodes(nodeList...); err != nil {
		return nil, err
	}
	ds.fg = fg

	// Register channel after channel pipeline is ready.
	// This'll reject any FlushChannel and FlushSegments calls to prevent inconsistency between DN and DC over flushTs
	// if fail to init flowgraph nodes.
	err = params.WriteBufferManager.Register(channelName, metacache,
		writebuffer.WithMetaWriter(syncmgr.BrokerMetaWriter(params.Broker, config.serverID)),
		writebuffer.WithIDAllocator(params.Allocator),
		writebuffer.WithTaskObserverCallback(wbTaskObserverCallback))
	if err != nil {
		log.Warn("failed to register channel buffer", zap.String("channel", channelName), zap.Error(err))
		return nil, err
	}

	return ds, nil
}

// NewDataSyncService gets a dataSyncService, but flowgraphs are not running
// initCtx is used to init the dataSyncService only, if initCtx.Canceled or initCtx.Timeout
// NewDataSyncService stops and returns the initCtx.Err()
func NewDataSyncService(initCtx context.Context, pipelineParams *util.PipelineParams, info *datapb.ChannelWatchInfo, tickler *util.Tickler) (*DataSyncService, error) {
	// recover segment checkpoints
	var (
		err                   error
		metaCache             metacache.MetaCache
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
	if metaCache, err = getMetaCacheWithTickler(initCtx, pipelineParams, info, tickler, unflushedSegmentInfos, flushedSegmentInfos); err != nil {
		return nil, err
	}

	input, err := createNewInputFromDispatcher(initCtx,
		pipelineParams.DispClient,
		info.GetVchan().GetChannelName(),
		info.GetVchan().GetSeekPosition(),
		info.GetSchema(),
		info.GetDbProperties(),
	)
	if err != nil {
		return nil, err
	}
	ds, err := getServiceWithChannel(initCtx, pipelineParams, info, metaCache, unflushedSegmentInfos, flushedSegmentInfos, input, nil, nil)
	if err != nil {
		// deregister channel if failed to init flowgraph to avoid resource leak.
		pipelineParams.DispClient.Deregister(info.GetVchan().GetChannelName())
		return nil, err
	}
	return ds, nil
}

func NewStreamingNodeDataSyncService(
	initCtx context.Context,
	pipelineParams *util.PipelineParams,
	info *datapb.ChannelWatchInfo,
	input <-chan *msgstream.MsgPack,
	wbTaskObserverCallback writebuffer.TaskObserverCallback,
	dropCallback func(),
) (*DataSyncService, error) {
	// recover segment checkpoints
	var (
		err                   error
		metaCache             metacache.MetaCache
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
	if metaCache, err = getMetaCacheForStreaming(initCtx, pipelineParams, info, unflushedSegmentInfos, flushedSegmentInfos); err != nil {
		return nil, err
	}
	return getServiceWithChannel(initCtx, pipelineParams, info, metaCache, unflushedSegmentInfos, flushedSegmentInfos, input, wbTaskObserverCallback, dropCallback)
}

func NewDataSyncServiceWithMetaCache(metaCache metacache.MetaCache) *DataSyncService {
	return &DataSyncService{metacache: metaCache}
}
