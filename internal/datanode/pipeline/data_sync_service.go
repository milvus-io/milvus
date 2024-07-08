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

	"github.com/milvus-io/milvus/internal/datanode/broker"
	"github.com/milvus-io/milvus/internal/datanode/compaction"
	"github.com/milvus-io/milvus/internal/datanode/io"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/internal/datanode/util"
	"github.com/milvus-io/milvus/internal/datanode/writebuffer"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
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
	collectionID util.UniqueID // collection id of vchan for which this data sync service serves
	vchannelName string

	// TODO: should be equal to paramtable.GetNodeID(), but intergrationtest has 1 paramtable for a minicluster, the NodeID
	// varies, will cause savebinglogpath check fail. So we pass ServerID into DataSyncService to aviod it failure.
	serverID util.UniqueID

	fg *flowgraph.TimeTickedFlowGraph // internal flowgraph processes insert/delta messages

	broker  broker.Broker
	syncMgr syncmgr.SyncManager

	timetickSender *util.TimeTickSender // reference to TimeTickSender
	compactor      compaction.Executor  // reference to compaction executor

	dispClient   msgdispatcher.Client
	chunkManager storage.ChunkManager

	stopOnce sync.Once
}

type nodeConfig struct {
	msFactory    msgstream.Factory // msgStream factory
	collectionID util.UniqueID
	vChannelName string
	metacache    metacache.MetaCache
	serverID     util.UniqueID
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
			dsService.dispClient.Deregister(dsService.vchannelName)
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

func getMetaCacheWithTickler(initCtx context.Context, params *util.PipelineParams, info *datapb.ChannelWatchInfo, tickler *util.Tickler, unflushed, flushed []*datapb.SegmentInfo, storageV2Cache *metacache.StorageV2Cache) (metacache.MetaCache, error) {
	tickler.SetTotal(int32(len(unflushed) + len(flushed)))
	return initMetaCache(initCtx, storageV2Cache, params.ChunkManager, info, tickler, unflushed, flushed)
}

func initMetaCache(initCtx context.Context, storageV2Cache *metacache.StorageV2Cache, chunkManager storage.ChunkManager, info *datapb.ChannelWatchInfo, tickler interface{ Inc() }, unflushed, flushed []*datapb.SegmentInfo) (metacache.MetaCache, error) {
	// tickler will update addSegment progress to watchInfo
	futures := make([]*conc.Future[any], 0, len(unflushed)+len(flushed))
	segmentPks := typeutil.NewConcurrentMap[int64, []*storage.PkStatistics]()

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
				if params.Params.CommonCfg.EnableStorageV2.GetAsBool() {
					stats, err = compaction.LoadStatsV2(storageV2Cache, segment, info.GetSchema())
				} else {
					stats, err = compaction.LoadStats(initCtx, chunkManager, info.GetSchema(), segment.GetID(), segment.GetStatslogs())
				}
				if err != nil {
					return nil, err
				}
				segmentPks.Insert(segment.GetID(), stats)
				tickler.Inc()

				return struct{}{}, nil
			})

			futures = append(futures, future)
		}
	}

	loadSegmentStats("growing", unflushed)
	loadSegmentStats("sealed", flushed)

	// use fetched segment info
	info.Vchan.FlushedSegments = flushed
	info.Vchan.UnflushedSegments = unflushed

	if err := conc.AwaitAll(futures...); err != nil {
		return nil, err
	}

	// return channel, nil
	metacache := metacache.NewMetaCache(info, func(segment *datapb.SegmentInfo) *metacache.BloomFilterSet {
		entries, _ := segmentPks.Get(segment.GetID())
		return metacache.NewBloomFilterSet(entries...)
	})

	return metacache, nil
}

func getServiceWithChannel(initCtx context.Context, params *util.PipelineParams, info *datapb.ChannelWatchInfo, metacache metacache.MetaCache, storageV2Cache *metacache.StorageV2Cache, unflushed, flushed []*datapb.SegmentInfo) (*DataSyncService, error) {
	var (
		channelName  = info.GetVchan().GetChannelName()
		collectionID = info.GetVchan().GetCollectionID()
	)

	config := &nodeConfig{
		msFactory:    params.MsgStreamFactory,
		collectionID: collectionID,
		vChannelName: channelName,
		metacache:    metacache,
		serverID:     params.Session.ServerID,
	}

	err := params.WriteBufferManager.Register(channelName, metacache, storageV2Cache,
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
	dmStreamNode, err := newDmInputNode(initCtx, params.DispClient, info.GetVchan().GetSeekPosition(), config)
	if err != nil {
		return nil, err
	}

	ddNode, err := newDDNode(
		params.Ctx,
		collectionID,
		channelName,
		info.GetVchan().GetDroppedSegmentIds(),
		flushed,
		unflushed,
		params.CompactionExecutor,
	)
	if err != nil {
		return nil, err
	}

	writeNode := newWriteNode(params.Ctx, params.WriteBufferManager, ds.timetickSender, config)
	ttNode, err := newTTNode(config, params.WriteBufferManager, params.CheckpointUpdater)
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

	var storageCache *metacache.StorageV2Cache
	if params.Params.CommonCfg.EnableStorageV2.GetAsBool() {
		storageCache, err = metacache.NewStorageV2Cache(info.Schema)
		if err != nil {
			return nil, err
		}
	}

	// init metaCache meta
	metaCache, err := getMetaCacheWithTickler(initCtx, pipelineParams, info, tickler, unflushedSegmentInfos, flushedSegmentInfos, storageCache)
	if err != nil {
		return nil, err
	}

	return getServiceWithChannel(initCtx, pipelineParams, info, metaCache, storageCache, unflushedSegmentInfos, flushedSegmentInfos)
}

func NewDataSyncServiceWithMetaCache(metaCache metacache.MetaCache) *DataSyncService {
	return &DataSyncService{metacache: metaCache}
}
