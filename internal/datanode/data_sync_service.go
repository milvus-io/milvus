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
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/datanode/broker"
	"github.com/milvus-io/milvus/internal/datanode/compaction"
	"github.com/milvus-io/milvus/internal/datanode/io"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/internal/datanode/util"
	"github.com/milvus-io/milvus/internal/datanode/writebuffer"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// dataSyncService controls a flowgraph for a specific collection
type dataSyncService struct {
	ctx          context.Context
	cancelFn     context.CancelFunc
	metacache    metacache.MetaCache
	opID         int64
	collectionID UniqueID // collection id of vchan for which this data sync service serves
	vchannelName string

	// TODO: should be equal to paramtable.GetNodeID(), but intergrationtest has 1 paramtable for a minicluster, the NodeID
	// varies, will cause savebinglogpath check fail. So we pass ServerID into dataSyncService to aviod it failure.
	serverID UniqueID

	fg *flowgraph.TimeTickedFlowGraph // internal flowgraph processes insert/delta messages

	broker  broker.Broker
	syncMgr syncmgr.SyncManager

	flushCh          chan flushMsg
	resendTTCh       chan resendTTMsg    // chan to ask for resending DataNode time tick message.
	timetickSender   *timeTickSender     // reference to timeTickSender
	compactor        compaction.Executor // reference to compaction executor
	flushingSegCache *Cache              // a guarding cache stores currently flushing segment ids

	clearSignal  chan<- string       // signal channel to notify flowgraph close for collection/partition drop msg consumed
	idAllocator  allocator.Allocator // id/timestamp allocator
	msFactory    msgstream.Factory
	dispClient   msgdispatcher.Client
	chunkManager storage.ChunkManager

	stopOnce sync.Once
}

type nodeConfig struct {
	msFactory    msgstream.Factory // msgStream factory
	collectionID UniqueID
	vChannelName string
	metacache    metacache.MetaCache
	allocator    allocator.Allocator
	serverID     UniqueID
}

// start the flow graph in dataSyncService
func (dsService *dataSyncService) start() {
	if dsService.fg != nil {
		log.Info("dataSyncService starting flow graph", zap.Int64("collectionID", dsService.collectionID),
			zap.String("vChanName", dsService.vchannelName))
		dsService.fg.Start()
	} else {
		log.Warn("dataSyncService starting flow graph is nil", zap.Int64("collectionID", dsService.collectionID),
			zap.String("vChanName", dsService.vchannelName))
	}
}

func (dsService *dataSyncService) GracefullyClose() {
	if dsService.fg != nil {
		log.Info("dataSyncService gracefully closing flowgraph")
		dsService.fg.SetCloseMethod(flowgraph.CloseGracefully)
		dsService.close()
	}
}

func (dsService *dataSyncService) close() {
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

func getMetaCacheWithTickler(initCtx context.Context, node *DataNode, info *datapb.ChannelWatchInfo, tickler *tickler, unflushed, flushed []*datapb.SegmentInfo, storageV2Cache *metacache.StorageV2Cache) (metacache.MetaCache, error) {
	tickler.setTotal(int32(len(unflushed) + len(flushed)))
	return initMetaCache(initCtx, storageV2Cache, node.chunkManager, info, tickler, unflushed, flushed)
}

func getMetaCacheWithEtcdTickler(initCtx context.Context, node *DataNode, info *datapb.ChannelWatchInfo, tickler *etcdTickler, unflushed, flushed []*datapb.SegmentInfo, storageV2Cache *metacache.StorageV2Cache) (metacache.MetaCache, error) {
	tickler.watch()
	defer tickler.stop()

	return initMetaCache(initCtx, storageV2Cache, node.chunkManager, info, tickler, unflushed, flushed)
}

func initMetaCache(initCtx context.Context, storageV2Cache *metacache.StorageV2Cache, chunkManager storage.ChunkManager, info *datapb.ChannelWatchInfo, tickler interface{ inc() }, unflushed, flushed []*datapb.SegmentInfo) (metacache.MetaCache, error) {
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
					stats, err = util.LoadStatsV2(storageV2Cache, segment, info.GetSchema())
				} else {
					stats, err = util.LoadStats(initCtx, chunkManager, info.GetSchema(), segment.GetID(), segment.GetStatslogs())
				}
				if err != nil {
					return nil, err
				}
				segmentPks.Insert(segment.GetID(), stats)
				tickler.inc()

				return struct{}{}, nil
			})

			futures = append(futures, future)
		}
	}

	// growing segments's stats should always be loaded, for generating merged pk bf.
	loadSegmentStats("growing", unflushed)
	if !paramtable.Get().DataNodeCfg.SkipBFStatsLoad.GetAsBool() {
		loadSegmentStats("sealed", flushed)
	}

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

func getServiceWithChannel(initCtx context.Context, node *DataNode, info *datapb.ChannelWatchInfo, metacache metacache.MetaCache, storageV2Cache *metacache.StorageV2Cache, unflushed, flushed []*datapb.SegmentInfo) (*dataSyncService, error) {
	var (
		channelName  = info.GetVchan().GetChannelName()
		collectionID = info.GetVchan().GetCollectionID()
	)

	config := &nodeConfig{
		msFactory: node.factory,
		allocator: node.allocator,

		collectionID: collectionID,
		vChannelName: channelName,
		metacache:    metacache,
		serverID:     node.session.ServerID,
	}

	var (
		flushCh    = make(chan flushMsg, 100)
		resendTTCh = make(chan resendTTMsg, 100)
	)

	ctx, cancel := context.WithCancel(node.ctx)
	ds := &dataSyncService{
		ctx:        ctx,
		cancelFn:   cancel,
		flushCh:    flushCh,
		resendTTCh: resendTTCh,
		opID:       info.GetOpID(),

		dispClient: node.dispClient,
		msFactory:  node.factory,
		broker:     node.broker,

		idAllocator:  config.allocator,
		metacache:    config.metacache,
		collectionID: config.collectionID,
		vchannelName: config.vChannelName,
		serverID:     config.serverID,

		flushingSegCache: node.segmentCache,
		clearSignal:      node.clearSignal,
		chunkManager:     node.chunkManager,
		compactor:        node.compactionExecutor,
		timetickSender:   node.timeTickSender,
		syncMgr:          node.syncMgr,

		fg: nil,
	}

	// init flowgraph
	fg := flowgraph.NewTimeTickedFlowGraph(node.ctx)
	dmStreamNode, err := newDmInputNode(initCtx, node.dispClient, info.GetVchan().GetSeekPosition(), config)
	if err != nil {
		return nil, err
	}

	ddNode, err := newDDNode(
		node.ctx,
		collectionID,
		channelName,
		info.GetVchan().GetDroppedSegmentIds(),
		flushed,
		unflushed,
		node.compactionExecutor,
	)
	if err != nil {
		return nil, err
	}

	var updater statsUpdater
	if Params.DataNodeCfg.DataNodeTimeTickByRPC.GetAsBool() {
		updater = ds.timetickSender
	} else {
		m, err := config.msFactory.NewMsgStream(ctx)
		if err != nil {
			return nil, err
		}

		m.AsProducer([]string{Params.CommonCfg.DataCoordTimeTick.GetValue()})
		metrics.DataNodeNumProducers.WithLabelValues(fmt.Sprint(config.serverID)).Inc()
		log.Info("datanode AsProducer", zap.String("TimeTickChannelName", Params.CommonCfg.DataCoordTimeTick.GetValue()))

		m.ForceEnableProduce(true)

		updater = newMqStatsUpdater(config, m)
	}

	writeNode := newWriteNode(node.ctx, node.writeBufferManager, updater, config)

	ttNode, err := newTTNode(config, node.writeBufferManager, node.channelCheckpointUpdater)
	if err != nil {
		return nil, err
	}

	if err := fg.AssembleNodes(dmStreamNode, ddNode, writeNode, ttNode); err != nil {
		return nil, err
	}
	ds.fg = fg

	// Register channel after channel pipeline is ready.
	// This'll reject any FlushChannel and FlushSegments calls to prevent inconsistency between DN and DC over flushTs
	// if fail to init flowgraph nodes.
	err = node.writeBufferManager.Register(channelName, metacache, storageV2Cache, writebuffer.WithMetaWriter(syncmgr.BrokerMetaWriter(node.broker, config.serverID)), writebuffer.WithIDAllocator(node.allocator))
	if err != nil {
		log.Warn("failed to register channel buffer", zap.String("channel", channelName), zap.Error(err))
		return nil, err
	}

	return ds, nil
}

// newServiceWithEtcdTickler gets a dataSyncService, but flowgraphs are not running
// initCtx is used to init the dataSyncService only, if initCtx.Canceled or initCtx.Timeout
// newServiceWithEtcdTickler stops and returns the initCtx.Err()
func newServiceWithEtcdTickler(initCtx context.Context, node *DataNode, info *datapb.ChannelWatchInfo, tickler *etcdTickler) (*dataSyncService, error) {
	// recover segment checkpoints
	unflushedSegmentInfos, err := node.broker.GetSegmentInfo(initCtx, info.GetVchan().GetUnflushedSegmentIds())
	if err != nil {
		return nil, err
	}
	flushedSegmentInfos, err := node.broker.GetSegmentInfo(initCtx, info.GetVchan().GetFlushedSegmentIds())
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
	// init channel meta
	metaCache, err := getMetaCacheWithEtcdTickler(initCtx, node, info, tickler, unflushedSegmentInfos, flushedSegmentInfos, storageCache)
	if err != nil {
		return nil, err
	}

	return getServiceWithChannel(initCtx, node, info, metaCache, storageCache, unflushedSegmentInfos, flushedSegmentInfos)
}

// newDataSyncService gets a dataSyncService, but flowgraphs are not running
// initCtx is used to init the dataSyncService only, if initCtx.Canceled or initCtx.Timeout
// newDataSyncService stops and returns the initCtx.Err()
// NOTE: compactiable for event manager
func newDataSyncService(initCtx context.Context, node *DataNode, info *datapb.ChannelWatchInfo, tickler *tickler) (*dataSyncService, error) {
	// recover segment checkpoints
	var (
		err                   error
		unflushedSegmentInfos []*datapb.SegmentInfo
		flushedSegmentInfos   []*datapb.SegmentInfo
	)
	if len(info.GetVchan().GetUnflushedSegmentIds()) > 0 {
		unflushedSegmentInfos, err = node.broker.GetSegmentInfo(initCtx, info.GetVchan().GetUnflushedSegmentIds())
		if err != nil {
			return nil, err
		}
	}
	if len(info.GetVchan().GetFlushedSegmentIds()) > 0 {
		flushedSegmentInfos, err = node.broker.GetSegmentInfo(initCtx, info.GetVchan().GetFlushedSegmentIds())
		if err != nil {
			return nil, err
		}
	}

	var storageCache *metacache.StorageV2Cache
	if params.Params.CommonCfg.EnableStorageV2.GetAsBool() {
		storageCache, err = metacache.NewStorageV2Cache(info.Schema)
		if err != nil {
			return nil, err
		}
	}
	// init metaCache meta
	metaCache, err := getMetaCacheWithTickler(initCtx, node, info, tickler, unflushedSegmentInfos, flushedSegmentInfos, storageCache)
	if err != nil {
		return nil, err
	}

	return getServiceWithChannel(initCtx, node, info, metaCache, storageCache, unflushedSegmentInfos, flushedSegmentInfos)
}
