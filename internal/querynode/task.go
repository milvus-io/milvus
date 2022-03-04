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

package querynode

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime/debug"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	queryPb "github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

type task interface {
	ID() UniqueID       // return ReqID
	SetID(uid UniqueID) // set ReqID
	Timestamp() Timestamp
	PreExecute(ctx context.Context) error
	Execute(ctx context.Context) error
	PostExecute(ctx context.Context) error
	WaitToFinish() error
	Notify(err error)
	OnEnqueue() error
}

type baseTask struct {
	done chan error
	ctx  context.Context
	id   UniqueID
}

type addQueryChannelTask struct {
	baseTask
	req  *queryPb.AddQueryChannelRequest
	node *QueryNode
}

type watchDmChannelsTask struct {
	baseTask
	req  *queryPb.WatchDmChannelsRequest
	node *QueryNode
}

type watchDeltaChannelsTask struct {
	baseTask
	req  *queryPb.WatchDeltaChannelsRequest
	node *QueryNode
}

type loadSegmentsTask struct {
	baseTask
	req  *queryPb.LoadSegmentsRequest
	node *QueryNode
}

type releaseCollectionTask struct {
	baseTask
	req  *queryPb.ReleaseCollectionRequest
	node *QueryNode
}

type releasePartitionsTask struct {
	baseTask
	req  *queryPb.ReleasePartitionsRequest
	node *QueryNode
}

func (b *baseTask) ID() UniqueID {
	return b.id
}

func (b *baseTask) SetID(uid UniqueID) {
	b.id = uid
}

func (b *baseTask) WaitToFinish() error {
	err := <-b.done
	return err
}

func (b *baseTask) Notify(err error) {
	b.done <- err
}

// addQueryChannel
func (r *addQueryChannelTask) Timestamp() Timestamp {
	if r.req.Base == nil {
		log.Warn("nil base req in addQueryChannelTask", zap.Any("collectionID", r.req.CollectionID))
		return 0
	}
	return r.req.Base.Timestamp
}

func (r *addQueryChannelTask) OnEnqueue() error {
	if r.req == nil || r.req.Base == nil {
		r.SetID(rand.Int63n(100000000000))
	} else {
		r.SetID(r.req.Base.MsgID)
	}
	return nil
}

func (r *addQueryChannelTask) PreExecute(ctx context.Context) error {
	return nil
}

func (r *addQueryChannelTask) Execute(ctx context.Context) error {
	log.Debug("Execute addQueryChannelTask",
		zap.Any("collectionID", r.req.CollectionID))

	collectionID := r.req.CollectionID
	if r.node.queryService == nil {
		errMsg := "null query service, collectionID = " + fmt.Sprintln(collectionID)
		return errors.New(errMsg)
	}

	if r.node.queryService.hasQueryCollection(collectionID) {
		log.Debug("queryCollection has been existed when addQueryChannel",
			zap.Any("collectionID", collectionID),
		)
		return nil
	}

	// add search collection
	err := r.node.queryService.addQueryCollection(collectionID)
	if err != nil {
		return err
	}
	log.Debug("add query collection", zap.Any("collectionID", collectionID))

	// add request channel
	sc, err := r.node.queryService.getQueryCollection(collectionID)
	if err != nil {
		return err
	}
	consumeChannels := []string{r.req.QueryChannel}
	consumeSubName := funcutil.GenChannelSubName(Params.CommonCfg.QueryNodeSubName, collectionID, Params.QueryNodeCfg.QueryNodeID)

	sc.queryMsgStream.AsConsumer(consumeChannels, consumeSubName)
	metrics.QueryNodeNumConsumers.WithLabelValues(fmt.Sprint(collectionID), fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Inc()
	if r.req.SeekPosition == nil || len(r.req.SeekPosition.MsgID) == 0 {
		// as consumer
		log.Debug("QueryNode AsConsumer", zap.Strings("channels", consumeChannels), zap.String("sub name", consumeSubName))
	} else {
		// seek query channel
		err = sc.queryMsgStream.Seek([]*internalpb.MsgPosition{r.req.SeekPosition})
		if err != nil {
			return err
		}
		log.Debug("querynode seek query channel: ", zap.Any("consumeChannels", consumeChannels),
			zap.String("seek position", string(r.req.SeekPosition.MsgID)))
	}

	// add result channel
	// producerChannels := []string{r.req.QueryResultChannel}
	// sc.queryResultMsgStream.AsProducer(producerChannels)
	// log.Debug("QueryNode AsProducer", zap.Strings("channels", producerChannels))

	// init global sealed segments
	for _, segment := range r.req.GlobalSealedSegments {
		sc.globalSegmentManager.addGlobalSegmentInfo(segment)
	}

	// start queryCollection, message stream need to asConsumer before start
	sc.start()
	log.Debug("start query collection", zap.Any("collectionID", collectionID))

	log.Debug("addQueryChannelTask done",
		zap.Any("collectionID", r.req.CollectionID),
	)
	return nil
}

func (r *addQueryChannelTask) PostExecute(ctx context.Context) error {
	return nil
}

// watchDmChannelsTask
func (w *watchDmChannelsTask) Timestamp() Timestamp {
	if w.req.Base == nil {
		log.Warn("nil base req in watchDmChannelsTask", zap.Any("collectionID", w.req.CollectionID))
		return 0
	}
	return w.req.Base.Timestamp
}

func (w *watchDmChannelsTask) OnEnqueue() error {
	if w.req == nil || w.req.Base == nil {
		w.SetID(rand.Int63n(100000000000))
	} else {
		w.SetID(w.req.Base.MsgID)
	}
	return nil
}

func (w *watchDmChannelsTask) PreExecute(ctx context.Context) error {
	return nil
}

func (w *watchDmChannelsTask) Execute(ctx context.Context) error {
	collectionID := w.req.CollectionID
	partitionIDs := w.req.GetPartitionIDs()

	var lType loadType
	// if no partitionID is specified, load type is load collection
	if len(partitionIDs) != 0 {
		lType = loadTypePartition
	} else {
		lType = loadTypeCollection
	}

	// get all vChannels
	vChannels := make([]Channel, 0)
	pChannels := make([]Channel, 0)
	VPChannels := make(map[string]string) // map[vChannel]pChannel
	for _, info := range w.req.Infos {
		v := info.ChannelName
		p := rootcoord.ToPhysicalChannel(info.ChannelName)
		vChannels = append(vChannels, v)
		pChannels = append(pChannels, p)
		VPChannels[v] = p
	}

	if len(VPChannels) != len(vChannels) {
		return errors.New("get physical channels failed, illegal channel length, collectionID = " + fmt.Sprintln(collectionID))
	}

	log.Debug("Starting WatchDmChannels ...",
		zap.String("collectionName", w.req.Schema.Name),
		zap.Int64("collectionID", collectionID),
		zap.Strings("vChannels", vChannels),
		zap.Strings("pChannels", pChannels),
	)

	// init collection meta
	sCol := w.node.streaming.replica.addCollection(collectionID, w.req.Schema)
	hCol := w.node.historical.replica.addCollection(collectionID, w.req.Schema)

	// load growing segments
	unFlushedSegments := make([]*queryPb.SegmentLoadInfo, 0)
	unFlushedSegmentIDs := make([]UniqueID, 0)
	for _, info := range w.req.Infos {
		for _, ufInfo := range info.UnflushedSegments {
			// unFlushed segment may not have binLogs, skip loading
			if len(ufInfo.Binlogs) > 0 {
				unFlushedSegments = append(unFlushedSegments, &queryPb.SegmentLoadInfo{
					SegmentID:    ufInfo.ID,
					PartitionID:  ufInfo.PartitionID,
					CollectionID: ufInfo.CollectionID,
					BinlogPaths:  ufInfo.Binlogs,
					NumOfRows:    ufInfo.NumOfRows,
					Statslogs:    ufInfo.Statslogs,
					Deltalogs:    ufInfo.Deltalogs,
				})
				unFlushedSegmentIDs = append(unFlushedSegmentIDs, ufInfo.ID)
			}
		}
	}
	req := &queryPb.LoadSegmentsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadSegments,
			MsgID:   w.req.Base.MsgID, // use parent task's msgID
		},
		Infos:        unFlushedSegments,
		CollectionID: collectionID,
		Schema:       w.req.Schema,
	}
	log.Debug("loading growing segments in WatchDmChannels...",
		zap.Int64("collectionID", collectionID),
		zap.Int64s("unFlushedSegmentIDs", unFlushedSegmentIDs),
	)
	err := w.node.loader.loadSegment(req, segmentTypeGrowing)
	if err != nil {
		return err
	}
	log.Debug("successfully load growing segments done in WatchDmChannels",
		zap.Int64("collectionID", collectionID),
		zap.Int64s("unFlushedSegmentIDs", unFlushedSegmentIDs),
	)

	// remove growing segment if watch dmChannels failed
	defer func() {
		if err != nil {
			for _, segmentID := range unFlushedSegmentIDs {
				w.node.streaming.replica.removeSegment(segmentID)
			}
		}
	}()

	consumeSubName := funcutil.GenChannelSubName(Params.CommonCfg.QueryNodeSubName, collectionID, Params.QueryNodeCfg.QueryNodeID)

	// group channels by to seeking or consuming
	channel2SeekPosition := make(map[string]*internalpb.MsgPosition)
	channel2AsConsumerPosition := make(map[string]*internalpb.MsgPosition)
	for _, info := range w.req.Infos {
		if info.SeekPosition == nil || len(info.SeekPosition.MsgID) == 0 {
			channel2AsConsumerPosition[info.ChannelName] = info.SeekPosition
			continue
		}
		info.SeekPosition.MsgGroup = consumeSubName
		channel2SeekPosition[info.ChannelName] = info.SeekPosition
	}
	log.Debug("watchDMChannel, group channels done", zap.Int64("collectionID", collectionID))

	// add excluded segments for unFlushed segments,
	// unFlushed segments before check point should be filtered out.
	unFlushedCheckPointInfos := make([]*datapb.SegmentInfo, 0)
	for _, info := range w.req.Infos {
		unFlushedCheckPointInfos = append(unFlushedCheckPointInfos, info.UnflushedSegments...)
	}
	w.node.streaming.replica.addExcludedSegments(collectionID, unFlushedCheckPointInfos)
	log.Debug("watchDMChannel, add check points info for unFlushed segments done",
		zap.Int64("collectionID", collectionID),
		zap.Any("unFlushedCheckPointInfos", unFlushedCheckPointInfos),
	)

	// add excluded segments for flushed segments,
	// flushed segments with later check point than seekPosition should be filtered out.
	flushedCheckPointInfos := make([]*datapb.SegmentInfo, 0)
	for _, info := range w.req.Infos {
		for _, flushedSegment := range info.FlushedSegments {
			for _, position := range channel2SeekPosition {
				if flushedSegment.DmlPosition != nil &&
					flushedSegment.DmlPosition.ChannelName == position.ChannelName &&
					flushedSegment.DmlPosition.Timestamp > position.Timestamp {
					flushedCheckPointInfos = append(flushedCheckPointInfos, flushedSegment)
				}
			}
		}
	}
	w.node.streaming.replica.addExcludedSegments(collectionID, flushedCheckPointInfos)
	log.Debug("watchDMChannel, add check points info for flushed segments done",
		zap.Int64("collectionID", collectionID),
		zap.Any("flushedCheckPointInfos", flushedCheckPointInfos),
	)

	// add excluded segments for dropped segments,
	// dropped segments with later check point than seekPosition should be filtered out.
	droppedCheckPointInfos := make([]*datapb.SegmentInfo, 0)
	for _, info := range w.req.Infos {
		for _, droppedSegment := range info.DroppedSegments {
			for _, position := range channel2SeekPosition {
				if droppedSegment != nil &&
					droppedSegment.DmlPosition.ChannelName == position.ChannelName &&
					droppedSegment.DmlPosition.Timestamp > position.Timestamp {
					droppedCheckPointInfos = append(droppedCheckPointInfos, droppedSegment)
				}
			}
		}
	}
	w.node.streaming.replica.addExcludedSegments(collectionID, droppedCheckPointInfos)
	log.Debug("watchDMChannel, add check points info for dropped segments done",
		zap.Int64("collectionID", collectionID),
		zap.Any("droppedCheckPointInfos", droppedCheckPointInfos),
	)

	// add flow graph
	channel2FlowGraph := w.node.dataSyncService.addFlowGraphsForDMLChannels(collectionID, vChannels)
	log.Debug("Query node add DML flow graphs", zap.Int64("collectionID", collectionID), zap.Any("channels", vChannels))

	// channels as consumer
	for _, channel := range vChannels {
		fg := channel2FlowGraph[channel]
		if _, ok := channel2AsConsumerPosition[channel]; ok {
			// use pChannel to consume
			err = fg.consumerFlowGraph(VPChannels[channel], consumeSubName)
			if err != nil {
				log.Error("msgStream as consumer failed for dmChannels", zap.Int64("collectionID", collectionID), zap.String("vChannel", channel))
				break
			}
		}

		if pos, ok := channel2SeekPosition[channel]; ok {
			pos.MsgGroup = consumeSubName
			// use pChannel to seek
			pos.ChannelName = VPChannels[channel]
			err = fg.seekQueryNodeFlowGraph(pos)
			if err != nil {
				log.Error("msgStream seek failed for dmChannels", zap.Int64("collectionID", collectionID), zap.String("vChannel", channel))
				break
			}
		}
	}

	if err != nil {
		log.Warn("watchDMChannel, add flowGraph for dmChannels failed", zap.Int64("collectionID", collectionID), zap.Strings("vChannels", vChannels), zap.Error(err))
		for _, fg := range channel2FlowGraph {
			fg.flowGraph.Close()
		}
		w.node.dataSyncService.removeFlowGraphsByDMLChannels(vChannels)
		return err
	}

	log.Debug("watchDMChannel, add flowGraph for dmChannels success", zap.Int64("collectionID", collectionID), zap.Strings("vChannels", vChannels))

	sCol.addVChannels(vChannels)
	sCol.addPChannels(pChannels)
	sCol.setLoadType(lType)

	hCol.addVChannels(vChannels)
	hCol.addPChannels(pChannels)
	hCol.setLoadType(lType)
	if lType == loadTypePartition {
		for _, partitionID := range partitionIDs {
			sCol.deleteReleasedPartition(partitionID)
			hCol.deleteReleasedPartition(partitionID)
			w.node.streaming.replica.addPartition(collectionID, partitionID)
			w.node.historical.replica.addPartition(collectionID, partitionID)
		}
	}
	log.Debug("watchDMChannel, init replica done", zap.Int64("collectionID", collectionID), zap.Strings("vChannels", vChannels))

	// create tSafe
	for _, channel := range vChannels {
		w.node.tSafeReplica.addTSafe(channel)
	}

	// add tSafe watcher if queryCollection exists
	qc, err := w.node.queryService.getQueryCollection(collectionID)
	if err == nil {
		for _, channel := range vChannels {
			err = qc.addTSafeWatcher(channel)
			if err != nil {
				// tSafe have been exist, not error
				log.Warn(err.Error())
			}
		}
	}

	// start flow graphs
	for _, fg := range channel2FlowGraph {
		fg.flowGraph.Start()
	}

	log.Debug("WatchDmChannels done", zap.Int64("collectionID", collectionID), zap.Strings("vChannels", vChannels))
	return nil
}

func (w *watchDmChannelsTask) PostExecute(ctx context.Context) error {
	return nil
}

// watchDeltaChannelsTask
func (w *watchDeltaChannelsTask) Timestamp() Timestamp {
	if w.req.Base == nil {
		log.Warn("nil base req in watchDeltaChannelsTask", zap.Any("collectionID", w.req.CollectionID))
		return 0
	}
	return w.req.Base.Timestamp
}

func (w *watchDeltaChannelsTask) OnEnqueue() error {
	if w.req == nil || w.req.Base == nil {
		w.SetID(rand.Int63n(100000000000))
	} else {
		w.SetID(w.req.Base.MsgID)
	}
	return nil
}

func (w *watchDeltaChannelsTask) PreExecute(ctx context.Context) error {
	return nil
}

func (w *watchDeltaChannelsTask) Execute(ctx context.Context) error {
	collectionID := w.req.CollectionID

	// get all vChannels
	vDeltaChannels := make([]Channel, 0)
	pDeltaChannels := make([]Channel, 0)
	VPDeltaChannels := make(map[string]string) // map[vChannel]pChannel
	vChannel2SeekPosition := make(map[string]*internalpb.MsgPosition)
	for _, info := range w.req.Infos {
		v := info.ChannelName
		p := rootcoord.ToPhysicalChannel(info.ChannelName)
		vDeltaChannels = append(vDeltaChannels, v)
		pDeltaChannels = append(pDeltaChannels, p)
		VPDeltaChannels[v] = p
		vChannel2SeekPosition[v] = info.SeekPosition
	}
	log.Debug("Starting WatchDeltaChannels ...",
		zap.Any("collectionID", collectionID),
		zap.Any("vDeltaChannels", vDeltaChannels),
		zap.Any("pChannels", pDeltaChannels),
	)
	if len(VPDeltaChannels) != len(vDeltaChannels) {
		return errors.New("get physical channels failed, illegal channel length, collectionID = " + fmt.Sprintln(collectionID))
	}
	log.Debug("Get physical channels done",
		zap.Any("collectionID", collectionID),
	)

	if hasCollectionInHistorical := w.node.historical.replica.hasCollection(collectionID); !hasCollectionInHistorical {
		return fmt.Errorf("cannot find collection with collectionID, %d", collectionID)
	}
	hCol, err := w.node.historical.replica.getCollectionByID(collectionID)
	if err != nil {
		return err
	}

	if hasCollectionInStreaming := w.node.streaming.replica.hasCollection(collectionID); !hasCollectionInStreaming {
		return fmt.Errorf("cannot find collection with collectionID, %d", collectionID)
	}
	sCol, err := w.node.streaming.replica.getCollectionByID(collectionID)
	if err != nil {
		return err
	}

	channel2FlowGraph := w.node.dataSyncService.addFlowGraphsForDeltaChannels(collectionID, vDeltaChannels)
	consumeSubName := funcutil.GenChannelSubName(Params.CommonCfg.QueryNodeSubName, collectionID, Params.QueryNodeCfg.QueryNodeID)
	// channels as consumer
	for _, channel := range vDeltaChannels {
		fg := channel2FlowGraph[channel]
		// use pChannel to consume
		err = fg.consumerFlowGraphLatest(VPDeltaChannels[channel], consumeSubName)
		if err != nil {
			log.Error("msgStream as consumer failed for deltaChannels", zap.Int64("collectionID", collectionID), zap.Strings("vDeltaChannels", vDeltaChannels))
			break
		}
		err = w.node.loader.FromDmlCPLoadDelete(w.ctx, collectionID, vChannel2SeekPosition[channel])
		if err != nil {
			log.Error("watchDeltaChannelsTask from dml cp load delete failed", zap.Int64("collectionID", collectionID), zap.Strings("vDeltaChannels", vDeltaChannels))
			break
		}
	}
	if err != nil {
		log.Warn("watchDeltaChannel, add flowGraph for deltaChannel failed", zap.Int64("collectionID", collectionID), zap.Strings("vDeltaChannels", vDeltaChannels), zap.Error(err))
		for _, fg := range channel2FlowGraph {
			fg.flowGraph.Close()
		}
		w.node.dataSyncService.removeFlowGraphsByDeltaChannels(vDeltaChannels)
		return err
	}

	log.Debug("watchDeltaChannel, add flowGraph for deltaChannel success", zap.Int64("collectionID", collectionID), zap.Strings("vDeltaChannels", vDeltaChannels))

	//set collection replica
	hCol.addVDeltaChannels(vDeltaChannels)
	hCol.addPDeltaChannels(pDeltaChannels)

	sCol.addVDeltaChannels(vDeltaChannels)
	sCol.addPDeltaChannels(pDeltaChannels)

	// create tSafe
	for _, channel := range vDeltaChannels {
		w.node.tSafeReplica.addTSafe(channel)
	}

	// add tSafe watcher if queryCollection exists
	qc, err := w.node.queryService.getQueryCollection(collectionID)
	if err == nil {
		for _, channel := range vDeltaChannels {
			err = qc.addTSafeWatcher(channel)
			if err != nil {
				// tSafe have been existed, not error
				log.Warn(err.Error())
			}
		}
	}

	// start flow graphs
	for _, fg := range channel2FlowGraph {
		fg.flowGraph.Start()
	}

	log.Debug("WatchDeltaChannels done", zap.Int64("collectionID", collectionID), zap.String("ChannelIDs", fmt.Sprintln(vDeltaChannels)))
	return nil
}

func (w *watchDeltaChannelsTask) PostExecute(ctx context.Context) error {
	return nil
}

// loadSegmentsTask
func (l *loadSegmentsTask) Timestamp() Timestamp {
	if l.req.Base == nil {
		log.Warn("nil base req in loadSegmentsTask")
		return 0
	}
	return l.req.Base.Timestamp
}

func (l *loadSegmentsTask) OnEnqueue() error {
	if l.req == nil || l.req.Base == nil {
		l.SetID(rand.Int63n(100000000000))
	} else {
		l.SetID(l.req.Base.MsgID)
	}
	return nil
}

func (l *loadSegmentsTask) PreExecute(ctx context.Context) error {
	return nil
}

func (l *loadSegmentsTask) Execute(ctx context.Context) error {
	// TODO: support db
	log.Debug("Query node load segment", zap.String("loadSegmentRequest", fmt.Sprintln(l.req)))
	var err error

	// init meta
	for _, info := range l.req.Infos {
		collectionID := info.CollectionID
		partitionID := info.PartitionID
		l.node.historical.replica.addCollection(collectionID, l.req.Schema)
		err = l.node.historical.replica.addPartition(collectionID, partitionID)
		if err != nil {
			return err
		}
		l.node.streaming.replica.addCollection(collectionID, l.req.Schema)
		err = l.node.streaming.replica.addPartition(collectionID, partitionID)
		if err != nil {
			return err
		}
	}

	err = l.node.loader.loadSegment(l.req, segmentTypeSealed)
	if err != nil {
		log.Warn(err.Error())
		return err
	}

	for _, info := range l.req.Infos {
		collectionID := info.CollectionID
		partitionID := info.PartitionID
		sCol, err := l.node.streaming.replica.getCollectionByID(collectionID)
		if err != nil {
			return err
		}
		sCol.deleteReleasedPartition(partitionID)
		hCol, err := l.node.historical.replica.getCollectionByID(collectionID)
		if err != nil {
			return err
		}
		hCol.deleteReleasedPartition(partitionID)
	}

	log.Debug("LoadSegments done", zap.String("SegmentLoadInfos", fmt.Sprintln(l.req.Infos)))
	return nil
}

func (l *loadSegmentsTask) PostExecute(ctx context.Context) error {
	return nil
}

// releaseCollectionTask
func (r *releaseCollectionTask) Timestamp() Timestamp {
	if r.req.Base == nil {
		log.Warn("nil base req in releaseCollectionTask", zap.Any("collectionID", r.req.CollectionID))
		return 0
	}
	return r.req.Base.Timestamp
}

func (r *releaseCollectionTask) OnEnqueue() error {
	if r.req == nil || r.req.Base == nil {
		r.SetID(rand.Int63n(100000000000))
	} else {
		r.SetID(r.req.Base.MsgID)
	}
	return nil
}

func (r *releaseCollectionTask) PreExecute(ctx context.Context) error {
	return nil
}

type ReplicaType int

const (
	replicaNone ReplicaType = iota
	replicaStreaming
	replicaHistorical
)

func (r *releaseCollectionTask) Execute(ctx context.Context) error {
	log.Debug("Execute release collection task", zap.Any("collectionID", r.req.CollectionID))
	log.Debug("release streaming", zap.Any("collectionID", r.req.CollectionID))
	// sleep to wait for query tasks done
	const gracefulReleaseTime = 1
	time.Sleep(gracefulReleaseTime * time.Second)
	log.Debug("Starting release collection...",
		zap.Any("collectionID", r.req.CollectionID),
	)

	// remove query collection
	// queryCollection and Collection would be deleted in releaseCollection,
	// so we don't need to remove the tSafeWatcher or channel manually.
	r.node.queryService.stopQueryCollection(r.req.CollectionID)

	err := r.releaseReplica(r.node.streaming.replica, replicaStreaming)
	if err != nil {
		return fmt.Errorf("release collection failed, collectionID = %d, err = %s", r.req.CollectionID, err)
	}

	// remove collection metas in streaming and historical
	log.Debug("release historical", zap.Any("collectionID", r.req.CollectionID))
	err = r.releaseReplica(r.node.historical.replica, replicaHistorical)
	if err != nil {
		return fmt.Errorf("release collection failed, collectionID = %d, err = %s", r.req.CollectionID, err)
	}

	debug.FreeOSMemory()

	log.Debug("ReleaseCollection done", zap.Int64("collectionID", r.req.CollectionID))
	return nil
}

func (r *releaseCollectionTask) releaseReplica(replica ReplicaInterface, replicaType ReplicaType) error {
	collection, err := replica.getCollectionByID(r.req.CollectionID)
	if err != nil {
		return err
	}
	// set release time
	log.Debug("set release time", zap.Any("collectionID", r.req.CollectionID))
	collection.setReleaseTime(r.req.Base.Timestamp)

	// remove all flow graphs of the target collection
	var channels []Channel
	if replicaType == replicaStreaming {
		channels = collection.getVChannels()
		r.node.dataSyncService.removeFlowGraphsByDMLChannels(channels)
	} else {
		// remove all tSafes and flow graphs of the target collection
		channels = collection.getVDeltaChannels()
		r.node.dataSyncService.removeFlowGraphsByDeltaChannels(channels)
	}

	// remove all tSafes of the target collection
	for _, channel := range channels {
		log.Debug("Releasing tSafe in releaseCollectionTask...",
			zap.Any("collectionID", r.req.CollectionID),
			zap.Any("vDeltaChannel", channel),
		)
		r.node.tSafeReplica.removeTSafe(channel)
	}

	// remove excludedSegments record
	replica.removeExcludedSegments(r.req.CollectionID)
	err = replica.removeCollection(r.req.CollectionID)
	if err != nil {
		return err
	}
	return nil
}

func (r *releaseCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

// releasePartitionsTask
func (r *releasePartitionsTask) Timestamp() Timestamp {
	if r.req.Base == nil {
		log.Warn("nil base req in releasePartitionsTask", zap.Any("collectionID", r.req.CollectionID))
		return 0
	}
	return r.req.Base.Timestamp
}

func (r *releasePartitionsTask) OnEnqueue() error {
	if r.req == nil || r.req.Base == nil {
		r.SetID(rand.Int63n(100000000000))
	} else {
		r.SetID(r.req.Base.MsgID)
	}
	return nil
}

func (r *releasePartitionsTask) PreExecute(ctx context.Context) error {
	return nil
}

func (r *releasePartitionsTask) Execute(ctx context.Context) error {
	log.Debug("Execute release partition task",
		zap.Any("collectionID", r.req.CollectionID),
		zap.Any("partitionIDs", r.req.PartitionIDs))

	// sleep to wait for query tasks done
	const gracefulReleaseTime = 1
	time.Sleep(gracefulReleaseTime * time.Second)

	// get collection from streaming and historical
	hCol, err := r.node.historical.replica.getCollectionByID(r.req.CollectionID)
	if err != nil {
		return fmt.Errorf("release partitions failed, collectionID = %d, err = %s", r.req.CollectionID, err)
	}
	sCol, err := r.node.streaming.replica.getCollectionByID(r.req.CollectionID)
	if err != nil {
		return fmt.Errorf("release partitions failed, collectionID = %d, err = %s", r.req.CollectionID, err)
	}
	log.Debug("start release partition", zap.Any("collectionID", r.req.CollectionID))

	for _, id := range r.req.PartitionIDs {
		// remove partition from streaming and historical
		hasPartitionInHistorical := r.node.historical.replica.hasPartition(id)
		if hasPartitionInHistorical {
			err := r.node.historical.replica.removePartition(id)
			if err != nil {
				// not return, try to release all partitions
				log.Warn(err.Error())
			}
		}
		hasPartitionInStreaming := r.node.streaming.replica.hasPartition(id)
		if hasPartitionInStreaming {
			err := r.node.streaming.replica.removePartition(id)
			if err != nil {
				// not return, try to release all partitions
				log.Warn(err.Error())
			}
		}

		hCol.addReleasedPartition(id)
		sCol.addReleasedPartition(id)
	}

	log.Debug("Release partition task done",
		zap.Any("collectionID", r.req.CollectionID),
		zap.Any("partitionIDs", r.req.PartitionIDs))
	return nil
}

func (r *releasePartitionsTask) PostExecute(ctx context.Context) error {
	return nil
}
