// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package querynode

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	queryPb "github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/util/mqclient"
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
	consumeChannels := []string{r.req.RequestChannelID}
	consumeSubName := Params.MsgChannelSubName + "-" + strconv.FormatInt(collectionID, 10) + "-" + strconv.Itoa(rand.Int())

	if Params.skipQueryChannelRecovery {
		log.Debug("Skip query channel seek back ", zap.Strings("channels", consumeChannels),
			zap.String("seek position", string(r.req.SeekPosition.MsgID)),
			zap.Uint64("ts", r.req.SeekPosition.Timestamp))
		sc.queryMsgStream.AsConsumerWithPosition(consumeChannels, consumeSubName, mqclient.SubscriptionPositionLatest)
	} else {
		sc.queryMsgStream.AsConsumer(consumeChannels, consumeSubName)
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
	}

	// add result channel
	producerChannels := []string{r.req.ResultChannelID}
	sc.queryResultMsgStream.AsProducer(producerChannels)
	log.Debug("QueryNode AsProducer", zap.Strings("channels", producerChannels))

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
	partitionID := w.req.PartitionID
	// if no partitionID is specified, load type is load collection
	loadPartition := partitionID != 0

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
	log.Debug("Starting WatchDmChannels ...",
		zap.Any("collectionName", w.req.Schema.Name),
		zap.Any("collectionID", collectionID),
		zap.Any("vChannels", vChannels),
		zap.Any("pChannels", pChannels),
	)
	if len(VPChannels) != len(vChannels) {
		return errors.New("get physical channels failed, illegal channel length, collectionID = " + fmt.Sprintln(collectionID))
	}
	log.Debug("Get physical channels done",
		zap.Any("collectionID", collectionID),
	)

	// init replica
	if hasCollectionInStreaming := w.node.streaming.replica.hasCollection(collectionID); !hasCollectionInStreaming {
		err := w.node.streaming.replica.addCollection(collectionID, w.req.Schema)
		if err != nil {
			return err
		}
	}
	// init replica
	if hasCollectionInHistorical := w.node.historical.replica.hasCollection(collectionID); !hasCollectionInHistorical {
		err := w.node.historical.replica.addCollection(collectionID, w.req.Schema)
		if err != nil {
			return err
		}
	}
	var l loadType
	if loadPartition {
		l = loadTypePartition
	} else {
		l = loadTypeCollection
	}
	sCol, err := w.node.streaming.replica.getCollectionByID(collectionID)
	if err != nil {
		return err
	}
	sCol.addVChannels(vChannels)
	sCol.addPChannels(pChannels)
	sCol.setLoadType(l)
	hCol, err := w.node.historical.replica.getCollectionByID(collectionID)
	if err != nil {
		return err
	}
	hCol.addVChannels(vChannels)
	hCol.addPChannels(pChannels)
	hCol.setLoadType(l)
	if loadPartition {
		sCol.deleteReleasedPartition(partitionID)
		hCol.deleteReleasedPartition(partitionID)
		if hasPartitionInStreaming := w.node.streaming.replica.hasPartition(partitionID); !hasPartitionInStreaming {
			err := w.node.streaming.replica.addPartition(collectionID, partitionID)
			if err != nil {
				return err
			}
		}
		if hasPartitionInHistorical := w.node.historical.replica.hasPartition(partitionID); !hasPartitionInHistorical {
			err := w.node.historical.replica.addPartition(collectionID, partitionID)
			if err != nil {
				return err
			}
		}
	}
	log.Debug("watchDMChannel, init replica done", zap.Any("collectionID", collectionID))

	// get subscription name
	getUniqueSubName := func() string {
		prefixName := Params.MsgChannelSubName
		return prefixName + "-" + strconv.FormatInt(collectionID, 10) + "-" + strconv.Itoa(rand.Int())
	}
	consumeSubName := getUniqueSubName()

	// group channels by to seeking or consuming
	toSeekChannels := make([]*internalpb.MsgPosition, 0)
	toSubChannels := make([]Channel, 0)
	for _, info := range w.req.Infos {
		if info.SeekPosition == nil || len(info.SeekPosition.MsgID) == 0 {
			toSubChannels = append(toSubChannels, info.ChannelName)
			continue
		}
		info.SeekPosition.MsgGroup = consumeSubName
		toSeekChannels = append(toSeekChannels, info.SeekPosition)
	}
	log.Debug("watchDMChannel, group channels done", zap.Any("collectionID", collectionID))

	// add excluded segments for unFlushed segments,
	// unFlushed segments before check point should be filtered out.
	unFlushedCheckPointInfos := make([]*datapb.SegmentInfo, 0)
	for _, info := range w.req.Infos {
		unFlushedCheckPointInfos = append(unFlushedCheckPointInfos, info.UnflushedSegments...)
	}
	w.node.streaming.replica.addExcludedSegments(collectionID, unFlushedCheckPointInfos)
	log.Debug("watchDMChannel, add check points info for unFlushed segments done",
		zap.Any("collectionID", collectionID),
		zap.Any("unFlushedCheckPointInfos", unFlushedCheckPointInfos),
	)

	// add excluded segments for flushed segments,
	// flushed segments with later check point than seekPosition should be filtered out.
	flushedCheckPointInfos := make([]*datapb.SegmentInfo, 0)
	for _, info := range w.req.Infos {
		for _, flushedSegment := range info.FlushedSegments {
			for _, position := range toSeekChannels {
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
		zap.Any("collectionID", collectionID),
		zap.Any("flushedCheckPointInfos", flushedCheckPointInfos),
	)

	// add excluded segments for dropped segments,
	// dropped segments with later check point than seekPosition should be filtered out.
	droppedCheckPointInfos := make([]*datapb.SegmentInfo, 0)
	for _, info := range w.req.Infos {
		for _, droppedSegment := range info.DroppedSegments {
			for _, position := range toSeekChannels {
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
		zap.Any("collectionID", collectionID),
		zap.Any("droppedCheckPointInfos", droppedCheckPointInfos),
	)

	// create tSafe
	for _, channel := range vChannels {
		w.node.tSafeReplica.addTSafe(channel)
	}

	// add flow graph
	if loadPartition {
		w.node.dataSyncService.addPartitionFlowGraph(collectionID, partitionID, vChannels)
		log.Debug("Query node add partition flow graphs", zap.Any("channels", vChannels))
	} else {
		w.node.dataSyncService.addCollectionFlowGraph(collectionID, vChannels)
		log.Debug("Query node add collection flow graphs", zap.Any("channels", vChannels))
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

	// channels as consumer
	var nodeFGs map[Channel]*queryNodeFlowGraph
	if loadPartition {
		nodeFGs, err = w.node.dataSyncService.getPartitionFlowGraphs(partitionID, vChannels)
		if err != nil {
			return err
		}
	} else {
		nodeFGs, err = w.node.dataSyncService.getCollectionFlowGraphs(collectionID, vChannels)
		if err != nil {
			return err
		}
	}
	for _, channel := range toSubChannels {
		for _, fg := range nodeFGs {
			if fg.channel == channel {
				// use pChannel to consume
				err := fg.consumerFlowGraph(VPChannels[channel], consumeSubName)
				if err != nil {
					errMsg := "msgStream consume error :" + err.Error()
					log.Warn(errMsg)
					return errors.New(errMsg)
				}
			}
		}
	}
	log.Debug("as consumer channels",
		zap.Any("collectionID", collectionID),
		zap.Any("toSubChannels", toSubChannels))

	// seek channel
	for _, pos := range toSeekChannels {
		for _, fg := range nodeFGs {
			if fg.channel == pos.ChannelName {
				pos.MsgGroup = consumeSubName
				// use pChannel to seek
				pos.ChannelName = VPChannels[fg.channel]
				err := fg.seekQueryNodeFlowGraph(pos)
				if err != nil {
					errMsg := "msgStream seek error :" + err.Error()
					log.Warn(errMsg)
					return errors.New(errMsg)
				}
			}
		}
	}
	log.Debug("Seek all channel done",
		zap.Any("collectionID", collectionID),
		zap.Any("toSeekChannels", toSeekChannels))

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
		Infos:        unFlushedSegments,
		CollectionID: collectionID,
		Schema:       w.req.Schema,
	}
	log.Debug("loading growing segments in WatchDmChannels...",
		zap.Any("collectionID", collectionID),
		zap.Any("unFlushedSegmentIDs", unFlushedSegmentIDs),
	)
	err = w.node.loader.loadSegment(req, segmentTypeGrowing)
	if err != nil {
		return err
	}
	log.Debug("load growing segments done in WatchDmChannels",
		zap.Any("collectionID", collectionID),
		zap.Any("unFlushedSegmentIDs", unFlushedSegmentIDs),
	)

	// start flow graphs
	if loadPartition {
		err = w.node.dataSyncService.startPartitionFlowGraph(partitionID, vChannels)
		if err != nil {
			return err
		}
	} else {
		err = w.node.dataSyncService.startCollectionFlowGraph(collectionID, vChannels)
		if err != nil {
			return err
		}
	}

	log.Debug("WatchDmChannels done", zap.String("ChannelIDs", fmt.Sprintln(vChannels)))
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
	for _, info := range w.req.Infos {
		v := info.ChannelName
		p := rootcoord.ToPhysicalChannel(info.ChannelName)
		vDeltaChannels = append(vDeltaChannels, v)
		pDeltaChannels = append(pDeltaChannels, p)
		VPDeltaChannels[v] = p
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

	// Check if the same deltaChannel has been watched
	for _, dstChan := range vDeltaChannels {
		for _, srcChan := range hCol.vDeltaChannels {
			if dstChan == srcChan {
				return nil
			}
		}
	}

	hCol.addVDeltaChannels(vDeltaChannels)
	hCol.addPDeltaChannels(pDeltaChannels)

	if hasCollectionInStreaming := w.node.streaming.replica.hasCollection(collectionID); !hasCollectionInStreaming {
		return fmt.Errorf("cannot find collection with collectionID, %d", collectionID)
	}
	sCol, err := w.node.streaming.replica.getCollectionByID(collectionID)
	if err != nil {
		return err
	}
	sCol.addVDeltaChannels(vDeltaChannels)
	sCol.addPDeltaChannels(pDeltaChannels)

	// get subscription name
	getUniqueSubName := func() string {
		prefixName := Params.MsgChannelSubName
		return prefixName + "-" + strconv.FormatInt(collectionID, 10) + "-" + strconv.Itoa(rand.Int())
	}
	consumeSubName := getUniqueSubName()

	// group channels by to seeking or consuming
	toSubChannels := make([]Channel, 0)
	for _, info := range w.req.Infos {
		toSubChannels = append(toSubChannels, info.ChannelName)
	}
	log.Debug("watchDeltaChannel, group channels done", zap.Any("collectionID", collectionID))

	// create tSafe
	for _, channel := range vDeltaChannels {
		w.node.tSafeReplica.addTSafe(channel)
	}

	w.node.dataSyncService.addCollectionDeltaFlowGraph(collectionID, vDeltaChannels)

	// add tSafe watcher if queryCollection exists
	qc, err := w.node.queryService.getQueryCollection(collectionID)
	if err == nil {
		for _, channel := range vDeltaChannels {
			err = qc.addTSafeWatcher(channel)
			if err != nil {
				// tSafe have been exist, not error
				log.Warn(err.Error())
			}
		}
	}

	// channels as consumer
	var nodeFGs map[Channel]*queryNodeFlowGraph
	nodeFGs, err = w.node.dataSyncService.getCollectionDeltaFlowGraphs(collectionID, vDeltaChannels)
	if err != nil {
		return err
	}
	for _, channel := range toSubChannels {
		for _, fg := range nodeFGs {
			if fg.channel == channel {
				// use pChannel to consume
				err := fg.consumerFlowGraphLatest(VPDeltaChannels[channel], consumeSubName)
				if err != nil {
					errMsg := "msgStream consume error :" + err.Error()
					log.Warn(errMsg)
					return errors.New(errMsg)
				}
			}
		}
	}
	log.Debug("as consumer channels",
		zap.Any("collectionID", collectionID),
		zap.Any("toSubChannels", toSubChannels))

	for _, info := range w.req.Infos {
		w.node.loader.FromDmlCPLoadDelete(w.ctx, collectionID, info.SeekPosition)
	}

	// start flow graphs
	err = w.node.dataSyncService.startCollectionDeltaFlowGraph(collectionID, vDeltaChannels)
	if err != nil {
		return err
	}

	log.Debug("WatchDeltaChannels done", zap.String("ChannelIDs", fmt.Sprintln(vDeltaChannels)))
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
		hasCollectionInHistorical := l.node.historical.replica.hasCollection(collectionID)
		hasPartitionInHistorical := l.node.historical.replica.hasPartition(partitionID)
		if !hasCollectionInHistorical {
			err = l.node.historical.replica.addCollection(collectionID, l.req.Schema)
			if err != nil {
				return err
			}
		}
		if !hasPartitionInHistorical {
			err = l.node.historical.replica.addPartition(collectionID, partitionID)
			if err != nil {
				return err
			}
		}
		hasCollectionInStreaming := l.node.streaming.replica.hasCollection(collectionID)
		hasPartitionInStreaming := l.node.streaming.replica.hasPartition(partitionID)
		if !hasCollectionInStreaming {
			err = l.node.streaming.replica.addCollection(collectionID, l.req.Schema)
			if err != nil {
				return err
			}
		}
		if !hasPartitionInStreaming {
			err = l.node.streaming.replica.addPartition(collectionID, partitionID)
			if err != nil {
				return err
			}
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
	errMsg := "release collection failed, collectionID = " + strconv.FormatInt(r.req.CollectionID, 10) + ", err = "
	log.Debug("release streaming", zap.Any("collectionID", r.req.CollectionID))
	// sleep to wait for query tasks done
	const gracefulReleaseTime = 1
	time.Sleep(gracefulReleaseTime * time.Second)
	log.Debug("Starting release collection...",
		zap.Any("collectionID", r.req.CollectionID),
	)

	// remove query collection
	r.node.queryService.stopQueryCollection(r.req.CollectionID)

	err := r.releaseReplica(r.node.streaming.replica, replicaStreaming)
	if err != nil {
		return errors.New(errMsg + err.Error())
	}

	// remove collection metas in streaming and historical
	log.Debug("release historical", zap.Any("collectionID", r.req.CollectionID))
	err = r.releaseReplica(r.node.historical.replica, replicaHistorical)
	if err != nil {
		return errors.New(errMsg + err.Error())
	}
	r.node.historical.removeGlobalSegmentIDsByCollectionID(r.req.CollectionID)

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

	if replicaType == replicaStreaming {
		r.node.dataSyncService.removeCollectionFlowGraph(r.req.CollectionID)
		// remove partition flow graphs which partitions belong to the target collection
		partitionIDs, err := replica.getPartitionIDs(r.req.CollectionID)
		if err != nil {
			return err
		}
		for _, partitionID := range partitionIDs {
			r.node.dataSyncService.removePartitionFlowGraph(partitionID)
		}
		// remove all tSafes of the target collection
		for _, channel := range collection.getVChannels() {
			log.Debug("Releasing tSafe in releaseCollectionTask...",
				zap.Any("collectionID", r.req.CollectionID),
				zap.Any("vChannel", channel),
			)
			// no tSafe in tSafeReplica, don't return error
			_ = r.node.tSafeReplica.removeTSafe(channel)
			// queryCollection and Collection would be deleted in releaseCollection,
			// so we don't need to remove the tSafeWatcher or channel manually.
		}
	} else {
		r.node.dataSyncService.removeCollectionDeltaFlowGraph(r.req.CollectionID)
		// remove all tSafes of the target collection
		for _, channel := range collection.getVDeltaChannels() {
			log.Debug("Releasing tSafe in releaseCollectionTask...",
				zap.Any("collectionID", r.req.CollectionID),
				zap.Any("vDeltaChannel", channel),
			)
			// no tSafe in tSafeReplica, don't return error
			_ = r.node.tSafeReplica.removeTSafe(channel)
			// queryCollection and Collection would be deleted in releaseCollection,
			// so we don't need to remove the tSafeWatcher or channel manually.
		}
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
	errMsg := "release partitions failed, collectionID = " + strconv.FormatInt(r.req.CollectionID, 10) + ", err = "

	// sleep to wait for query tasks done
	const gracefulReleaseTime = 1
	time.Sleep(gracefulReleaseTime * time.Second)

	// get collection from streaming and historical
	hCol, err := r.node.historical.replica.getCollectionByID(r.req.CollectionID)
	if err != nil {
		return err
	}
	sCol, err := r.node.streaming.replica.getCollectionByID(r.req.CollectionID)
	if err != nil {
		return err
	}
	log.Debug("start release partition", zap.Any("collectionID", r.req.CollectionID))

	// release partitions
	vChannels := sCol.getVChannels()
	for _, id := range r.req.PartitionIDs {
		if _, err := r.node.dataSyncService.getPartitionFlowGraphs(id, vChannels); err == nil {
			r.node.dataSyncService.removePartitionFlowGraph(id)
			// remove all tSafes of the target partition
			for _, channel := range vChannels {
				log.Debug("Releasing tSafe in releasePartitionTask...",
					zap.Any("collectionID", r.req.CollectionID),
					zap.Any("partitionID", id),
					zap.Any("vChannel", channel),
				)
				// no tSafe in tSafeReplica, don't return error
				isRemoved := r.node.tSafeReplica.removeTSafe(channel)
				if isRemoved {
					// no tSafe or tSafe has been removed,
					// we need to remove the corresponding tSafeWatcher in queryCollection,
					// and remove the corresponding channel in collection
					qc, err := r.node.queryService.getQueryCollection(r.req.CollectionID)
					if err != nil {
						return err
					}
					err = qc.removeTSafeWatcher(channel)
					if err != nil {
						return err
					}
					sCol.removeVChannel(channel)
					hCol.removeVChannel(channel)
				}
			}
		}

		// remove partition from streaming and historical
		hasPartitionInHistorical := r.node.historical.replica.hasPartition(id)
		if hasPartitionInHistorical {
			err := r.node.historical.replica.removePartition(id)
			if err != nil {
				// not return, try to release all partitions
				log.Warn(errMsg + err.Error())
			}
		}
		hasPartitionInStreaming := r.node.streaming.replica.hasPartition(id)
		if hasPartitionInStreaming {
			err := r.node.streaming.replica.removePartition(id)
			if err != nil {
				// not return, try to release all partitions
				log.Warn(errMsg + err.Error())
			}
		}

		hCol.addReleasedPartition(id)
		sCol.addReleasedPartition(id)
	}
	pids, err := r.node.historical.replica.getPartitionIDs(r.req.CollectionID)
	if err != nil {
		return err
	}
	log.Debug("start release history pids", zap.Any("pids", pids), zap.Any("load type", hCol.getLoadType()))
	if len(pids) == 0 && hCol.getLoadType() == loadTypePartition {
		r.node.dataSyncService.removeCollectionDeltaFlowGraph(r.req.CollectionID)
		log.Debug("release delta channels", zap.Any("deltaChannels", hCol.getVDeltaChannels()))
		vChannels := hCol.getVDeltaChannels()
		for _, channel := range vChannels {
			log.Debug("Releasing tSafe in releasePartitionTask...",
				zap.Any("collectionID", r.req.CollectionID),
				zap.Any("vChannel", channel),
			)
			// no tSafe in tSafeReplica, don't return error
			isRemoved := r.node.tSafeReplica.removeTSafe(channel)
			if isRemoved {
				// no tSafe or tSafe has been removed,
				// we need to remove the corresponding tSafeWatcher in queryCollection,
				// and remove the corresponding channel in collection
				qc, err := r.node.queryService.getQueryCollection(r.req.CollectionID)
				if err != nil {
					return err
				}
				err = qc.removeTSafeWatcher(channel)
				if err != nil {
					return err
				}
				sCol.removeVDeltaChannel(channel)
				hCol.removeVDeltaChannel(channel)
			}
		}
	}

	// release global segment info
	r.node.historical.removeGlobalSegmentIDsByPartitionIds(r.req.PartitionIDs)

	log.Debug("Release partition task done",
		zap.Any("collectionID", r.req.CollectionID),
		zap.Any("partitionIDs", r.req.PartitionIDs))
	return nil
}

func (r *releasePartitionsTask) PostExecute(ctx context.Context) error {
	return nil
}
