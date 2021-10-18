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

type watchDmChannelsTask struct {
	baseTask
	req  *queryPb.WatchDmChannelsRequest
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
	log.Debug("starting WatchDmChannels ...",
		zap.Any("collectionName", w.req.Schema.Name),
		zap.Any("collectionID", collectionID),
		zap.Any("vChannels", vChannels),
		zap.Any("pChannels", pChannels),
	)
	if len(VPChannels) != len(vChannels) {
		return errors.New("get physical channels failed, illegal channel length, collectionID = " + fmt.Sprintln(collectionID))
	}
	log.Debug("get physical channels done",
		zap.Any("collectionID", collectionID),
	)

	// init replica
	if hasCollectionInStreaming := w.node.streaming.replica.hasCollection(collectionID); !hasCollectionInStreaming {
		err := w.node.streaming.replica.addCollection(collectionID, w.req.Schema)
		if err != nil {
			return err
		}
	}
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

	// add check points info
	checkPointInfos := make([]*datapb.SegmentInfo, 0)
	for _, info := range w.req.Infos {
		checkPointInfos = append(checkPointInfos, info.UnflushedSegments...)
	}
	w.node.streaming.replica.addExcludedSegments(collectionID, checkPointInfos)
	log.Debug("watchDMChannel, add check points info done", zap.Any("collectionID", collectionID))

	// create tSafe
	for _, channel := range vChannels {
		w.node.streaming.tSafeReplica.addTSafe(channel)
	}

	// add flow graph
	if loadPartition {
		w.node.streaming.dataSyncService.addPartitionFlowGraph(collectionID, partitionID, vChannels)
		log.Debug("query node add partition flow graphs", zap.Any("channels", vChannels))
	} else {
		w.node.streaming.dataSyncService.addCollectionFlowGraph(collectionID, vChannels)
		log.Debug("query node add collection flow graphs", zap.Any("channels", vChannels))
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
		nodeFGs, err = w.node.streaming.dataSyncService.getPartitionFlowGraphs(partitionID, vChannels)
		if err != nil {
			return err
		}
	} else {
		nodeFGs, err = w.node.streaming.dataSyncService.getCollectionFlowGraphs(collectionID, vChannels)
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
	log.Debug("seek all channel done",
		zap.Any("collectionID", collectionID),
		zap.Any("toSeekChannels", toSeekChannels))

	// start flow graphs
	if loadPartition {
		err = w.node.streaming.dataSyncService.startPartitionFlowGraph(partitionID, vChannels)
		if err != nil {
			return err
		}
	} else {
		err = w.node.streaming.dataSyncService.startCollectionFlowGraph(collectionID, vChannels)
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
	log.Debug("query node load segment", zap.String("loadSegmentRequest", fmt.Sprintln(l.req)))
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

	err = checkSegmentMemory(l.req.Infos, l.node.historical.replica, l.node.streaming.replica)
	if err != nil {
		log.Warn(err.Error())
		return err
	}

	switch l.req.LoadCondition {
	case queryPb.TriggerCondition_handoff:
		err = l.node.historical.loader.loadSegmentOfConditionHandOff(l.req)
	case queryPb.TriggerCondition_loadBalance:
		err = l.node.historical.loader.loadSegmentOfConditionLoadBalance(l.req)
	case queryPb.TriggerCondition_grpcRequest:
		err = l.node.historical.loader.loadSegmentOfConditionGRPC(l.req)
	case queryPb.TriggerCondition_nodeDown:
		err = l.node.historical.loader.loadSegmentOfConditionNodeDown(l.req)
	}

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

func (r *releaseCollectionTask) Execute(ctx context.Context) error {
	log.Debug("receive release collection task", zap.Any("collectionID", r.req.CollectionID))
	errMsg := "release collection failed, collectionID = " + strconv.FormatInt(r.req.CollectionID, 10) + ", err = "
	collection, err := r.node.streaming.replica.getCollectionByID(r.req.CollectionID)
	if err != nil {
		err = errors.New(errMsg + err.Error())
		return err
	}

	// set release time
	collection.setReleaseTime(r.req.Base.Timestamp)

	// sleep to wait for query tasks done
	const gracefulReleaseTime = 1
	time.Sleep(gracefulReleaseTime * time.Second)
	log.Debug("starting release collection...",
		zap.Any("collectionID", r.req.CollectionID),
	)

	// remove collection flow graph
	r.node.streaming.dataSyncService.removeCollectionFlowGraph(r.req.CollectionID)

	// remove partition flow graphs which partitions belong to the target collection
	partitionIDs, err := r.node.streaming.replica.getPartitionIDs(r.req.CollectionID)
	if err != nil {
		err = errors.New(errMsg + err.Error())
		return err
	}
	for _, partitionID := range partitionIDs {
		r.node.streaming.dataSyncService.removePartitionFlowGraph(partitionID)
	}

	// remove all tSafes of the target collection
	for _, channel := range collection.getVChannels() {
		log.Debug("releasing tSafe in releaseCollectionTask...",
			zap.Any("collectionID", r.req.CollectionID),
			zap.Any("vChannel", channel),
		)
		// no tSafe in tSafeReplica, don't return error
		err = r.node.streaming.tSafeReplica.removeTSafe(channel)
		if err != nil {
			log.Warn(err.Error())
		}
	}

	// remove excludedSegments record
	r.node.streaming.replica.removeExcludedSegments(r.req.CollectionID)

	// remove query collection
	r.node.queryService.stopQueryCollection(r.req.CollectionID)

	// remove collection metas in streaming and historical
	hasCollectionInHistorical := r.node.historical.replica.hasCollection(r.req.CollectionID)
	if hasCollectionInHistorical {
		err = r.node.historical.replica.removeCollection(r.req.CollectionID)
		if err != nil {
			err = errors.New(errMsg + err.Error())
			return err
		}
	}
	hasCollectionInStreaming := r.node.streaming.replica.hasCollection(r.req.CollectionID)
	if hasCollectionInStreaming {
		err = r.node.streaming.replica.removeCollection(r.req.CollectionID)
		if err != nil {
			err = errors.New(errMsg + err.Error())
			return err
		}
	}

	// release global segment info
	r.node.historical.removeGlobalSegmentIDsByCollectionID(r.req.CollectionID)

	log.Debug("ReleaseCollection done", zap.Int64("collectionID", r.req.CollectionID))
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
	log.Debug("receive release partition task",
		zap.Any("collectionID", r.req.CollectionID),
		zap.Any("partitionIDs", r.req.PartitionIDs))
	errMsg := "release partitions failed, collectionID = " + strconv.FormatInt(r.req.CollectionID, 10) + ", err = "

	// sleep to wait for query tasks done
	const gracefulReleaseTime = 1
	time.Sleep(gracefulReleaseTime * time.Second)

	// get collection from streaming and historical
	hCol, err := r.node.historical.replica.getCollectionByID(r.req.CollectionID)
	if err != nil {
		err = errors.New(errMsg + err.Error())
		return err
	}
	sCol, err := r.node.streaming.replica.getCollectionByID(r.req.CollectionID)
	if err != nil {
		err = errors.New(errMsg + err.Error())
		return err
	}

	// release partitions
	vChannels := sCol.getVChannels()
	for _, id := range r.req.PartitionIDs {
		if _, err = r.node.streaming.dataSyncService.getPartitionFlowGraphs(id, vChannels); err == nil {
			r.node.streaming.dataSyncService.removePartitionFlowGraph(id)
			// remove all tSafes of the target partition
			for _, channel := range vChannels {
				log.Debug("releasing tSafe in releasePartitionTask...",
					zap.Any("collectionID", r.req.CollectionID),
					zap.Any("partitionID", id),
					zap.Any("vChannel", channel),
				)
				// no tSafe in tSafeReplica, don't return error
				err = r.node.streaming.tSafeReplica.removeTSafe(channel)
				if err != nil {
					log.Warn(err.Error())
				}
			}
		}

		// remove partition from streaming and historical
		hasPartitionInHistorical := r.node.historical.replica.hasPartition(id)
		if hasPartitionInHistorical {
			err = r.node.historical.replica.removePartition(id)
			if err != nil {
				// not return, try to release all partitions
				log.Warn(errMsg + err.Error())
			}
		}
		hasPartitionInStreaming := r.node.streaming.replica.hasPartition(id)
		if hasPartitionInStreaming {
			err = r.node.streaming.replica.removePartition(id)
			if err != nil {
				// not return, try to release all partitions
				log.Warn(errMsg + err.Error())
			}
		}

		// add released partition record
		hCol.addReleasedPartition(id)
		sCol.addReleasedPartition(id)
	}

	// release global segment info
	r.node.historical.removeGlobalSegmentIDsByPartitionIds(r.req.PartitionIDs)

	log.Debug("release partition task done",
		zap.Any("collectionID", r.req.CollectionID),
		zap.Any("partitionIDs", r.req.PartitionIDs))
	return nil
}

func (r *releasePartitionsTask) PostExecute(ctx context.Context) error {
	return nil
}
