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
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	queryPb "github.com/milvus-io/milvus/internal/proto/querypb"
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
		log.Error("nil base req in watchDmChannelsTask", zap.Any("collectionID", w.req.CollectionID))
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
	loadPartition := partitionID != 0
	consumeChannels := w.req.ChannelIDs
	log.Debug("starting WatchDmChannels ...", zap.String("ChannelIDs", fmt.Sprintln(consumeChannels)))

	// 1. init replica
	if hasCollectionInStreaming := w.node.streaming.replica.hasCollection(collectionID); !hasCollectionInStreaming {
		err := w.node.streaming.replica.addCollection(collectionID, w.req.Schema)
		if err != nil {
			return err
		}
		w.node.streaming.replica.initExcludedSegments(collectionID)
		collection, err := w.node.streaming.replica.getCollectionByID(collectionID)
		if err != nil {
			return err
		}
		collection.addWatchedDmChannels(w.req.ChannelIDs)
	}
	if loadPartition {
		if hasPartitionInStreaming := w.node.streaming.replica.hasPartition(partitionID); !hasPartitionInStreaming {
			err := w.node.streaming.replica.addPartition(collectionID, partitionID)
			if err != nil {
				return err
			}
		}
	}

	// 2. get subscription name
	getUniqueSubName := func() string {
		prefixName := Params.MsgChannelSubName
		return prefixName + "-" + strconv.FormatInt(collectionID, 10)
	}
	consumeSubName := getUniqueSubName()

	// 3. group channels by to seeking or consuming
	toSeekChannels := make([]*internalpb.MsgPosition, 0)
	toSubChannels := make([]string, 0)
	for _, info := range w.req.Infos {
		if len(info.Pos.MsgID) == 0 {
			toSubChannels = append(toSubChannels, info.ChannelID)
			continue
		}
		info.Pos.MsgGroup = consumeSubName
		toSeekChannels = append(toSeekChannels, info.Pos)
	}

	// 4. add excluded info
	err := w.node.streaming.replica.addExcludedSegments(collectionID, w.req.ExcludeInfos)
	if err != nil {
		log.Error(err.Error())
		return err
	}
	excludedSegment := make([]UniqueID, 0) // for logging
	for _, ei := range w.req.ExcludeInfos {
		excludedSegment = append(excludedSegment, ei.SegmentID)
	}
	log.Debug("prevent inserting segments", zap.String("segmentIDs", fmt.Sprintln(excludedSegment)))

	// 4. add flow graph
	if loadPartition {
		err = w.node.streaming.dataSyncService.addPartitionFlowGraph(collectionID, partitionID, consumeChannels)
		if err != nil {
			return err
		}
		log.Debug("query node add partition flow graphs", zap.Any("channels", consumeChannels))
	} else {
		err = w.node.streaming.dataSyncService.addCollectionFlowGraph(collectionID, consumeChannels)
		if err != nil {
			return err
		}
		log.Debug("query node add collection flow graphs", zap.Any("channels", consumeChannels))
	}


	// 5. channels as consumer
	var nodeFGs []*queryNodeFlowGraph
	if loadPartition {
		nodeFGs, err = w.node.streaming.dataSyncService.getPartitionFlowGraphs(partitionID)
		if err != nil {
			return err
		}
	} else {
		nodeFGs, err = w.node.streaming.dataSyncService.getCollectionFlowGraphs(collectionID)
		if err != nil {
			return err
		}
	}
	for _, channel := range toSubChannels {
		for _, fg := range nodeFGs {
			if fg.channel == channel {
				err := fg.consumerFlowGraph(channel, consumeSubName)
				if err != nil {
					errMsg := "msgStream consume error :" + err.Error()
					log.Error(errMsg)
					return errors.New(errMsg)
				}
			}
		}
	}
	log.Debug("as consumer channels", zap.Any("channels", consumeChannels))

	// 6. seek channel
	for _, pos := range toSeekChannels {
		for _, fg := range nodeFGs {
			if fg.channel == pos.ChannelName {
				err := fg.seekQueryNodeFlowGraph(pos)
				if err != nil {
					errMsg := "msgStream seek error :" + err.Error()
					log.Error(errMsg)
					return errors.New(errMsg)
				}
			}
		}
	}

	// 7. start search collection
	w.node.searchService.startSearchCollection(collectionID)
	log.Debug("start search collection", zap.Any("collectionID", collectionID))

	// 8. start flow graphs
	if loadPartition {
		err = w.node.streaming.dataSyncService.startPartitionFlowGraph(partitionID)
		if err != nil {
			return err
		}
	} else {
		err = w.node.streaming.dataSyncService.startCollectionFlowGraph(collectionID)
		if err != nil {
			return err
		}
	}

	log.Debug("WatchDmChannels done", zap.String("ChannelIDs", fmt.Sprintln(w.req.ChannelIDs)))
	return nil
}

func (w *watchDmChannelsTask) PostExecute(ctx context.Context) error {
	return nil
}

// loadSegmentsTask
func (l *loadSegmentsTask) Timestamp() Timestamp {
	if l.req.Base == nil {
		log.Error("nil base req in loadSegmentsTask", zap.Any("collectionID", l.req.CollectionID))
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
		log.Error(err.Error())
		return err
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
		log.Error("nil base req in releaseCollectionTask", zap.Any("collectionID", r.req.CollectionID))
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
	r.node.streaming.dataSyncService.removeCollectionFlowGraph(r.req.CollectionID)
	collection, err := r.node.historical.replica.getCollectionByID(r.req.CollectionID)
	if err != nil {
		log.Error(err.Error())
	} else {
		// remove all tSafes of the target collection
		for _, channel := range collection.getWatchedDmChannels() {
			r.node.streaming.tSafeReplica.removeTSafe(channel)
		}
	}

	r.node.streaming.replica.removeExcludedSegments(r.req.CollectionID)

	if r.node.searchService.hasSearchCollection(r.req.CollectionID) {
		r.node.searchService.stopSearchCollection(r.req.CollectionID)
	}

	hasCollectionInHistorical := r.node.historical.replica.hasCollection(r.req.CollectionID)
	if hasCollectionInHistorical {
		err := r.node.historical.replica.removeCollection(r.req.CollectionID)
		if err != nil {
			return err
		}
	}

	hasCollectionInStreaming := r.node.streaming.replica.hasCollection(r.req.CollectionID)
	if hasCollectionInStreaming {
		err := r.node.streaming.replica.removeCollection(r.req.CollectionID)
		if err != nil {
			return err
		}
	}

	// TODO: for debugging, remove this
	time.Sleep(2 * time.Second)

	log.Debug("ReleaseCollection done", zap.Int64("collectionID", r.req.CollectionID))
	return nil
}

func (r *releaseCollectionTask) PostExecute(ctx context.Context) error {
	return nil
}

// releasePartitionsTask
func (r *releasePartitionsTask) Timestamp() Timestamp {
	if r.req.Base == nil {
		log.Error("nil base req in releasePartitionsTask", zap.Any("collectionID", r.req.CollectionID))
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
	for _, id := range r.req.PartitionIDs {
		hasPartitionInHistorical := r.node.historical.replica.hasPartition(id)
		if hasPartitionInHistorical {
			err := r.node.historical.replica.removePartition(id)
			if err != nil {
				// not return, try to release all partitions
				log.Error(err.Error())
			}
		}

		hasPartitionInStreaming := r.node.streaming.replica.hasPartition(id)
		if hasPartitionInStreaming {
			err := r.node.streaming.replica.removePartition(id)
			if err != nil {
				log.Error(err.Error())
			}
		}
	}
	return nil
}

func (r *releasePartitionsTask) PostExecute(ctx context.Context) error {
	return nil
}
