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
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
)

type vChannelStage struct {
	ctx    context.Context
	cancel context.CancelFunc

	collectionID UniqueID
	vChannel     Channel

	input       chan queryMsg
	queryOutput chan queryResult

	streaming *streaming

	unsolvedMsgMu sync.Mutex // guards unsolvedMsg
	unsolvedMsg   []queryMsg

	tSafeWatcher *tSafeWatcher
}

func newVChannelStage(ctx context.Context,
	collectionID UniqueID,
	vChannel Channel,
	input chan queryMsg,
	queryOutput chan queryResult,
	streaming *streaming) *vChannelStage {

	ctx1, cancel := context.WithCancel(ctx)
	return &vChannelStage{
		ctx:          ctx1,
		cancel:       cancel,
		collectionID: collectionID,
		vChannel:     vChannel,
		input:        input,
		queryOutput:  queryOutput,
		streaming:    streaming,
	}
}

func (q *vChannelStage) start() {
	q.register()
	for {
		select {
		case <-q.ctx.Done():
			log.Debug("stop vChannelStage", zap.Int64("collectionID", q.collectionID))
			return
		case msg := <-q.input:
			q.receiveQueryMsg(msg)
		case <-q.tSafeWatcher.notifyChan:
			t := q.streaming.tSafeReplica.getTSafe(q.vChannel)
			q.doUnsolvedQueryMsg(t)
		}
	}
}

func (q *vChannelStage) stop() {
	q.cancel()
}

func (q *vChannelStage) receiveQueryMsg(msg queryMsg) {
	// tSafe check
	guaranteeTs := msg.GuaranteeTs()
	serviceTime := q.streaming.tSafeReplica.getTSafe(q.vChannel)
	if guaranteeTs > serviceTime {
		gt, _ := tsoutil.ParseTS(guaranteeTs)
		st, _ := tsoutil.ParseTS(serviceTime)
		log.Debug("query node::vChannelStage: add to query unsolved",
			zap.Any("collectionID", q.collectionID),
			zap.Any("sm.GuaranteeTimestamp", gt),
			zap.Any("serviceTime", st),
			zap.Any("delta seconds", (guaranteeTs-serviceTime)/(1000*1000*1000)),
			zap.Any("msgID", msg.ID()),
			zap.Any("msgType", msg.Type()),
		)
		q.addToUnsolvedMsg(msg)
		return
	}

	log.Debug("doing query in receiveQueryMsg...",
		zap.Any("collectionID", q.collectionID),
		zap.Any("msgID", msg.ID()),
		zap.Any("msgType", msg.Type()),
	)
	err := q.execute(msg)
	if err != nil {
		log.Debug("query failed in receiveQueryMsg",
			zap.Int64("collectionID", q.collectionID),
			zap.Int64("msgID", msg.ID()),
			zap.Error(err),
		)
	} else {
		log.Debug("query done in receiveQueryMsg",
			zap.Int64("collectionID", q.collectionID),
			zap.Int64("msgID", msg.ID()),
		)
	}
}

func (q *vChannelStage) doUnsolvedQueryMsg(serviceTime Timestamp) {
	//st, _ := tsoutil.ParseTS(serviceTime)
	//log.Debug("get tSafe from flow graph",
	//	zap.Int64("collectionID", q.collectionID),
	//	zap.Any("tSafe", st))
	unSolvedMsg := make([]queryMsg, 0)
	tempMsg := q.popAllUnsolvedMsg()

	for _, m := range tempMsg {
		guaranteeTs := m.GuaranteeTs()
		gt, _ := tsoutil.ParseTS(guaranteeTs)
		st, _ := tsoutil.ParseTS(serviceTime)
		log.Debug("get query message from unsolvedMsg",
			zap.Int64("collectionID", q.collectionID),
			zap.Int64("msgID", m.ID()),
			zap.Any("reqTime_p", gt),
			zap.Any("serviceTime_p", st),
			zap.Any("guaranteeTime_l", guaranteeTs),
			zap.Any("serviceTime_l", serviceTime),
		)
		// release check
		collection, err := q.streaming.replica.getCollectionByID(q.collectionID)
		if err != nil {
			q.queryError(m, err)
			continue
		}
		if guaranteeTs >= collection.getReleaseTime() {
			err = fmt.Errorf("collection has been released, msgID = %d, collectionID = %d", m.ID(), q.collectionID)
			q.queryError(m, err)
			continue
		}
		// service time check
		if guaranteeTs <= serviceTime {
			unSolvedMsg = append(unSolvedMsg, m)
			continue
		}
		log.Debug("query node::doUnsolvedMsg: add to unsolvedMsg",
			zap.Any("collectionID", q.collectionID),
			zap.Any("sm.BeginTs", gt),
			zap.Any("serviceTime", st),
			zap.Any("delta seconds", (guaranteeTs-serviceTime)/(1000*1000*1000)),
			zap.Any("msgID", m.ID()),
		)
		q.addToUnsolvedMsg(m)
	}

	if len(unSolvedMsg) <= 0 {
		return
	}
	for _, msg := range unSolvedMsg {
		log.Debug("doing query in doUnsolvedQueryMsg...",
			zap.Any("collectionID", q.collectionID),
			zap.Any("msgID", msg.ID()),
			zap.Any("msgType", msg.Type()),
		)
		err := q.execute(msg)
		if err != nil {
			log.Debug("query failed in doUnsolvedQueryMsg",
				zap.Int64("collectionID", q.collectionID),
				zap.Int64("msgID", msg.ID()),
				zap.Error(err),
			)
		} else {
			log.Debug("query done in doUnsolvedQueryMsg",
				zap.Int64("collectionID", q.collectionID),
				zap.Int64("msgID", msg.ID()),
			)
		}
	}
	log.Debug("doUnsolvedMsg: do query done", zap.Int("num of query msg", len(unSolvedMsg)))
}

func (q *vChannelStage) execute(msg queryMsg) error {
	msgType := msg.Type()
	var err error

	switch msgType {
	case commonpb.MsgType_Retrieve:
		rm := msg.(*retrieveMsg)
		segmentRetrieved, res, err := q.retrieve(rm)
		retrieveRes := &retrieveResult{
			msg:              rm,
			err:              err,
			segmentRetrieved: segmentRetrieved,
			vChannel:         q.vChannel,
			res:              res,
		}
		q.queryOutput <- retrieveRes
	case commonpb.MsgType_Search:
		sm := msg.(*searchMsg)
		searchResults, sealedSegmentSearched, err := q.search(sm)
		searchRes := &searchResult{
			reqs:                  sm.reqs,
			msg:                   sm,
			err:                   err,
			searchResults:         searchResults,
			sealedSegmentSearched: sealedSegmentSearched,
			vChannel:              q.vChannel,
		}
		q.queryOutput <- searchRes
	default:
		err = fmt.Errorf("receive invalid msgType = %d", msgType)
		log.Warn(err.Error())
	}

	return err
}

func (q *vChannelStage) register() {
	channel := q.vChannel
	log.Debug("register tSafe watcher and init watcher select case",
		zap.Any("collectionID", q.collectionID),
		zap.Any("dml channel", q.vChannel),
	)
	q.tSafeWatcher = newTSafeWatcher()
	q.streaming.tSafeReplica.registerTSafeWatcher(channel, q.tSafeWatcher)
}

func (q *vChannelStage) addToUnsolvedMsg(msg queryMsg) {
	q.unsolvedMsgMu.Lock()
	defer q.unsolvedMsgMu.Unlock()
	q.unsolvedMsg = append(q.unsolvedMsg, msg)
}

func (q *vChannelStage) popAllUnsolvedMsg() []queryMsg {
	q.unsolvedMsgMu.Lock()
	defer q.unsolvedMsgMu.Unlock()
	tmp := q.unsolvedMsg
	q.unsolvedMsg = q.unsolvedMsg[:0]
	return tmp
}

func (q *vChannelStage) queryError(msg queryMsg, err error) {
	msgType := msg.Type()
	log.Debug("queryError in unsolvedStage",
		zap.Int64("collectionID", q.collectionID),
		zap.Int64("msgID", msg.ID()),
		zap.Error(err),
	)

	switch msgType {
	case commonpb.MsgType_Retrieve:
		rm := msg.(*retrieveMsg)
		retrieveRes := &retrieveResult{
			msg:      rm,
			err:      err,
			vChannel: q.vChannel,
		}
		q.queryOutput <- retrieveRes
	case commonpb.MsgType_Search:
		sm := msg.(*searchMsg)
		searchRes := &searchResult{
			reqs:     sm.reqs,
			msg:      sm,
			err:      err,
			vChannel: q.vChannel,
		}
		q.queryOutput <- searchRes
	}
}

func (q *vChannelStage) retrieve(retrieveMsg *retrieveMsg) ([]UniqueID, []*segcorepb.RetrieveResults, error) {
	collectionID := retrieveMsg.CollectionID
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("retrieve %d", collectionID))

	var partitionIDsInStreaming []UniqueID
	partitionIDsInQuery := retrieveMsg.PartitionIDs
	if len(partitionIDsInQuery) == 0 {
		partitionIDsInStreamingCol, err := q.streaming.replica.getPartitionIDs(collectionID)
		if err != nil {
			return nil, nil, err
		}
		partitionIDsInStreaming = partitionIDsInStreamingCol
	} else {
		for _, id := range partitionIDsInQuery {
			_, err := q.streaming.replica.getPartitionByID(id)
			if err != nil {
				return nil, nil, err
			}
			partitionIDsInStreaming = append(partitionIDsInStreaming, id)
		}
	}
	sealedSegmentSearched := make([]UniqueID, 0)
	var mergeList []*segcorepb.RetrieveResults
	for _, partitionID := range partitionIDsInStreaming {
		segmentIDs, err := q.streaming.replica.getSegmentIDs(partitionID)
		if err != nil {
			return nil, nil, err
		}
		for _, segmentID := range segmentIDs {
			segment, err := q.streaming.replica.getSegmentByID(segmentID)
			if err != nil {
				return nil, nil, err
			}
			result, err := segment.getEntityByIds(retrieveMsg.plan)
			if err != nil {
				return nil, nil, err
			}
			mergeList = append(mergeList, result)
		}
	}
	tr.Record("streaming retrieve done")
	tr.Elapse("all done")
	return sealedSegmentSearched, mergeList, nil
}

func (q *vChannelStage) search(searchMsg *searchMsg) ([]*SearchResult, []UniqueID, error) {
	travelTimestamp := searchMsg.TravelTimestamp
	collectionID := searchMsg.CollectionID

	// multiple search is not supported for now
	if len(searchMsg.reqs) != 1 {
		return nil, nil, errors.New("illegal search requests, collectionID = " + fmt.Sprintln(collectionID))
	}
	queryNum := searchMsg.reqs[0].getNumOfQuery()
	topK := searchMsg.plan.getTopK()

	tr := timerecord.NewTimeRecorder(fmt.Sprintf("search %d(nq=%d, k=%d)", searchMsg.CollectionID, queryNum, topK))
	sealedSegmentSearched := make([]UniqueID, 0)

	// streaming search
	strSearchResults, err := q.streaming.search(searchMsg.reqs,
		collectionID,
		searchMsg.PartitionIDs,
		q.vChannel,
		searchMsg.plan,
		travelTimestamp)

	if err != nil {
		log.Warn(err.Error())
		return nil, nil, err
	}

	tr.Record("streaming search done")
	tr.Elapse("all done")
	return strSearchResults, sealedSegmentSearched, nil
}
