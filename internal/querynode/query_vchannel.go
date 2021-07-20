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

	oplog "github.com/opentracing/opentracing-go/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
)

type vChannelStage struct {
	ctx    context.Context
	cancel context.CancelFunc

	collectionID UniqueID
	vChannel     Channel

	input          chan queryMsg
	unsolvedOutput chan queryMsg
	queryOutput    chan queryResult

	streaming *streaming
}

func newVChannelStage(ctx context.Context,
	cancel context.CancelFunc,
	collectionID UniqueID,
	vChannel Channel,
	input chan queryMsg,
	unsolvedOutput chan queryMsg,
	queryOutput chan queryResult,
	streaming *streaming) *vChannelStage {

	return &vChannelStage{
		ctx:            ctx,
		cancel:         cancel,
		collectionID:   collectionID,
		vChannel:       vChannel,
		input:          input,
		unsolvedOutput: unsolvedOutput,
		queryOutput:    queryOutput,
		streaming:      streaming,
	}
}

func (q *vChannelStage) start() {
	for {
		select {
		case <-q.ctx.Done():
			log.Debug("stop vChannelStage", zap.Int64("collectionID", q.collectionID))
			return
		case msg := <-q.input:
			msgType := msg.Type()
			sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
			msg.SetTraceCtx(ctx)

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
				q.unsolvedOutput <- msg
				sp.LogFields(
					oplog.String("send to query unsolved", "send to query unsolved"),
					oplog.Object("guarantee ts", gt),
					oplog.Object("serviceTime", st),
					oplog.Float64("delta seconds", float64(guaranteeTs-serviceTime)/(1000.0*1000.0*1000.0)),
				)
				sp.Finish()
				continue
			}

			log.Debug("doing query in vChannelStage...",
				zap.Any("collectionID", q.collectionID),
				zap.Any("msgID", msg.ID()),
				zap.Any("msgType", msgType),
			)

			switch msgType {
			case commonpb.MsgType_Retrieve:
				rm := msg.(*retrieveMsg)
				segmentRetrieved, res, err := q.retrieve(rm)
				retrieveRes := &retrieveResult{
					msg:              rm.RetrieveMsg,
					err:              err,
					segmentRetrieved: segmentRetrieved,
					res:              res,
				}
				q.queryOutput <- retrieveRes
			case commonpb.MsgType_Search:
				sm := msg.(*searchMsg)
				searchResults, matchedSegments, sealedSegmentSearched, err := q.search(sm)
				searchRes := &searchResult{
					reqs:                  sm.reqs,
					msg:                   sm,
					err:                   err,
					searchResults:         searchResults,
					matchedSegments:       matchedSegments,
					sealedSegmentSearched: sealedSegmentSearched,
				}
				q.queryOutput <- searchRes
			default:
				err := fmt.Errorf("receive invalid msgType = %d", msgType)
				log.Error(err.Error())
			}

			sp.Finish()
		}
	}
}

func (q *vChannelStage) retrieve(retrieveMsg *retrieveMsg) ([]UniqueID, []*segcorepb.RetrieveResults, error) {
	collectionID := retrieveMsg.CollectionID
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("retrieve %d", collectionID))

	defer retrieveMsg.plan.delete()

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
	sealedSegmentRetrieved := make([]UniqueID, 0)
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
			sealedSegmentRetrieved = append(sealedSegmentRetrieved, segmentID)
		}
	}
	tr.Record("streaming retrieve done")
	tr.Elapse("all done")
	return sealedSegmentRetrieved, mergeList, nil
}

func (q *vChannelStage) search(searchMsg *searchMsg) ([]*SearchResult, []*Segment, []UniqueID, error) {
	sp, ctx := trace.StartSpanFromContext(searchMsg.TraceCtx())
	defer sp.Finish()
	searchMsg.SetTraceCtx(ctx)

	travelTimestamp := searchMsg.TravelTimestamp
	collectionID := searchMsg.CollectionID

	// multiple search is not supported for now
	if len(searchMsg.reqs) != 1 {
		return nil, nil, nil, errors.New("illegal search requests, collectionID = " + fmt.Sprintln(collectionID))
	}
	queryNum := searchMsg.reqs[0].getNumOfQuery()
	topK := searchMsg.plan.getTopK()

	if searchMsg.GetDslType() == commonpb.DslType_BoolExprV1 {
		sp.LogFields(oplog.String("statistical time", "stats start"),
			oplog.Object("nq", queryNum),
			oplog.Object("expr", searchMsg.SerializedExprPlan))
	} else {
		sp.LogFields(oplog.String("statistical time", "stats start"),
			oplog.Object("nq", queryNum),
			oplog.Object("dsl", searchMsg.Dsl))
	}

	tr := timerecord.NewTimeRecorder(fmt.Sprintf("search %d(nq=%d, k=%d)", searchMsg.CollectionID, queryNum, topK))
	sealedSegmentSearched := make([]UniqueID, 0)

	// streaming search
	strSearchResults, strSegmentResults, err := q.streaming.search(searchMsg.reqs,
		collectionID,
		searchMsg.PartitionIDs,
		q.vChannel,
		searchMsg.plan,
		travelTimestamp)

	if err != nil {
		log.Error(err.Error())
		return nil, nil, nil, err
	}

	for _, seg := range strSegmentResults {
		sealedSegmentSearched = append(sealedSegmentSearched, seg.segmentID)
	}
	tr.Record("streaming search done")
	sp.LogFields(oplog.String("statistical time", "streaming search done"))
	tr.Elapse("all done")
	return strSearchResults, strSegmentResults, sealedSegmentSearched, nil
}
