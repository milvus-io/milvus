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
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/trace"
)

type historicalStage struct {
	ctx context.Context

	collectionID UniqueID

	input  chan queryMsg
	output chan queryResult

	historical *historical
	vcm        *storage.VectorChunkManager
}

func newHistoricalStage(ctx context.Context,
	collectionID UniqueID,
	input chan queryMsg,
	output chan queryResult,
	historical *historical,
	vcm *storage.VectorChunkManager) *historicalStage {

	return &historicalStage{
		ctx:          ctx,
		collectionID: collectionID,
		input:        input,
		output:       output,
		historical:   historical,
		vcm:          vcm,
	}
}

func (hs *historicalStage) start() {
	for {
		select {
		case <-hs.ctx.Done():
			log.Debug("stop historicalStage", zap.Int64("collectionID", hs.collectionID))
			return
		case msg := <-hs.input:
			msgType := msg.Type()
			sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
			msg.SetTraceCtx(ctx)
			log.Debug("doing query in historicalStage...",
				zap.Any("collectionID", hs.collectionID),
				zap.Any("msgID", msg.ID()),
				zap.Any("msgType", msgType),
			)
			switch msgType {
			case commonpb.MsgType_Retrieve:
				rm := msg.(*retrieveMsg)
				retrieveResults, segmentRetrievedIDs, err := hs.retrieve(rm)
				retrieveRes := &retrieveResult{
					msg:              rm,
					err:              err,
					segmentRetrieved: segmentRetrievedIDs,
					res:              retrieveResults,
				}
				hs.output <- retrieveRes
			case commonpb.MsgType_Search:
				sm := msg.(*searchMsg)
				searchResults, matchedSegments, sealedSegmentSearched, err := hs.search(sm)
				searchRes := &searchResult{
					reqs:                  sm.reqs,
					msg:                   sm,
					err:                   err,
					searchResults:         searchResults,
					matchedSegments:       matchedSegments,
					sealedSegmentSearched: sealedSegmentSearched,
				}
				hs.output <- searchRes
			default:
				err := fmt.Errorf("receive invalid msgType = %d", msgType)
				log.Error(err.Error())
			}

			sp.Finish()
		}
	}
}

func (hs *historicalStage) retrieve(retrieveMsg *retrieveMsg) ([]*segcorepb.RetrieveResults, []UniqueID, error) {
	collectionID := retrieveMsg.CollectionID
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("retrieve %d", collectionID))

	fetchResults, segmentResultIDs, err :=
		hs.historical.fetch(collectionID, retrieveMsg.PartitionIDs, hs.vcm, retrieveMsg.plan)
	if err != nil {
		log.Error(err.Error())
		return nil, nil, err
	}
	tr.Record("historical retrieve done")
	tr.Elapse("all done")
	return fetchResults, segmentResultIDs, nil
}

func (hs *historicalStage) search(searchMsg *searchMsg) ([]*SearchResult, []*Segment, []UniqueID, error) {
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

	// historical search
	hisSearchResults, hisSegmentResults, err :=
		hs.historical.search(searchMsg.reqs, collectionID, searchMsg.PartitionIDs, searchMsg.plan, travelTimestamp)
	if err != nil {
		log.Error(err.Error())
		return nil, nil, nil, err
	}

	for _, seg := range hisSegmentResults {
		sealedSegmentSearched = append(sealedSegmentSearched, seg.segmentID)
	}
	tr.Record("historical search done")
	sp.LogFields(oplog.String("statistical time", "historical search done"))
	tr.Elapse("all done")
	return hisSearchResults, hisSegmentResults, sealedSegmentSearched, nil
}
