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
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
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

	historical         *historical
	localChunkManager  storage.ChunkManager
	remoteChunkManager storage.ChunkManager
	vectorChunkManager storage.ChunkManager
	localCacheEnabled  bool
}

func newHistoricalStage(ctx context.Context,
	collectionID UniqueID,
	input chan queryMsg,
	output chan queryResult,
	historical *historical,
	localChunkManager storage.ChunkManager,
	remoteChunkManager storage.ChunkManager,
	localCacheEnabled bool) *historicalStage {

	return &historicalStage{
		ctx:                ctx,
		collectionID:       collectionID,
		input:              input,
		output:             output,
		historical:         historical,
		localChunkManager:  localChunkManager,
		remoteChunkManager: remoteChunkManager,
		localCacheEnabled:  localCacheEnabled,
	}
}

func (q *historicalStage) start() {
	for {
		select {
		case <-q.ctx.Done():
			log.Debug("stop historicalStage", zap.Int64("collectionID", q.collectionID))
			return
		case msg := <-q.input:
			msgType := msg.Type()
			sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
			msg.SetTraceCtx(ctx)
			log.Debug("doing query in historicalStage...",
				zap.Any("collectionID", q.collectionID),
				zap.Any("msgID", msg.ID()),
				zap.Any("msgType", msgType),
			)
			switch msgType {
			case commonpb.MsgType_Retrieve:
				rm := msg.(*retrieveMsg)
				segmentRetrieved, res, err := q.retrieve(rm)
				retrieveRes := &retrieveResult{
					msg:              rm,
					err:              err,
					segmentRetrieved: segmentRetrieved,
					res:              res,
				}
				if err != nil {
					log.Warn("retrieve error in historicalStage",
						zap.Any("collectionID", q.collectionID),
						zap.Any("msgID", msg.ID()),
						zap.Error(err),
					)
				}
				q.output <- retrieveRes
			case commonpb.MsgType_Search:
				sm := msg.(*searchMsg)
				searchResults, sealedSegmentSearched, err := q.search(sm)
				searchRes := &searchResult{
					reqs:                  sm.reqs,
					msg:                   sm,
					err:                   err,
					searchResults:         searchResults,
					sealedSegmentSearched: sealedSegmentSearched,
				}
				if err != nil {
					log.Warn("search error in historicalStage",
						zap.Any("collectionID", q.collectionID),
						zap.Any("msgID", msg.ID()),
						zap.Error(err),
					)
				}
				q.output <- searchRes
			default:
				err := fmt.Errorf("receive invalid msgType = %d", msgType)
				log.Warn(err.Error())
			}

			sp.Finish()
		}
	}
}

func (q *historicalStage) retrieve(retrieveMsg *retrieveMsg) ([]UniqueID, []*segcorepb.RetrieveResults, error) {
	collectionID := retrieveMsg.CollectionID
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("retrieve %d", collectionID))

	collection, err := q.historical.replica.getCollectionByID(collectionID)
	if err != nil {
		return nil, nil, err
	}

	var mergeList []*segcorepb.RetrieveResults

	if q.vectorChunkManager == nil {
		if q.localChunkManager == nil {
			return nil, nil, fmt.Errorf("can not create vector chunk manager for local chunk manager is nil")
		}
		if q.remoteChunkManager == nil {
			return nil, nil, fmt.Errorf("can not create vector chunk manager for remote chunk manager is nil")
		}
		q.vectorChunkManager = storage.NewVectorChunkManager(q.localChunkManager, q.remoteChunkManager,
			&etcdpb.CollectionMeta{
				ID:     collection.id,
				Schema: collection.schema,
			}, q.localCacheEnabled)
	}

	hisRetrieveResults, sealedSegmentRetrieved, err1 := q.historical.retrieve(collectionID,
		retrieveMsg.PartitionIDs,
		q.vectorChunkManager,
		retrieveMsg.plan)
	if err1 != nil {
		log.Warn(err1.Error())
		return nil, nil, err1
	}
	mergeList = append(mergeList, hisRetrieveResults...)
	tr.Record("historical retrieve done")
	tr.Elapse("all done")
	return sealedSegmentRetrieved, mergeList, nil
}

func (q *historicalStage) search(searchMsg *searchMsg) ([]*SearchResult, []UniqueID, error) {
	sp, ctx := trace.StartSpanFromContext(searchMsg.TraceCtx())
	defer sp.Finish()
	searchMsg.SetTraceCtx(ctx)

	travelTimestamp := searchMsg.TravelTimestamp
	collectionID := searchMsg.CollectionID

	// multiple search is not supported for now
	if len(searchMsg.reqs) != 1 {
		return nil, nil, errors.New("illegal search requests, collectionID = " + fmt.Sprintln(collectionID))
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

	// historical search
	hisSearchResults, sealedSegmentSearched, err := q.historical.search(searchMsg.reqs,
		collectionID,
		searchMsg.PartitionIDs,
		searchMsg.plan,
		travelTimestamp)
	if err != nil {
		log.Warn(err.Error())
		return nil, nil, err
	}

	tr.Record("historical search done")
	sp.LogFields(oplog.String("statistical time", "historical search done"))
	tr.Elapse("all done")
	return hisSearchResults, sealedSegmentSearched, nil
}
