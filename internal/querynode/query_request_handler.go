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
	"fmt"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/trace"
	oplog "github.com/opentracing/opentracing-go/log"
	"go.uber.org/zap"
)

type queryRequestHandler struct {
	ctx    context.Context
	cancel context.CancelFunc

	collectionID UniqueID

	input   chan queryMsg
	outputs []chan queryMsg

	streaming         *streaming
	historical        *historical
	queryResultStream msgstream.MsgStream
}

func newQueryRequestHandler(ctx context.Context,
	cancel context.CancelFunc,
	collectionID UniqueID,
	outputs []chan queryMsg,
	streaming *streaming,
	historical *historical,
	queryResultStream msgstream.MsgStream) *queryRequestHandler {

	return &queryRequestHandler{
		ctx:               ctx,
		cancel:            cancel,
		collectionID:      collectionID,
		input:             make(chan queryMsg, queryBufferSize),
		outputs:           outputs,
		streaming:         streaming,
		historical:        historical,
		queryResultStream: queryResultStream,
	}
}

func (q *queryRequestHandler) start() {
	for {
		select {
		case <-q.ctx.Done():
			log.Debug("stop queryRequestHandler", zap.Int64("collectionID", q.collectionID))
			return
		case msg := <-q.input:
			msgType := msg.Type()
			var collectionID UniqueID
			var msgTypeStr string

			switch msgType {
			case commonpb.MsgType_Retrieve:
				collectionID = msg.(*msgstream.RetrieveMsg).CollectionID
				msgTypeStr = "retrieve"
				log.Debug("consume retrieve message",
					zap.Any("collectionID", collectionID),
					zap.Int64("msgID", msg.ID()),
				)
			case commonpb.MsgType_Search:
				collectionID = msg.(*msgstream.SearchMsg).CollectionID
				msgTypeStr = "search"
				log.Debug("consume search message",
					zap.Any("collectionID", collectionID),
					zap.Int64("msgID", msg.ID()),
				)
			default:
				err := fmt.Errorf("receive invalid msgType = %d", msgType)
				log.Error(err.Error())
				return
			}
			if collectionID != q.collectionID {
				log.Error("not target collection query request",
					zap.Any("collectionID", q.collectionID),
					zap.Int64("target collectionID", collectionID),
					zap.Int64("msgID", msg.ID()),
				)
				return
			}

			sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
			msg.SetTraceCtx(ctx)
			tr := timerecord.NewTimeRecorder(fmt.Sprintf("receiveQueryMsg %d", msg.ID()))

			// check if collection has been released
			collection, err := q.historical.replica.getCollectionByID(collectionID)
			if err != nil {
				log.Error(err.Error())
				publishFailedQueryResult(msg, err.Error(), q.queryResultStream)
				log.Debug("do query failed in receiveQueryMsg, publish failed query result",
					zap.Int64("collectionID", collectionID),
					zap.Int64("msgID", msg.ID()),
					zap.String("msgType", msgTypeStr),
				)
				return
			}
			guaranteeTs := msg.GuaranteeTs()
			if guaranteeTs >= collection.getReleaseTime() {
				err = fmt.Errorf("retrieve failed, collection has been released, msgID = %d, collectionID = %d", msg.ID(), collectionID)
				log.Error(err.Error())
				publishFailedQueryResult(msg, err.Error(), q.queryResultStream)
				log.Debug("do query failed in receiveQueryMsg, publish failed query result",
					zap.Int64("collectionID", collectionID),
					zap.Int64("msgID", msg.ID()),
					zap.String("msgType", msgTypeStr),
				)
				return
			}

			switch msgType {
			case commonpb.MsgType_Retrieve:
				plan, err := q.parseRetrievePlan(msg)
				if err != nil {
					log.Error(err.Error())
					publishFailedQueryResult(msg, err.Error(), q.queryResultStream)
					log.Debug("parseRetrievePlan failed, publish failed query result",
						zap.Int64("collectionID", collectionID),
						zap.Int64("msgID", msg.ID()),
						zap.String("msgType", msgTypeStr),
					)
				}
				retrieveMsg := &retrieveMessage{
					RetrieveMsg: msg.(*msgstream.RetrieveMsg),
					plan:        plan,
				}
				q.sendRequests(retrieveMsg)
			case commonpb.MsgType_Search:
				plan, reqs, err := q.parseSearchPlan(msg)
				if err != nil {
					log.Error(err.Error())
					publishFailedQueryResult(msg, err.Error(), q.queryResultStream)
					log.Debug("parseSearchPlan failed, publish failed query result",
						zap.Int64("collectionID", collectionID),
						zap.Int64("msgID", msg.ID()),
						zap.String("msgType", msgTypeStr),
					)
				}
				searchMsg := &searchMessage{
					SearchMsg: msg.(*msgstream.SearchMsg),
					plan:      plan,
					reqs:      reqs,
				}
				q.sendRequests(searchMsg)
			default:
				err := fmt.Errorf("receive invalid msgType = %d", msgType)
				log.Error(err.Error())
			}
			tr.Record("operation done")

			log.Debug("do query done in receiveQueryMsg",
				zap.Int64("collectionID", collectionID),
				zap.Int64("msgID", msg.ID()),
				zap.String("msgType", msgTypeStr),
			)
			tr.Elapse("all done")
			sp.Finish()
		}
	}
}

func (q *queryRequestHandler) sendRequests(msg queryMsg) {
	for i := range q.outputs {
		q.outputs[i] <- msg
	}
	log.Debug("query request handler send requests done",
		zap.Any("collectionID", q.collectionID),
		zap.Any("msgID", msg.ID()),
	)
}

func (q *queryRequestHandler) parseSearchPlan(msg queryMsg) (*searchPlan, []*searchRequest, error) {
	searchMsg := msg.(*msgstream.SearchMsg)
	sp, ctx := trace.StartSpanFromContext(searchMsg.TraceCtx())
	defer sp.Finish()
	searchMsg.SetTraceCtx(ctx)

	collectionID := searchMsg.CollectionID
	collection, err := q.streaming.replica.getCollectionByID(collectionID)
	if err != nil {
		return nil, nil, err
	}

	var plan *searchPlan
	if searchMsg.GetDslType() == commonpb.DslType_BoolExprV1 {
		expr := searchMsg.SerializedExprPlan
		plan, err = createSearchPlanByExpr(collection, expr)
		if err != nil {
			return nil, nil, err
		}
	} else {
		dsl := searchMsg.Dsl
		plan, err = createSearchPlan(collection, dsl)
		if err != nil {
			return nil, nil, err
		}
	}
	topK := plan.getTopK()
	if topK == 0 {
		return nil, nil, fmt.Errorf("limit must be greater than 0")
	}
	if topK >= 16385 {
		return nil, nil, fmt.Errorf("limit %d is too large", topK)
	}
	searchRequestBlob := searchMsg.PlaceholderGroup
	searchReq, err := parseSearchRequest(plan, searchRequestBlob)
	if err != nil {
		return nil, nil, err
	}
	queryNum := searchReq.getNumOfQuery()
	searchRequests := make([]*searchRequest, 0)
	searchRequests = append(searchRequests, searchReq)

	if searchMsg.GetDslType() == commonpb.DslType_BoolExprV1 {
		sp.LogFields(oplog.String("statistical time", "stats start"),
			oplog.Object("nq", queryNum),
			oplog.Object("expr", searchMsg.SerializedExprPlan))
	} else {
		sp.LogFields(oplog.String("statistical time", "stats start"),
			oplog.Object("nq", queryNum),
			oplog.Object("dsl", searchMsg.Dsl))
	}

	return plan, searchRequests, nil
}

func (q *queryRequestHandler) parseRetrievePlan(msg queryMsg) (*RetrievePlan, error) {
	// TODO(yukun)
	// step 1: get retrieve object and defer destruction
	// step 2: for each segment, call retrieve to get ids proto buffer
	// step 3: merge all proto in go
	// step 4: publish results
	// retrieveProtoBlob, err := proto.Marshal(&retrieveMsg.RetrieveRequest)
	retrieveMsg := msg.(*msgstream.RetrieveMsg)
	sp, ctx := trace.StartSpanFromContext(retrieveMsg.TraceCtx())
	defer sp.Finish()
	retrieveMsg.SetTraceCtx(ctx)
	timestamp := retrieveMsg.RetrieveRequest.TravelTimestamp

	collectionID := retrieveMsg.CollectionID
	collection, err := q.streaming.replica.getCollectionByID(collectionID)
	if err != nil {
		return nil, err
	}

	req := &segcorepb.RetrieveRequest{
		Ids:          retrieveMsg.Ids,
		OutputFields: retrieveMsg.OutputFields,
	}

	plan, err := createRetrievePlan(collection, req, timestamp)
	return plan, err
}
