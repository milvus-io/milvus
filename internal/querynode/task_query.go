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
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/timerecord"
)

var _ readTask = (*queryTask)(nil)

type queryTask struct {
	baseReadTask
	iReq    *internalpb.RetrieveRequest
	req     *querypb.QueryRequest
	Ret     *internalpb.RetrieveResults
	cpuOnce sync.Once
}

func (q *queryTask) PreExecute(ctx context.Context) error {
	// check ctx timeout
	if !funcutil.CheckCtxValid(q.Ctx()) {
		return errors.New("search context timeout1$")
	}
	return nil
}

// TODO: merge queryOnStreaming and queryOnHistorical?
func (q *queryTask) queryOnStreaming() error {
	// check ctx timeout
	ctx := q.Ctx()
	if !funcutil.CheckCtxValid(q.Ctx()) {
		return errors.New("query context timeout")
	}

	// check if collection has been released, check streaming since it's released first
	_, err := q.QS.metaReplica.getCollectionByID(q.CollectionID)
	if err != nil {
		return err
	}

	q.QS.collection.RLock() // locks the collectionPtr
	defer q.QS.collection.RUnlock()
	if _, released := q.QS.collection.getReleaseTime(); released {
		log.Ctx(ctx).Debug("collection release before search", zap.Int64("msgID", q.ID()),
			zap.Int64("collectionID", q.CollectionID))
		return fmt.Errorf("retrieve failed, collection has been released, collectionID = %d", q.CollectionID)
	}

	// deserialize query plan
	plan, err := createRetrievePlanByExpr(q.QS.collection, q.iReq.GetSerializedExprPlan(), q.TravelTimestamp, q.ID())
	if err != nil {
		return err
	}
	defer plan.delete()

	sResults, _, _, sErr := retrieveStreaming(ctx, q.QS.metaReplica, plan, q.CollectionID, q.iReq.GetPartitionIDs(), q.QS.channel, q.QS.vectorChunkManager)
	if sErr != nil {
		return sErr
	}

	q.tr.RecordSpan()
	mergedResult, err := mergeSegcoreRetrieveResults(ctx, sResults)
	if err != nil {
		return err
	}

	q.Ret = &internalpb.RetrieveResults{
		Status:     &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		Ids:        mergedResult.Ids,
		FieldsData: mergedResult.FieldsData,
	}
	q.reduceDur = q.tr.RecordSpan()
	return nil
}

func (q *queryTask) queryOnHistorical() error {
	// check ctx timeout
	ctx := q.Ctx()
	if !funcutil.CheckCtxValid(ctx) {
		return errors.New("search context timeout3$")
	}

	// check if collection has been released, check historical since it's released first
	_, err := q.QS.metaReplica.getCollectionByID(q.CollectionID)
	if err != nil {
		return err
	}

	q.QS.collection.RLock() // locks the collectionPtr
	defer q.QS.collection.RUnlock()

	if _, released := q.QS.collection.getReleaseTime(); released {
		log.Ctx(ctx).Debug("collection release before search", zap.Int64("msgID", q.ID()),
			zap.Int64("collectionID", q.CollectionID))
		return fmt.Errorf("retrieve failed, collection has been released, collectionID = %d", q.CollectionID)
	}

	// deserialize query plan
	plan, err := createRetrievePlanByExpr(q.QS.collection, q.iReq.GetSerializedExprPlan(), q.TravelTimestamp, q.ID())
	if err != nil {
		return err
	}
	defer plan.delete()
	retrieveResults, _, _, err := retrieveHistorical(ctx, q.QS.metaReplica, plan, q.CollectionID, nil, q.req.SegmentIDs, q.QS.vectorChunkManager)
	if err != nil {
		return err
	}
	mergedResult, err := mergeSegcoreRetrieveResults(ctx, retrieveResults)
	if err != nil {
		return err
	}
	q.Ret = &internalpb.RetrieveResults{
		Status:     &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		Ids:        mergedResult.Ids,
		FieldsData: mergedResult.FieldsData,
	}
	return nil
}

func (q *queryTask) Execute(ctx context.Context) error {
	if q.DataScope == querypb.DataScope_Streaming {
		return q.queryOnStreaming()
	} else if q.DataScope == querypb.DataScope_Historical {
		return q.queryOnHistorical()
	}
	return fmt.Errorf("queryTask do not implement query on all data scope")
}

func (q *queryTask) estimateCPUUsage() {
	q.cpu = 10
	if q.cpu > q.maxCPU {
		q.cpu = q.maxCPU
	}
}

func (q *queryTask) CPUUsage() int32 {
	q.cpuOnce.Do(func() {
		q.estimateCPUUsage()
	})
	return q.cpu
}

func newQueryTask(ctx context.Context, src *querypb.QueryRequest) *queryTask {
	target := &queryTask{
		baseReadTask: baseReadTask{
			baseTask: baseTask{
				done: make(chan error),
				ctx:  ctx,
				id:   src.Req.Base.GetMsgID(),
				ts:   src.Req.Base.GetTimestamp(),
			},
			DbID:               src.Req.GetReqID(),
			CollectionID:       src.Req.GetCollectionID(),
			TravelTimestamp:    src.Req.GetTravelTimestamp(),
			GuaranteeTimestamp: src.Req.GetGuaranteeTimestamp(),
			TimeoutTimestamp:   src.Req.GetTimeoutTimestamp(),
			tr:                 timerecord.NewTimeRecorder("queryTask"),
			DataScope:          src.GetScope(),
		},
		iReq: src.Req,
		req:  src,
	}
	return target
}
