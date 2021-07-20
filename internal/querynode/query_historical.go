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
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/trace"
)

type historicalStage struct {
	ctx    context.Context
	cancel context.CancelFunc

	collectionID UniqueID

	input  chan queryMsg
	output chan queryResult

	historical *historical
}

func newHistoricalStage(ctx context.Context,
	cancel context.CancelFunc,
	collectionID UniqueID,
	input chan queryMsg,
	output chan queryResult,
	historical *historical) *historicalStage {

	return &historicalStage{
		ctx:          ctx,
		cancel:       cancel,
		collectionID: collectionID,
		input:        input,
		output:       output,
		historical:   historical,
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
					msg:              rm.RetrieveMsg,
					err:              err,
					segmentRetrieved: segmentRetrieved,
					res:              res,
				}
				q.output <- retrieveRes
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
				q.output <- searchRes
			default:
				err := fmt.Errorf("receive invalid msgType = %d", msgType)
				log.Error(err.Error())
			}

			sp.Finish()
		}
	}
}

func (q *historicalStage) retrieve(retrieveMsg *retrieveMsg) ([]UniqueID, []*segcorepb.RetrieveResults, error) {
	collectionID := retrieveMsg.CollectionID
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("retrieve %d", collectionID))

	defer retrieveMsg.plan.delete()

	var partitionIDsInHistorical []UniqueID
	partitionIDsInQuery := retrieveMsg.PartitionIDs
	if len(partitionIDsInQuery) == 0 {
		partitionIDsInHistoricalCol, err := q.historical.replica.getPartitionIDs(collectionID)
		if err != nil {
			return nil, nil, err
		}
		partitionIDsInHistorical = partitionIDsInHistoricalCol
	} else {
		for _, id := range partitionIDsInQuery {
			_, err := q.historical.replica.getPartitionByID(id)
			if err != nil {
				return nil, nil, err
			}
			partitionIDsInHistorical = append(partitionIDsInHistorical, id)
		}
	}
	sealedSegmentRetrieved := make([]UniqueID, 0)
	var mergeList []*segcorepb.RetrieveResults
	for _, partitionID := range partitionIDsInHistorical {
		segmentIDs, err := q.historical.replica.getSegmentIDs(partitionID)
		if err != nil {
			return nil, nil, err
		}
		for _, segmentID := range segmentIDs {
			segment, err := q.historical.replica.getSegmentByID(segmentID)
			if err != nil {
				return nil, nil, err
			}
			result, err := segment.getEntityByIds(retrieveMsg.plan)
			if err != nil {
				return nil, nil, err
			}
			if err = q.fillVectorFieldsData(segment, result); err != nil {
				return nil, nil, err
			}
			mergeList = append(mergeList, result)
			sealedSegmentRetrieved = append(sealedSegmentRetrieved, segmentID)
		}
	}
	tr.Record("historical retrieve done")
	tr.Elapse("all done")
	return sealedSegmentRetrieved, mergeList, nil
}

func (q *historicalStage) search(searchMsg *searchMsg) ([]*SearchResult, []*Segment, []UniqueID, error) {
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
	hisSearchResults, hisSegmentResults, err := q.historical.search(searchMsg.reqs,
		collectionID,
		searchMsg.PartitionIDs,
		searchMsg.plan,
		travelTimestamp)
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

func (q *historicalStage) fillVectorFieldsData(segment *Segment, result *segcorepb.RetrieveResults) error {
	for _, resultFieldData := range result.FieldsData {
		vecFieldInfo, err := segment.getVectorFieldInfo(resultFieldData.FieldId)
		if err != nil {
			continue
		}

		// if vector raw data is in memory, result should has been filled in valid vector raw data
		if vecFieldInfo.getRawDataInMemory() {
			continue
		}

		// load vector field data
		if err = q.historical.loader.loadSegmentVectorFieldData(vecFieldInfo); err != nil {
			return err
		}

		for i, offset := range result.Offset {
			var success bool
			for _, path := range vecFieldInfo.fieldBinlog.Binlogs {
				rawData := vecFieldInfo.getRawData(path)

				var numRows, dim int64
				switch fieldData := rawData.(type) {
				case *storage.FloatVectorFieldData:
					numRows = int64(fieldData.NumRows)
					dim = int64(fieldData.Dim)
					if offset < numRows {
						copy(resultFieldData.GetVectors().GetFloatVector().Data[int64(i)*dim:int64(i+1)*dim], fieldData.Data[offset*dim:(offset+1)*dim])
						success = true
					} else {
						offset -= numRows
					}
				case *storage.BinaryVectorFieldData:
					numRows = int64(fieldData.NumRows)
					dim = int64(fieldData.Dim)
					if offset < numRows {
						x := resultFieldData.GetVectors().GetData().(*schemapb.VectorField_BinaryVector)
						copy(x.BinaryVector[int64(i)*dim/8:int64(i+1)*dim/8], fieldData.Data[offset*dim/8:(offset+1)*dim/8])
						success = true
					} else {
						offset -= numRows
					}
				default:
					return fmt.Errorf("unexpected field data type")
				}
				if success {
					break
				}
			}
		}
	}
	return nil
}
