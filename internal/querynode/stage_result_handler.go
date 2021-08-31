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
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/golang/protobuf/proto"
	oplog "github.com/opentracing/opentracing-go/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type resultHandlerStage struct {
	ctx context.Context

	collectionID UniqueID

	streaming  *streaming
	historical *historical

	queryResultStream msgstream.MsgStream
	input             chan queryResult

	results map[UniqueID][]queryResult // map[msgID]queryResults
}

func newResultHandlerStage(ctx context.Context,
	collectionID UniqueID,
	streaming *streaming,
	historical *historical,
	input chan queryResult,
	queryResultStream msgstream.MsgStream) *resultHandlerStage {

	return &resultHandlerStage{
		ctx:               ctx,
		collectionID:      collectionID,
		streaming:         streaming,
		historical:        historical,
		queryResultStream: queryResultStream,
		input:             input,
		results:           make(map[UniqueID][]queryResult),
	}
}

func (q *resultHandlerStage) start() {
	log.Debug("starting resultHandlerStage...",
		zap.Any("collectionID", q.collectionID),
	)
	for {
		select {
		case <-q.ctx.Done():
			log.Debug("stop resultHandlerStage", zap.Int64("collectionID", q.collectionID))
			return
		case msg := <-q.input:
			log.Debug("receive result",
				zap.Any("collectionID", q.collectionID),
				zap.Any("msgID", msg.ID()),
			)
			if _, ok := q.results[msg.ID()]; !ok {
				q.results[msg.ID()] = make([]queryResult, 0)
			}
			q.results[msg.ID()] = append(q.results[msg.ID()], msg)
			for k, v := range q.results {
				channelNum := v[0].ChannelNum()
				// `channelNum + 1` means `vChannels + historical`
				if len(v) == channelNum+1 {
					log.Debug("prepare to reduce",
						zap.Any("collectionID", q.collectionID),
						zap.Any("msgID", msg.ID()),
						zap.Any("channelNum", channelNum),
					)
					// do reduce
					msgType := v[0].Type()
					switch msgType {
					case commonpb.MsgType_Retrieve:
						q.reduceRetrieve(k)
					case commonpb.MsgType_Search:
						q.reduceSearch(k)
					default:
						err := fmt.Errorf("resultHandlerStage receive invalid msgType = %d", msgType)
						log.Warn(err.Error())
					}
					delete(q.results, msg.ID())
				}
			}
		}
	}
}

func (q *resultHandlerStage) reduceRetrieve(msgID UniqueID) {
	msg := q.results[msgID][0].(*retrieveResult).msg
	collectionID := msg.CollectionID

	log.Debug("reducing Retrieve result...",
		zap.Any("collectionID", msg.CollectionID),
		zap.Any("msgID", msg.ID()),
	)

	// error check
	for _, res := range q.results[msgID] {
		if err := res.(*retrieveResult).err; err != nil {
			publishFailedQueryResult(msg.RetrieveMsg, err.Error(), q.queryResultStream)
			log.Debug("do retrieve failed in resultHandlerStage, publish failed query result",
				zap.Int64("collectionID", q.collectionID),
				zap.Int64("msgID", msg.ID()),
				zap.String("err", err.Error()),
			)
			return
		}
	}

	// get global sealed segments
	var globalSealedSegments []UniqueID
	partitionIDsInQuery := msg.PartitionIDs
	if len(partitionIDsInQuery) == 0 {
		globalSealedSegments = q.historical.getGlobalSegmentIDsByCollectionID(collectionID)
	} else {
		globalSealedSegments = q.historical.getGlobalSegmentIDsByPartitionIds(partitionIDsInQuery)
	}

	segmentRetrieved := make([]UniqueID, 0)
	mergeList := make([]*segcorepb.RetrieveResults, 0)
	channelsRetrieved := make([]Channel, 0)
	for _, res := range q.results[msgID] {
		retrieveRes, ok := res.(*retrieveResult)
		if !ok {
			log.Warn("invalid retrieve result",
				zap.Any("collectionID", q.collectionID),
				zap.Any("msgID", msgID),
			)
			return
		}
		segmentRetrieved = append(segmentRetrieved, retrieveRes.segmentRetrieved...)
		mergeList = append(mergeList, retrieveRes.res...)
		if channel := retrieveRes.vChannel; channel != "" {
			channelsRetrieved = append(channelsRetrieved, channel)
		}
	}

	result, err := q.mergeRetrieveResults(mergeList)
	if err != nil {
		log.Warn(err.Error())
		return
	}

	resultChannelInt := 0
	retrieveResultMsg := &msgstream.RetrieveResultMsg{
		BaseMsg: msgstream.BaseMsg{
			Ctx:        msg.Ctx,
			HashValues: []uint32{uint32(resultChannelInt)},
		},
		RetrieveResults: internalpb.RetrieveResults{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_RetrieveResult,
				MsgID:    msg.Base.MsgID,
				SourceID: msg.Base.SourceID,
			},
			Status:                    &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			Ids:                       result.Ids,
			FieldsData:                result.FieldsData,
			ResultChannelID:           msg.ResultChannelID,
			SealedSegmentIDsRetrieved: segmentRetrieved,
			ChannelIDsRetrieved:       channelsRetrieved,
			GlobalSealedSegmentIDs:    globalSealedSegments,
		},
	}

	log.Debug("delete plan",
		zap.Any("collectionID", msg.CollectionID),
		zap.Any("msgID", msg.ID()),
		zap.Any("vChannels", channelsRetrieved),
	)
	msg.plan.delete()

	publishQueryResult(retrieveResultMsg, q.queryResultStream)
	log.Debug("QueryNode publish RetrieveResultMsg",
		zap.Any("collectionID", msg.CollectionID),
		zap.Any("msgID", msg.ID()),
		zap.Any("vChannels", channelsRetrieved),
		zap.Any("sealedSegmentRetrieved", segmentRetrieved),
	)
}

func (q *resultHandlerStage) mergeRetrieveResults(dataArr []*segcorepb.RetrieveResults) (*segcorepb.RetrieveResults, error) {
	var final *segcorepb.RetrieveResults
	for _, data := range dataArr {
		// skip empty result, it will break merge result
		if data == nil || len(data.Offset) == 0 {
			continue
		}

		if final == nil {
			final = proto.Clone(data).(*segcorepb.RetrieveResults)
			continue
		}

		proto.Merge(final.Ids, data.Ids)
		if len(final.FieldsData) != len(data.FieldsData) {
			return nil, fmt.Errorf("mismatch FieldData in RetrieveResults")
		}

		for i := range final.FieldsData {
			proto.Merge(final.FieldsData[i], data.FieldsData[i])
		}
	}

	// not found, return default values indicating not result found
	if final == nil {
		final = &segcorepb.RetrieveResults{
			Ids:        nil,
			FieldsData: []*schemapb.FieldData{},
		}
	}

	return final, nil
}

func (q *resultHandlerStage) reduceSearch(msgID UniqueID) {
	log.Debug("reducing search result...",
		zap.Any("collectionID", q.collectionID),
		zap.Any("msgID", msgID),
	)

	sr, ok := q.results[msgID][0].(*searchResult)
	if !ok {
		log.Warn("type assertion failed for searchResult",
			zap.Any("msgID", msgID),
		)
		return
	}
	msg := sr.msg
	plan := msg.plan
	topK := plan.getTopK()
	searchRequests := sr.reqs

	sp, ctx := trace.StartSpanFromContext(msg.TraceCtx())
	defer sp.Finish()
	msg.SetTraceCtx(ctx)
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("search reduce %d", msg.CollectionID))

	searchTimestamp := msg.BeginTs()

	// error check
	for _, res := range q.results[msgID] {
		if err := res.(*searchResult).err; err != nil {
			publishFailedQueryResult(msg.SearchMsg, err.Error(), q.queryResultStream)
			log.Debug("do search failed in resultHandlerStage, publish failed query result",
				zap.Int64("collectionID", q.collectionID),
				zap.Int64("msgID", msg.ID()),
				zap.String("err", err.Error()),
			)
			return
		}
	}

	// get global sealed segments
	var globalSealedSegments []UniqueID
	if len(msg.PartitionIDs) > 0 {
		globalSealedSegments = q.historical.getGlobalSegmentIDsByPartitionIds(msg.PartitionIDs)
	} else {
		globalSealedSegments = q.historical.getGlobalSegmentIDsByCollectionID(q.collectionID)
	}

	searchResults := make([]*SearchResult, 0)
	sealedSegmentSearched := make([]UniqueID, 0)
	channelsSearched := make([]Channel, 0)
	defer deleteSearchResults(searchResults)

	// append all results
	for _, res := range q.results[msgID] {
		searchRes, ok := res.(*searchResult)
		if !ok {
			log.Warn("invalid search result",
				zap.Any("collectionID", q.collectionID),
				zap.Any("msgID", msgID),
			)
			return
		}
		searchResults = append(searchResults, searchRes.searchResults...)
		sealedSegmentSearched = append(sealedSegmentSearched, searchRes.sealedSegmentSearched...)
		if channel := searchRes.vChannel; channel != "" {
			channelsSearched = append(channelsSearched, searchRes.vChannel)
		}
	}

	log.Debug("check SealedSegments",
		zap.Any("collectionID", q.collectionID),
		zap.Any("msgID", msgID),
		zap.Any("globalSealedSegments", globalSealedSegments),
		zap.Any("sealedSegmentSearched", sealedSegmentSearched),
	)

	// get schema
	collectionID := msg.CollectionID
	collection, err := q.streaming.replica.getCollectionByID(collectionID)
	if err != nil {
		log.Warn(err.Error())
		return
	}
	schema, err := typeutil.CreateSchemaHelper(collection.schema)
	if err != nil {
		log.Warn(err.Error())
		return
	}

	log.Debug("search result length",
		zap.Any("collectionID", q.collectionID),
		zap.Any("msgID", msgID),
		zap.Any("length", len(searchResults)),
	)
	if len(searchResults) <= 0 {
		for _, searchReq := range searchRequests {
			queryNum := searchReq.getNumOfQuery()

			resultChannelInt := 0
			searchResultMsg := &msgstream.SearchResultMsg{
				BaseMsg: msgstream.BaseMsg{Ctx: msg.Ctx, HashValues: []uint32{uint32(resultChannelInt)}},
				SearchResults: internalpb.SearchResults{
					Base: &commonpb.MsgBase{
						MsgType:   commonpb.MsgType_SearchResult,
						MsgID:     msg.Base.MsgID,
						Timestamp: searchTimestamp,
						SourceID:  msg.Base.SourceID,
					},
					Status:                   &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
					ResultChannelID:          msg.ResultChannelID,
					MetricType:               plan.getMetricType(),
					NumQueries:               queryNum,
					TopK:                     topK,
					SlicedBlob:               nil,
					SlicedOffset:             1,
					SlicedNumCount:           1,
					SealedSegmentIDsSearched: sealedSegmentSearched,
					ChannelIDsSearched:       channelsSearched,
					GlobalSealedSegmentIDs:   globalSealedSegments,
				},
			}
			log.Debug("QueryNode Empty SearchResultMsg",
				zap.Any("collectionID", collection.ID()),
				zap.Any("msgID", msg.ID()),
				zap.Any("vChannels", channelsSearched),
				zap.Any("sealedSegmentSearched", sealedSegmentSearched),
			)
			publishQueryResult(searchResultMsg, q.queryResultStream)
			tr.Record("publish empty search result done")
			tr.Elapse("all done")
			return
		}
	}

	numSegment := int64(len(searchResults))
	var marshaledHits *MarshaledHits = nil

	err = reduceSearchResultsAndFillData(plan, searchResults, numSegment)
	sp.LogFields(oplog.String("statistical time", "reduceSearchResults end"))
	if err != nil {
		log.Warn(err.Error())
		return
	}
	marshaledHits, err = reorganizeSearchResults(searchResults, numSegment)
	sp.LogFields(oplog.String("statistical time", "reorganizeSearchResults end"))
	if err != nil {
		log.Warn(err.Error())
		return
	}
	defer deleteMarshaledHits(marshaledHits)
	hitsBlob, err := marshaledHits.getHitsBlob()
	sp.LogFields(oplog.String("statistical time", "getHitsBlob end"))
	if err != nil {
		log.Warn(err.Error())
		return
	}
	tr.Record("reduce result done")
	log.Debug("reduce search done",
		zap.Any("collectionID", q.collectionID),
		zap.Any("msgID", msgID),
	)

	var offset int64 = 0
	for index, searchReq := range searchRequests {
		queryNum := searchReq.getNumOfQuery()
		hitBlobSizePeerQuery, err := marshaledHits.hitBlobSizeInGroup(int64(index))
		if err != nil {
			log.Warn(err.Error())
			return
		}
		hits := make([][]byte, len(hitBlobSizePeerQuery))
		for i, l := range hitBlobSizePeerQuery {
			hits[i] = hitsBlob[offset : offset+l]
			//test code to checkout marshaled hits
			//marshaledHit := hitsBlob[offset:offset+l]
			//unMarshaledHit := milvuspb.Hits{}
			//err = proto.Unmarshal(marshaledHit, &unMarshaledHit)
			//if err != nil {
			//	return err
			//}
			//log.Debug("hits msg  = ", unMarshaledHit)
			offset += l
		}

		// TODO: remove inefficient code in cgo and use SearchResultData directly
		// TODO: Currently add a translate layer from hits to SearchResultData
		// TODO: hits marshal and unmarshal is likely bottleneck

		transformed, err := q.translateHits(schema, msg.OutputFieldsId, hits)
		if err != nil {
			log.Warn(err.Error())
			return
		}
		byteBlobs, err := proto.Marshal(transformed)
		if err != nil {
			log.Warn(err.Error())
			return
		}

		resultChannelInt := 0
		searchResultMsg := &msgstream.SearchResultMsg{
			BaseMsg: msgstream.BaseMsg{Ctx: msg.Ctx, HashValues: []uint32{uint32(resultChannelInt)}},
			SearchResults: internalpb.SearchResults{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_SearchResult,
					MsgID:     msg.Base.MsgID,
					Timestamp: searchTimestamp,
					SourceID:  msg.Base.SourceID,
				},
				Status:                   &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
				ResultChannelID:          msg.ResultChannelID,
				MetricType:               plan.getMetricType(),
				NumQueries:               queryNum,
				TopK:                     topK,
				SlicedBlob:               byteBlobs,
				SlicedOffset:             1,
				SlicedNumCount:           1,
				SealedSegmentIDsSearched: sealedSegmentSearched,
				ChannelIDsSearched:       channelsSearched,
				GlobalSealedSegmentIDs:   globalSealedSegments,
			},
		}
		log.Debug("QueryNode SearchResultMsg",
			zap.Any("collectionID", collection.ID()),
			zap.Any("msgID", msg.ID()),
			zap.Any("vChannels", channelsSearched),
			zap.Any("sealedSegmentSearched", sealedSegmentSearched),
		)

		// For debugging, please don't delete.
		//fmt.Println("==================== search result ======================")
		//for i := 0; i < l(hits); i++ {
		//	testHits := milvuspb.Hits{}
		//	err := proto.Unmarshal(hits[i], &testHits)
		//	if err != nil {
		//		panic(err)
		//	}
		//	fmt.Println(testHits.IDs)
		//	fmt.Println(testHits.Scores)
		//}
		publishQueryResult(searchResultMsg, q.queryResultStream)
		tr.Record("publish search result")
	}

	plan.delete()
	for _, r := range msg.reqs {
		r.delete()
	}

	sp.LogFields(oplog.String("statistical time", "before free c++ memory"))
	sp.LogFields(oplog.String("statistical time", "stats done"))
	tr.Elapse("all done")
}

func (q *resultHandlerStage) translateHits(schema *typeutil.SchemaHelper,
	fieldIDs []int64,
	rawHits [][]byte) (*schemapb.SearchResultData, error) {

	if len(rawHits) == 0 {
		return nil, fmt.Errorf("empty results")
	}

	var hits []*milvuspb.Hits
	for _, rawHit := range rawHits {
		var hit milvuspb.Hits
		err := proto.Unmarshal(rawHit, &hit)
		if err != nil {
			return nil, err
		}
		hits = append(hits, &hit)
	}

	blobOffset := 0
	// skip id
	numQueries := len(rawHits)
	pbHits := &milvuspb.Hits{}
	err := proto.Unmarshal(rawHits[0], pbHits)
	if err != nil {
		return nil, err
	}
	topK := len(pbHits.IDs)

	blobOffset += 8
	var ids []int64
	var scores []float32
	for _, hit := range hits {
		ids = append(ids, hit.IDs...)
		scores = append(scores, hit.Scores...)
	}

	finalResult := &schemapb.SearchResultData{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: ids,
				},
			},
		},
		Scores:     scores,
		TopK:       int64(topK),
		NumQueries: int64(numQueries),
	}

	for _, fieldID := range fieldIDs {
		fieldMeta, err := schema.GetFieldFromID(fieldID)
		if err != nil {
			return nil, err
		}
		switch fieldMeta.DataType {
		case schemapb.DataType_Bool:
			blobLen := 1
			var colData []bool
			for _, hit := range hits {
				for _, row := range hit.RowData {
					dataBlob := row[blobOffset : blobOffset+blobLen]
					data := dataBlob[0]
					colData = append(colData, data != 0)
				}
			}
			newCol := &schemapb.FieldData{
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BoolData{
							BoolData: &schemapb.BoolArray{
								Data: colData,
							},
						},
					},
				},
			}
			finalResult.FieldsData = append(finalResult.FieldsData, newCol)
			blobOffset += blobLen
		case schemapb.DataType_Int8:
			blobLen := 1
			var colData []int32
			for _, hit := range hits {
				for _, row := range hit.RowData {
					dataBlob := row[blobOffset : blobOffset+blobLen]
					data := int32(dataBlob[0])
					colData = append(colData, data)
				}
			}
			newCol := &schemapb.FieldData{
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: colData,
							},
						},
					},
				},
			}
			finalResult.FieldsData = append(finalResult.FieldsData, newCol)
			blobOffset += blobLen
		case schemapb.DataType_Int16:
			blobLen := 2
			var colData []int32
			for _, hit := range hits {
				for _, row := range hit.RowData {
					dataBlob := row[blobOffset : blobOffset+blobLen]
					data := int32(int16(binary.LittleEndian.Uint16(dataBlob)))
					colData = append(colData, data)
				}
			}
			newCol := &schemapb.FieldData{
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: colData,
							},
						},
					},
				},
			}
			finalResult.FieldsData = append(finalResult.FieldsData, newCol)
			blobOffset += blobLen
		case schemapb.DataType_Int32:
			blobLen := 4
			var colData []int32
			for _, hit := range hits {
				for _, row := range hit.RowData {
					dataBlob := row[blobOffset : blobOffset+blobLen]
					data := int32(binary.LittleEndian.Uint32(dataBlob))
					colData = append(colData, data)
				}
			}
			newCol := &schemapb.FieldData{
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: colData,
							},
						},
					},
				},
			}
			finalResult.FieldsData = append(finalResult.FieldsData, newCol)
			blobOffset += blobLen
		case schemapb.DataType_Int64:
			blobLen := 8
			var colData []int64
			for _, hit := range hits {
				for _, row := range hit.RowData {
					dataBlob := row[blobOffset : blobOffset+blobLen]
					data := int64(binary.LittleEndian.Uint64(dataBlob))
					colData = append(colData, data)
				}
			}
			newCol := &schemapb.FieldData{
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: colData,
							},
						},
					},
				},
			}
			finalResult.FieldsData = append(finalResult.FieldsData, newCol)
			blobOffset += blobLen
		case schemapb.DataType_Float:
			blobLen := 4
			var colData []float32
			for _, hit := range hits {
				for _, row := range hit.RowData {
					dataBlob := row[blobOffset : blobOffset+blobLen]
					data := math.Float32frombits(binary.LittleEndian.Uint32(dataBlob))
					colData = append(colData, data)
				}
			}
			newCol := &schemapb.FieldData{
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_FloatData{
							FloatData: &schemapb.FloatArray{
								Data: colData,
							},
						},
					},
				},
			}
			finalResult.FieldsData = append(finalResult.FieldsData, newCol)
			blobOffset += blobLen
		case schemapb.DataType_Double:
			blobLen := 8
			var colData []float64
			for _, hit := range hits {
				for _, row := range hit.RowData {
					dataBlob := row[blobOffset : blobOffset+blobLen]
					data := math.Float64frombits(binary.LittleEndian.Uint64(dataBlob))
					colData = append(colData, data)
				}
			}
			newCol := &schemapb.FieldData{
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DoubleData{
							DoubleData: &schemapb.DoubleArray{
								Data: colData,
							},
						},
					},
				},
			}
			finalResult.FieldsData = append(finalResult.FieldsData, newCol)
			blobOffset += blobLen
		default:
			return nil, errors.New(fmt.Sprintln("unsupported data type when translateHits, dateType = ", fieldMeta.DataType))
		}
	}
	return finalResult, nil
}
