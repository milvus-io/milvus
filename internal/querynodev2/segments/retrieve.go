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

package segments

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

// retrieveOnSegments performs retrieve on listed segments
// all segment ids are validated before calling this function
func retrieveOnSegments(ctx context.Context, segments []Segment, segType SegmentType, plan *RetrievePlan) ([]*segcorepb.RetrieveResults, error) {
	label := metrics.SealedSegmentLabel
	if segType == commonpb.SegmentState_Growing {
		label = metrics.GrowingSegmentLabel
	}

	tr := timerecord.NewTimeRecorder("retrieveOnSegments")
	retrieveFutures := make([]*conc.Future[any], 0, len(segments))

	for _, segment := range segments {
		future := segment.Retrieve(ctx, plan)
		retrieveFutures = append(retrieveFutures, future)
	}

	var errorList error
	var retrieveResults []*segcorepb.RetrieveResults
	for _, future := range retrieveFutures {
		result, err := future.Await()
		if err != nil {
			// can not early out because we have to handle search result here
			errorList = merr.Combine(errorList, err)
			continue
		}
		retrieveResults = append(retrieveResults, result.(*segcorepb.RetrieveResults))
	}

	// TODO we should add failed count for retrieve results.

	// retrieve doesn't need to free result cause it does extra copy in future.
	// TODO we should defer the copy after reduce so this could save extra copy
	if errorList != nil {
		metrics.QueryNodeSQFailCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
			metrics.QueryLabel, label).Inc()
		return nil, errorList
	}

	metrics.QueryNodeSQSegmentLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
		metrics.QueryLabel, label).Observe(float64(tr.ElapseSpan().Milliseconds()))

	return retrieveResults, nil
}

func retrieveOnSegmentsWithStream(ctx context.Context, segments []Segment, segType SegmentType, plan *RetrievePlan, svr streamrpc.QueryStreamServer) error {
	label := metrics.SealedSegmentLabel
	if segType == commonpb.SegmentState_Growing {
		label = metrics.GrowingSegmentLabel
	}

	tr := timerecord.NewTimeRecorder("retrieveOnSegmentsWithStream")
	var errorList error
	retrieveFutures := make([]*conc.Future[any], 0, len(segments))
	for _, segment := range segments {
		future := segment.Retrieve(ctx, plan)
		retrieveFutures = append(retrieveFutures, future)
	}

	for _, future := range retrieveFutures {
		result, err := future.Await()
		if err != nil {
			// can not early out because we have to handle search result here
			errorList = merr.Combine(errorList, err)
			continue
		}

		retrieveResult := result.(*segcorepb.RetrieveResults)
		err = svr.Send(&internalpb.RetrieveResults{
			Status:     merr.Success(),
			Ids:        retrieveResult.GetIds(),
			FieldsData: retrieveResult.GetFieldsData(),
		})

		if err != nil {
			errorList = merr.Combine(errorList, err)
		}
	}

	// retrieve doesn't need to free result cause it does extra copy in future.
	// TODO we should defer the copy after reduce so this could save extra copy
	if errorList != nil {
		metrics.QueryNodeSQFailCount.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
			metrics.QueryLabelStreaming, label).Inc()
		return errorList
	}

	metrics.QueryNodeSQSegmentLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
		metrics.QueryLabelStreaming, label).Observe(float64(tr.ElapseSpan().Milliseconds()))

	return nil
}

// retrieve will retrieve all the validate target segments
func Retrieve(ctx context.Context, manager *Manager, plan *RetrievePlan, req *querypb.QueryRequest) ([]*segcorepb.RetrieveResults, []Segment, error) {
	var err error
	var SegType commonpb.SegmentState
	var retrieveResults []*segcorepb.RetrieveResults
	var retrieveSegments []Segment

	segIDs := req.GetSegmentIDs()
	collID := req.Req.GetCollectionID()

	if req.GetScope() == querypb.DataScope_Historical {
		SegType = SegmentTypeSealed
		retrieveSegments, err = validateOnHistorical(ctx, manager, collID, nil, segIDs)
	} else {
		SegType = SegmentTypeGrowing
		retrieveSegments, err = validateOnStream(ctx, manager, collID, nil, segIDs)
	}

	if err != nil {
		return retrieveResults, retrieveSegments, err
	}

	retrieveResults, err = retrieveOnSegments(ctx, retrieveSegments, SegType, plan)
	return retrieveResults, retrieveSegments, err
}

// retrieveStreaming will retrieve all the validate target segments  and  return by stream
func RetrieveStream(ctx context.Context, manager *Manager, plan *RetrievePlan, req *querypb.QueryRequest, srv streamrpc.QueryStreamServer) ([]Segment, error) {
	var err error
	var SegType commonpb.SegmentState
	var retrieveSegments []Segment

	segIDs := req.GetSegmentIDs()
	collID := req.Req.GetCollectionID()

	if req.GetScope() == querypb.DataScope_Historical {
		SegType = SegmentTypeSealed
		retrieveSegments, err = validateOnHistorical(ctx, manager, collID, nil, segIDs)
	} else {
		SegType = SegmentTypeGrowing
		retrieveSegments, err = validateOnStream(ctx, manager, collID, nil, segIDs)
	}

	if err != nil {
		return retrieveSegments, err
	}

	err = retrieveOnSegmentsWithStream(ctx, retrieveSegments, SegType, plan, srv)
	return retrieveSegments, err
}
