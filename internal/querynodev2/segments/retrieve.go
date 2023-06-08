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
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	. "github.com/milvus-io/milvus/pkg/util/typeutil"
)

// retrieveOnSegments performs retrieve on listed segments
// all segment ids are validated before calling this function
func retrieveOnSegments(ctx context.Context, manager *Manager, segType SegmentType, plan *RetrievePlan, segIDs []UniqueID) ([]*segcorepb.RetrieveResults, error) {
	var (
		resultCh = make(chan *segcorepb.RetrieveResults, len(segIDs))
		errs     = make([]error, len(segIDs))
		wg       sync.WaitGroup
	)

	label := metrics.SealedSegmentLabel
	if segType == commonpb.SegmentState_Growing {
		label = metrics.GrowingSegmentLabel
	}

	for i, segID := range segIDs {
		wg.Add(1)
		go func(segID int64, i int) {
			defer wg.Done()
			segment, _ := manager.Segment.GetWithType(segID, segType).(*LocalSegment)
			if segment == nil {
				errs[i] = nil
				return
			}
			tr := timerecord.NewTimeRecorder("retrieveOnSegments")
			result, err := segment.Retrieve(ctx, plan)
			if err != nil {
				errs[i] = err
				return
			}
			if err = segment.ValidateIndexedFieldsData(ctx, result); err != nil {
				errs[i] = err
				return
			}
			errs[i] = nil
			resultCh <- result
			metrics.QueryNodeSQSegmentLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()),
				metrics.QueryLabel, label).Observe(float64(tr.ElapseSpan().Milliseconds()))
		}(segID, i)
	}
	wg.Wait()
	close(resultCh)

	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}

	var retrieveResults []*segcorepb.RetrieveResults
	for result := range resultCh {
		retrieveResults = append(retrieveResults, result)
	}

	return retrieveResults, nil
}

// retrieveHistorical will retrieve all the target segments in historical
func RetrieveHistorical(ctx context.Context, manager *Manager, plan *RetrievePlan, collID UniqueID, partIDs []UniqueID, segIDs []UniqueID) ([]*segcorepb.RetrieveResults, []UniqueID, []UniqueID, error) {
	var err error
	var retrieveResults []*segcorepb.RetrieveResults
	var retrieveSegmentIDs []UniqueID
	var retrievePartIDs []UniqueID
	retrievePartIDs, retrieveSegmentIDs, err = validateOnHistorical(ctx, manager, collID, partIDs, segIDs)
	if err != nil {
		return retrieveResults, retrieveSegmentIDs, retrievePartIDs, err
	}

	retrieveResults, err = retrieveOnSegments(ctx, manager, SegmentTypeSealed, plan, retrieveSegmentIDs)
	return retrieveResults, retrievePartIDs, retrieveSegmentIDs, err
}

// retrieveStreaming will retrieve all the target segments in streaming
func RetrieveStreaming(ctx context.Context, manager *Manager, plan *RetrievePlan, collID UniqueID, partIDs []UniqueID, segIDs []UniqueID) ([]*segcorepb.RetrieveResults, []UniqueID, []UniqueID, error) {
	var err error
	var retrieveResults []*segcorepb.RetrieveResults
	var retrievePartIDs []UniqueID
	var retrieveSegmentIDs []UniqueID

	retrievePartIDs, retrieveSegmentIDs, err = validateOnStream(ctx, manager, collID, partIDs, segIDs)
	if err != nil {
		return retrieveResults, retrieveSegmentIDs, retrievePartIDs, err
	}
	retrieveResults, err = retrieveOnSegments(ctx, manager, SegmentTypeGrowing, plan, retrieveSegmentIDs)
	return retrieveResults, retrievePartIDs, retrieveSegmentIDs, err
}
