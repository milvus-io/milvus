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
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/proto/planpb"

	"go.uber.org/zap"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/timerecord"
)

var _ readTask = (*searchTask)(nil)

type searchTask struct {
	baseReadTask
	iReq *internalpb.SearchRequest
	req  *querypb.SearchRequest

	MetricType       string
	PlaceholderGroup []byte
	NQ               int64
	OrigNQs          []int64
	TopK             int64
	OrigTopKs        []int64
	Ret              *internalpb.SearchResults
	otherTasks       []*searchTask
	cpuOnce          sync.Once
	plan             *planpb.PlanNode
	qInfo            *planpb.QueryInfo
}

func (s *searchTask) PreExecute(ctx context.Context) error {
	s.SetStep(TaskStepPreExecute)
	for _, t := range s.otherTasks {
		t.SetStep(TaskStepPreExecute)
	}
	s.combinePlaceHolderGroups()
	return nil
}

func (s *searchTask) init() error {
	if s.iReq.GetSerializedExprPlan() != nil {
		s.plan = &planpb.PlanNode{}
		err := proto.Unmarshal(s.iReq.GetSerializedExprPlan(), s.plan)
		if err != nil {
			return err
		}
		switch s.plan.GetNode().(type) {
		case *planpb.PlanNode_VectorAnns:
			s.qInfo = s.plan.GetVectorAnns().GetQueryInfo()
		}
	}
	return nil
}

// TODO: merge searchOnStreaming and searchOnHistorical?
func (s *searchTask) searchOnStreaming() error {
	// check ctx timeout
	if !funcutil.CheckCtxValid(s.Ctx()) {
		return errors.New("search context timeout")
	}

	if len(s.req.GetDmlChannels()) <= 0 {
		return errors.New("invalid nil dml channels")
	}

	// check if collection has been released, check streaming since it's released first
	_, err := s.QS.metaReplica.getCollectionByID(s.CollectionID)
	if err != nil {
		return err
	}

	s.QS.collection.RLock() // locks the collectionPtr
	defer s.QS.collection.RUnlock()
	if _, released := s.QS.collection.getReleaseTime(); released {
		log.Debug("collection release before search", zap.Int64("msgID", s.ID()),
			zap.Int64("collectionID", s.CollectionID))
		return fmt.Errorf("retrieve failed, collection has been released, collectionID = %d", s.CollectionID)
	}

	searchReq, err2 := newSearchRequest(s.QS.collection, s.req, s.PlaceholderGroup)
	if err2 != nil {
		return err2
	}
	defer searchReq.delete()

	// TODO add context
	partResults, _, _, sErr := searchStreaming(s.QS.metaReplica, searchReq, s.CollectionID, s.iReq.GetPartitionIDs(), s.req.GetDmlChannels()[0])
	if sErr != nil {
		log.Debug("failed to search streaming data", zap.Int64("msgID", s.ID()),
			zap.Int64("collectionID", s.CollectionID), zap.Error(sErr))
		return sErr
	}
	defer purgeMemoryAfterReduce()
	defer deleteSearchResults(partResults)
	return s.reduceResults(searchReq, partResults)
}

func (s *searchTask) searchOnHistorical() error {
	// check ctx timeout
	if !funcutil.CheckCtxValid(s.Ctx()) {
		return errors.New("search context timeout")
	}

	// check if collection has been released, check streaming since it's released first
	_, err := s.QS.metaReplica.getCollectionByID(s.CollectionID)
	if err != nil {
		return err
	}

	s.QS.collection.RLock() // locks the collectionPtr
	defer s.QS.collection.RUnlock()
	if _, released := s.QS.collection.getReleaseTime(); released {
		log.Debug("collection release before search", zap.Int64("msgID", s.ID()),
			zap.Int64("collectionID", s.CollectionID))
		return fmt.Errorf("retrieve failed, collection has been released, collectionID = %d", s.CollectionID)
	}

	segmentIDs := s.req.GetSegmentIDs()
	searchReq, err2 := newSearchRequest(s.QS.collection, s.req, s.PlaceholderGroup)
	if err2 != nil {
		return err2
	}
	defer searchReq.delete()

	partResults, _, _, err := searchHistorical(s.QS.metaReplica, searchReq, s.CollectionID, nil, segmentIDs)
	if err != nil {
		return err
	}
	defer purgeMemoryAfterReduce()
	defer deleteSearchResults(partResults)
	return s.reduceResults(searchReq, partResults)
}

func (s *searchTask) Execute(ctx context.Context) error {
	if s.DataScope == querypb.DataScope_Streaming {
		return s.searchOnStreaming()
	} else if s.DataScope == querypb.DataScope_Historical {
		return s.searchOnHistorical()
	}
	return fmt.Errorf("searchTask do not implement search on all data scope")
}

func (s *searchTask) Notify(err error) {
	if len(s.otherTasks) > 0 {
		metrics.QueryNodeSearchGroupSize.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Observe(float64(len(s.otherTasks) + 1))
		metrics.QueryNodeSearchGroupNQ.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Observe(float64(s.NQ))
		metrics.QueryNodeSearchGroupTopK.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Observe(float64(s.TopK))
	}
	s.done <- err
	for i := 0; i < len(s.otherTasks); i++ {
		s.otherTasks[i].Notify(err)
	}
}

func (s *searchTask) estimateCPUUsage() {
	var segmentNum int64
	if s.DataScope == querypb.DataScope_Streaming {
		// assume growing segments num is 5
		partitionIDs := s.iReq.GetPartitionIDs()
		channel := ""
		if len(s.req.GetDmlChannels()) > 0 {
			channel = s.req.GetDmlChannels()[0]
		}
		segIDs, err := s.QS.metaReplica.getSegmentIDsByVChannel(partitionIDs, channel, segmentTypeGrowing)
		if err != nil {
			log.Error("searchTask estimateCPUUsage", zap.Error(err))
		}
		segmentNum = int64(len(segIDs))
		if segmentNum <= 0 {
			segmentNum = 1
		}
	} else if s.DataScope == querypb.DataScope_Historical {
		segmentNum = int64(len(s.req.GetSegmentIDs()))
	}
	cpu := float64(s.NQ*segmentNum) * Params.QueryNodeCfg.CPURatio
	s.cpu = int32(cpu)
	if s.cpu <= 0 {
		s.cpu = 5
	} else if s.cpu > s.maxCPU {
		s.cpu = s.maxCPU
	}
}

func (s *searchTask) CPUUsage() int32 {
	s.cpuOnce.Do(func() {
		s.estimateCPUUsage()
	})
	return s.cpu
}

// reduceResults reduce search results
func (s *searchTask) reduceResults(searchReq *searchRequest, results []*SearchResult) error {
	isEmpty := len(results) == 0
	cnt := 1 + len(s.otherTasks)
	var t *searchTask
	s.tr.RecordSpan()
	if !isEmpty {
		sInfo := parseSliceInfo(s.OrigNQs, s.OrigTopKs, s.NQ)
		numSegment := int64(len(results))
		blobs, err := reduceSearchResultsAndFillData(searchReq.plan, results, numSegment, sInfo.sliceNQs, sInfo.sliceTopKs)
		if err != nil {
			log.Debug("marshal for historical results error", zap.Int64("msgID", s.ID()), zap.Error(err))
			return err
		}
		defer deleteSearchResultDataBlobs(blobs)

		for i := 0; i < cnt; i++ {
			blob, err := getSearchResultDataBlob(blobs, i)
			if err != nil {
				log.Debug("getSearchResultDataBlob for historical results error", zap.Int64("msgID", s.ID()),
					zap.Error(err))
				return err
			}
			bs := make([]byte, len(blob))
			copy(bs, blob)
			if i == 0 {
				t = s
			} else {
				t = s.otherTasks[i-1]
			}
			t.Ret = &internalpb.SearchResults{
				Status:         &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
				MetricType:     s.MetricType,
				NumQueries:     s.OrigNQs[i],
				TopK:           s.OrigTopKs[i],
				SlicedBlob:     bs,
				SlicedOffset:   1,
				SlicedNumCount: 1,
			}
		}
	} else {
		for i := 0; i < cnt; i++ {
			if i == 0 {
				t = s
			} else {
				t = s.otherTasks[i-1]
			}
			t.Ret = &internalpb.SearchResults{
				Status:         &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
				MetricType:     s.MetricType,
				NumQueries:     s.OrigNQs[i],
				TopK:           s.OrigTopKs[i],
				SlicedBlob:     nil,
				SlicedOffset:   1,
				SlicedNumCount: 1,
			}
		}
	}
	s.reduceDur = s.tr.RecordSpan()
	return nil
}

func (s *searchTask) CanMergeWith(t readTask) bool {
	s2, ok := t.(*searchTask)
	if !ok {
		return false
	}

	if s.DbID != s2.DbID {
		return false
	}

	if s.CollectionID != s2.CollectionID {
		return false
	}

	if s.QS != s2.QS {
		return false
	}

	if s.iReq.GetDslType() != s2.iReq.GetDslType() {
		return false
	}

	if s.iReq.GetPartitionIDs() == nil {
		if s2.iReq.GetPartitionIDs() != nil {
			return false
		}
	}
	if s2.iReq.GetPartitionIDs() == nil {
		if s.iReq.GetPartitionIDs() != nil {
			return false
		}
	}
	if !funcutil.SliceSetEqual(s.iReq.GetPartitionIDs(), s2.iReq.GetPartitionIDs()) {
		return false
	}

	if s.req.GetSegmentIDs() == nil {
		if s2.req.GetSegmentIDs() != nil {
			return false
		}
	}

	if s2.req.GetSegmentIDs() == nil {
		if s.req.GetSegmentIDs() != nil {
			return false
		}
	}

	if !funcutil.SliceSetEqual(s.req.GetSegmentIDs(), s2.req.GetSegmentIDs()) {
		return false
	}

	if !bytes.Equal(s.iReq.GetSerializedExprPlan(), s2.iReq.GetSerializedExprPlan()) {
		return false
	}

	if s.TravelTimestamp != s2.TravelTimestamp {
		return false
	}

	if !planparserv2.CheckPlanNodeIdentical(s.plan, s2.plan) {
		return false
	}

	pre := s.NQ * s.TopK * 1.0
	newTopK := s.TopK
	if newTopK < s2.TopK {
		newTopK = s2.TopK
	}
	after := (s.NQ + s2.NQ) * newTopK

	if pre == 0 {
		return false
	}
	ratio := float64(after) / float64(pre)
	if ratio > Params.QueryNodeCfg.TopKMergeRatio {
		return false
	}
	if s.NQ+s2.NQ > Params.QueryNodeCfg.MaxGroupNQ {
		return false
	}
	return true
}

func (s *searchTask) Merge(t readTask) {
	src, ok := t.(*searchTask)
	if !ok {
		return
	}
	newTopK := s.TopK
	if newTopK < src.TopK {
		newTopK = src.TopK
	}

	s.TopK = newTopK
	s.OrigTopKs = append(s.OrigTopKs, src.OrigTopKs...)
	s.OrigNQs = append(s.OrigNQs, src.OrigNQs...)
	s.NQ += src.NQ
	s.otherTasks = append(s.otherTasks, src)
}

// combinePlaceHolderGroups combine all the placeholder groups.
func (s *searchTask) combinePlaceHolderGroups() {
	if len(s.otherTasks) > 0 {
		ret := &commonpb.PlaceholderGroup{}
		_ = proto.Unmarshal(s.PlaceholderGroup, ret)
		for _, t := range s.otherTasks {
			x := &commonpb.PlaceholderGroup{}
			_ = proto.Unmarshal(t.PlaceholderGroup, x)
			ret.Placeholders[0].Values = append(ret.Placeholders[0].Values, x.Placeholders[0].Values...)
		}
		s.PlaceholderGroup, _ = proto.Marshal(ret)
	}
}

func newSearchTask(ctx context.Context, src *querypb.SearchRequest) (*searchTask, error) {
	target := &searchTask{
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
			tr:                 timerecord.NewTimeRecorder("searchTask"),
			DataScope:          src.GetScope(),
		},
		iReq:             src.Req,
		req:              src,
		TopK:             src.Req.GetTopk(),
		OrigTopKs:        []int64{src.Req.GetTopk()},
		NQ:               src.Req.GetNq(),
		OrigNQs:          []int64{src.Req.GetNq()},
		PlaceholderGroup: src.Req.GetPlaceholderGroup(),
		MetricType:       src.Req.GetMetricType(),
	}
	err := target.init()
	if err != nil {
		return nil, err
	}
	return target, nil
}
