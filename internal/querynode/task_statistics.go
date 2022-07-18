package querynode

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/timerecord"
)

// not implement any task
type statistics struct {
	id                 int64
	ts                 Timestamp
	ctx                context.Context
	qs                 *queryShard
	scope              querypb.DataScope
	travelTimestamp    Timestamp
	guaranteeTimestamp Timestamp
	timeoutTimestamp   Timestamp
	tr                 *timerecord.TimeRecorder
	iReq               *internalpb.GetStatisticsRequest
	req                *querypb.GetStatisticsRequest
	Ret                *internalpb.GetStatisticsResponse
	waitCanDoFunc      func(ctx context.Context) error
}

func (s *statistics) statisticOnStreaming() error {
	// check ctx timeout
	if !funcutil.CheckCtxValid(s.ctx) {
		return errors.New("get statistics context timeout")
	}

	// check if collection has been released, check streaming since it's released first
	_, err := s.qs.metaReplica.getCollectionByID(s.iReq.GetCollectionID())
	if err != nil {
		return err
	}

	s.qs.collection.RLock() // locks the collectionPtr
	defer s.qs.collection.RUnlock()
	if _, released := s.qs.collection.getReleaseTime(); released {
		log.Debug("collection release before do statistics", zap.Int64("msgID", s.id),
			zap.Int64("collectionID", s.iReq.GetCollectionID()))
		return fmt.Errorf("statistic failed, collection has been released, collectionID = %d", s.iReq.GetCollectionID())
	}

	results, _, _, err := statisticStreaming(s.qs.metaReplica, s.iReq.GetCollectionID(), s.iReq.GetPartitionIDs(), s.req.GetDmlChannels()[0])
	if err != nil {
		log.Debug("failed to statistic on streaming data", zap.Int64("msgID", s.id),
			zap.Int64("collectionID", s.iReq.GetCollectionID()), zap.Error(err))
		return err
	}
	return s.reduceResults(results)
}

func (s *statistics) statisticOnHistorical() error {
	// check ctx timeout
	if !funcutil.CheckCtxValid(s.ctx) {
		return errors.New("get statistics context timeout")
	}

	// check if collection has been released, check streaming since it's released first
	_, err := s.qs.metaReplica.getCollectionByID(s.iReq.GetCollectionID())
	if err != nil {
		return err
	}

	s.qs.collection.RLock() // locks the collectionPtr
	defer s.qs.collection.RUnlock()
	if _, released := s.qs.collection.getReleaseTime(); released {
		log.Debug("collection release before do statistics", zap.Int64("msgID", s.id),
			zap.Int64("collectionID", s.iReq.GetCollectionID()))
		return fmt.Errorf("statistic failed, collection has been released, collectionID = %d", s.iReq.GetCollectionID())
	}

	segmentIDs := s.req.GetSegmentIDs()
	results, _, _, err := statisticHistorical(s.qs.metaReplica, s.iReq.GetCollectionID(), s.iReq.GetPartitionIDs(), segmentIDs)
	if err != nil {
		return err
	}
	return s.reduceResults(results)
}

func (s *statistics) Execute(ctx context.Context) error {
	if err := s.waitCanDoFunc(ctx); err != nil {
		return err
	}
	if s.scope == querypb.DataScope_Streaming {
		return s.statisticOnStreaming()
	} else if s.scope == querypb.DataScope_Historical {
		return s.statisticOnHistorical()
	}
	return fmt.Errorf("statistics do not implement do statistic on all data scope")
}

func (s *statistics) reduceResults(results []map[string]interface{}) error {
	mergedResults := map[string]interface{}{
		"row_count": int64(0),
	}
	fieldMethod := map[string]func(interface{}) error{
		"row_count": func(v interface{}) error {
			count, ok := v.(int64)
			if !ok {
				return fmt.Errorf("invalid value type for row_count. expect int64, got %T", v)
			}
			mergedResults["row_count"] = mergedResults["row_count"].(int64) + count
			return nil
		},
	}
	for _, result := range results {
		for k, v := range result {
			fn, ok := fieldMethod[k]
			if !ok {
				return fmt.Errorf("unknown field %s", k)
			}
			if err := fn(v); err != nil {
				return err
			}
		}
	}

	stringMap := make(map[string]string)
	for k, v := range mergedResults {
		stringMap[k] = fmt.Sprint(v)
	}

	s.Ret = &internalpb.GetStatisticsResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		Stats:  funcutil.Map2KeyValuePair(stringMap),
	}
	return nil
}

func newStatistics(ctx context.Context, src *querypb.GetStatisticsRequest, scope querypb.DataScope, qs *queryShard, waitCanDo func(ctx context.Context) error) *statistics {
	target := &statistics{
		ctx:                ctx,
		id:                 src.Req.Base.GetMsgID(),
		ts:                 src.Req.Base.GetTimestamp(),
		scope:              scope,
		qs:                 qs,
		travelTimestamp:    src.Req.GetTravelTimestamp(),
		guaranteeTimestamp: src.Req.GetGuaranteeTimestamp(),
		timeoutTimestamp:   src.Req.GetTimeoutTimestamp(),
		tr:                 timerecord.NewTimeRecorder("statistics"),
		iReq:               src.Req,
		req:                src,
		waitCanDoFunc:      waitCanDo,
	}
	return target
}
