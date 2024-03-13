package tasks

import (
	"context"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type StreamingSearchTask struct {
	SearchTask
	others        []*StreamingSearchTask
	resultBlobs   segments.SearchResultDataBlobs
	streamReducer segments.StreamSearchReducer
	reduceMutex   sync.RWMutex
}

func NewStreamingSearchTask(ctx context.Context,
	collection *segments.Collection,
	manager *segments.Manager,
	req *querypb.SearchRequest,
	serverID int64,
) *StreamingSearchTask {
	ctx, span := otel.Tracer(typeutil.QueryNodeRole).Start(ctx, "schedule")
	return &StreamingSearchTask{
		SearchTask: SearchTask{
			ctx:              ctx,
			collection:       collection,
			segmentManager:   manager,
			req:              req,
			merged:           false,
			groupSize:        1,
			topk:             req.GetReq().GetTopk(),
			nq:               req.GetReq().GetNq(),
			placeholderGroup: req.GetReq().GetPlaceholderGroup(),
			originTopks:      []int64{req.GetReq().GetTopk()},
			originNqs:        []int64{req.GetReq().GetNq()},
			notifier:         make(chan error, 1),
			tr:               timerecord.NewTimeRecorderWithTrace(ctx, "searchTask"),
			scheduleSpan:     span,
			serverID:         serverID,
		},
	}
}

func (t *StreamingSearchTask) MergeWith(other Task) bool {
	return false
}

func (t *StreamingSearchTask) Execute() error {
	log := log.Ctx(t.ctx).With(
		zap.Int64("collectionID", t.collection.ID()),
		zap.String("shard", t.req.GetDmlChannels()[0]),
	)
	// 0. prepare search req
	if t.scheduleSpan != nil {
		t.scheduleSpan.End()
	}
	tr := timerecord.NewTimeRecorderWithTrace(t.ctx, "SearchTask")
	req := t.req
	t.combinePlaceHolderGroups()
	searchReq, err := segments.NewSearchRequest(t.ctx, t.collection, req, t.placeholderGroup)
	if err != nil {
		return err
	}
	defer searchReq.Delete()

	var pinnedSegments []segments.Segment
	// 1. search&&reduce or streaming-search&&streaming-reduce
	metricType := searchReq.Plan().GetMetricType()
	if req.GetScope() == querypb.DataScope_Historical {
		streamReduceFunc := func(result *segments.SearchResult) error {
			t.reduceMutex.Lock()
			defer t.reduceMutex.Unlock()
			reduceErr := t.streamReduce(t.ctx, searchReq.Plan(), result, t.originNqs, t.originTopks)
			return reduceErr
		}
		pinnedSegments, err = segments.SearchHistoricalStreamly(
			t.ctx,
			t.segmentManager,
			searchReq,
			req.GetReq().GetCollectionID(),
			nil,
			req.GetSegmentIDs(),
			streamReduceFunc)
		defer segments.DeleteStreamReduceHelper(t.streamReducer)
		if err != nil {
			log.Error("Failed to search sealed segments streamly", zap.Error(err))
			return err
		}
		t.resultBlobs, err = segments.GetStreamReduceResult(t.ctx, t.streamReducer)
		defer segments.DeleteSearchResultDataBlobs(t.resultBlobs)
		if err != nil {
			log.Error("Failed to get stream-reduced search result")
			return err
		}
	} else if req.GetScope() == querypb.DataScope_Streaming {
		var results []*segments.SearchResult
		results, pinnedSegments, err = segments.SearchStreaming(
			t.ctx,
			t.segmentManager,
			searchReq,
			req.GetReq().GetCollectionID(),
			nil,
			req.GetSegmentIDs(),
		)
		defer segments.DeleteSearchResults(results)
		if err != nil {
			return err
		}
		if t.maybeReturnForEmptyResults(results, metricType, tr) {
			return nil
		}
		tr.RecordSpan()
		t.resultBlobs, err = segments.ReduceSearchResultsAndFillData(
			t.ctx,
			searchReq.Plan(),
			results,
			int64(len(results)),
			t.originNqs,
			t.originTopks,
		)
		if err != nil {
			log.Warn("failed to reduce search results", zap.Error(err))
			return err
		}
		defer segments.DeleteSearchResultDataBlobs(t.resultBlobs)
		metrics.QueryNodeReduceLatency.WithLabelValues(
			fmt.Sprint(t.GetNodeID()),
			metrics.SearchLabel,
			metrics.ReduceSegments,
			metrics.BatchReduce).
			Observe(float64(tr.RecordSpan().Milliseconds()))
	}
	defer t.segmentManager.Segment.Unpin(pinnedSegments)

	// 2. reorganize blobs to original search request
	for i := range t.originNqs {
		blob, err := segments.GetSearchResultDataBlob(t.ctx, t.resultBlobs, i)
		if err != nil {
			return err
		}

		var task *StreamingSearchTask
		if i == 0 {
			task = t
		} else {
			task = t.others[i-1]
		}

		// Note: blob is unsafe because get from C
		bs := make([]byte, len(blob))
		copy(bs, blob)

		task.result = &internalpb.SearchResults{
			Base: &commonpb.MsgBase{
				SourceID: t.GetNodeID(),
			},
			Status:         merr.Success(),
			MetricType:     metricType,
			NumQueries:     t.originNqs[i],
			TopK:           t.originTopks[i],
			SlicedBlob:     bs,
			SlicedOffset:   1,
			SlicedNumCount: 1,
			CostAggregation: &internalpb.CostAggregation{
				ServiceTime: tr.ElapseSpan().Milliseconds(),
			},
		}
	}

	return nil
}

func (t *StreamingSearchTask) maybeReturnForEmptyResults(results []*segments.SearchResult,
	metricType string, tr *timerecord.TimeRecorder,
) bool {
	if len(results) == 0 {
		for i := range t.originNqs {
			var task *StreamingSearchTask
			if i == 0 {
				task = t
			} else {
				task = t.others[i-1]
			}

			task.result = &internalpb.SearchResults{
				Base: &commonpb.MsgBase{
					SourceID: t.GetNodeID(),
				},
				Status:         merr.Success(),
				MetricType:     metricType,
				NumQueries:     t.originNqs[i],
				TopK:           t.originTopks[i],
				SlicedOffset:   1,
				SlicedNumCount: 1,
				CostAggregation: &internalpb.CostAggregation{
					ServiceTime: tr.ElapseSpan().Milliseconds(),
				},
			}
		}
		return true
	}
	return false
}

func (t *StreamingSearchTask) streamReduce(ctx context.Context,
	plan *segments.SearchPlan,
	newResult *segments.SearchResult,
	sliceNQs []int64,
	sliceTopKs []int64,
) error {
	if t.streamReducer == nil {
		var err error
		t.streamReducer, err = segments.NewStreamReducer(ctx, plan, sliceNQs, sliceTopKs)
		if err != nil {
			log.Error("Fail to init stream reducer, return", zap.Error(err))
			return err
		}
	}

	return segments.StreamReduceSearchResult(ctx, newResult, t.streamReducer)
}
