package tasks

import (
	"bytes"
	"context"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"go.uber.org/zap"
)

type Task interface {
	Execute() error
	Done(err error)
	Canceled() error
	Wait() error
}

type SearchTask struct {
	ctx            context.Context
	collection     *segments.Collection
	segmentManager *segments.Manager
	req            *querypb.SearchRequest
	result         *internalpb.SearchResults
	originTopks    []int64
	originNqs      []int64
	others         []*SearchTask
	notifier       chan error
}

func NewSearchTask(ctx context.Context,
	collection *segments.Collection,
	manager *segments.Manager,
	req *querypb.SearchRequest,
) *SearchTask {
	return &SearchTask{
		ctx:            ctx,
		collection:     collection,
		segmentManager: manager,
		req:            req,
		originTopks:    []int64{req.GetReq().GetTopk()},
		originNqs:      []int64{req.GetReq().GetNq()},
		notifier:       make(chan error, 1),
	}
}

func (t *SearchTask) Execute() error {
	log := log.Ctx(t.ctx).With(
		zap.Int64("collectionID", t.collection.ID()),
		zap.String("shard", t.req.GetDmlChannels()[0]),
	)
	req := t.req
	searchReq, err := segments.NewSearchRequest(t.collection, req, req.GetReq().GetPlaceholderGroup())
	if err != nil {
		return err
	}
	defer searchReq.Delete()

	var results []*segments.SearchResult
	if req.GetScope() == querypb.DataScope_Historical {
		results, _, _, err = segments.SearchHistorical(
			t.ctx,
			t.segmentManager,
			searchReq,
			req.GetReq().GetCollectionID(),
			nil,
			req.GetSegmentIDs(),
		)
	} else if req.GetScope() == querypb.DataScope_Streaming {
		results, _, _, err = segments.SearchStreaming(
			t.ctx,
			t.segmentManager,
			searchReq,
			req.GetReq().GetCollectionID(),
			nil,
			req.GetSegmentIDs(),
		)
	}
	if err != nil {
		return err
	}
	defer segments.DeleteSearchResults(results)

	if len(results) == 0 {
		t.result = &internalpb.SearchResults{
			Status:         &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			MetricType:     req.GetReq().GetMetricType(),
			NumQueries:     req.GetReq().GetNq(),
			TopK:           req.GetReq().GetTopk(),
			SlicedBlob:     nil,
			SlicedOffset:   1,
			SlicedNumCount: 1,
		}
		return nil
	}

	blobs, err := segments.ReduceSearchResultsAndFillData(
		searchReq.Plan(),
		results,
		int64(len(results)),
		[]int64{req.GetReq().GetNq()},
		[]int64{req.GetReq().GetTopk()},
	)
	if err != nil {
		log.Warn("failed to reduce search results", zap.Error(err))
		return err
	}
	defer segments.DeleteSearchResultDataBlobs(blobs)

	blob, err := segments.GetSearchResultDataBlob(blobs, 0)
	if err != nil {
		return err
	}

	// Note: blob is unsafe because get from C
	bs := make([]byte, len(blob))
	copy(bs, blob)

	t.result = &internalpb.SearchResults{
		Status:         util.WrapStatus(commonpb.ErrorCode_Success, ""),
		MetricType:     req.GetReq().GetMetricType(),
		NumQueries:     req.GetReq().GetNq(),
		TopK:           req.GetReq().GetTopk(),
		SlicedBlob:     bs,
		SlicedOffset:   1,
		SlicedNumCount: 1,
	}
	return nil
}

func (t *SearchTask) Merge(other *SearchTask) bool {
	var (
		nq        = t.req.GetReq().GetNq()
		topk      = t.req.GetReq().GetTopk()
		otherNq   = other.req.GetReq().GetNq()
		otherTopk = other.req.GetReq().GetTopk()
	)

	pre := funcutil.Min(nq*topk, otherNq*otherTopk)
	maxTopk := funcutil.Max(topk, otherTopk)
	after := (nq + otherNq) * maxTopk
	ratio := float64(after) / float64(pre)

	// Check mergeable
	if t.req.GetReq().GetDbID() != other.req.GetReq().GetDbID() ||
		t.req.GetReq().GetCollectionID() != other.req.GetReq().GetCollectionID() ||
		t.req.GetReq().GetTravelTimestamp() != other.req.GetReq().GetTravelTimestamp() ||
		t.req.GetReq().GetDslType() != other.req.GetReq().GetDslType() ||
		t.req.GetDmlChannels()[0] != other.req.GetDmlChannels()[0] ||
		nq+otherNq > paramtable.Get().QueryNodeCfg.MaxGroupNQ.GetAsInt64() ||
		ratio > paramtable.Get().QueryNodeCfg.TopKMergeRatio.GetAsFloat() ||
		!funcutil.SliceSetEqual(t.req.GetReq().GetPartitionIDs(), other.req.GetReq().GetPartitionIDs()) ||
		!funcutil.SliceSetEqual(t.req.GetSegmentIDs(), other.req.GetSegmentIDs()) ||
		!bytes.Equal(t.req.GetReq().GetSerializedExprPlan(), other.req.GetReq().GetSerializedExprPlan()) {
		return false
	}

	// Merge
	t.req.GetReq().Topk = maxTopk
	t.req.GetReq().Nq += otherNq
	t.originTopks = append(t.originTopks, other.originTopks...)
	t.originNqs = append(t.originNqs, other.originNqs...)
	t.others = append(t.others, other)
	t.others = append(t.others, other.others...)

	return true
}

func (t *SearchTask) Done(err error) {
	if len(t.others) > 0 {
		metrics.QueryNodeSearchGroupSize.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Observe(float64(len(t.others) + 1))
		metrics.QueryNodeSearchGroupNQ.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Observe(float64(t.originNqs[0]))
		metrics.QueryNodeSearchGroupTopK.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Observe(float64(t.originTopks[0]))
	}
	t.notifier <- err
	for _, other := range t.others {
		other.Done(err)
	}
}

func (t *SearchTask) Canceled() error {
	return t.ctx.Err()
}

func (t *SearchTask) Wait() error {
	return <-t.notifier
}

func (t *SearchTask) Result() *internalpb.SearchResults {
	return t.result
}

type QueryTask struct {
}
