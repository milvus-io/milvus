package reduce

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type ResultInfo struct {
	nq             int64
	topK           int64
	metricType     string
	pkType         schemapb.DataType
	offset         int64
	groupByFieldId int64
	groupSize      int64
	isAdvance      bool
	// rerank context: when set, reducer will apply rerank on reduced data
	rerankFunction   *schemapb.FunctionSchema
	collectionSchema *schemapb.CollectionSchema
}

func NewReduceSearchResultInfo(
	nq int64,
	topK int64,
) *ResultInfo {
	return &ResultInfo{
		nq:   nq,
		topK: topK,
	}
}

func (r *ResultInfo) WithMetricType(metricType string) *ResultInfo {
	r.metricType = metricType
	return r
}

func (r *ResultInfo) WithPkType(pkType schemapb.DataType) *ResultInfo {
	r.pkType = pkType
	return r
}

func (r *ResultInfo) WithOffset(offset int64) *ResultInfo {
	r.offset = offset
	return r
}

func (r *ResultInfo) WithGroupByField(groupByField int64) *ResultInfo {
	r.groupByFieldId = groupByField
	return r
}

func (r *ResultInfo) WithGroupSize(groupSize int64) *ResultInfo {
	r.groupSize = groupSize
	return r
}

func (r *ResultInfo) WithAdvance(advance bool) *ResultInfo {
	r.isAdvance = advance
	return r
}

// WithRerank sets rerank context to be used by reducers that support it
func (r *ResultInfo) WithRerank(collSchema *schemapb.CollectionSchema, funcSchema *schemapb.FunctionSchema) *ResultInfo {
	r.collectionSchema = collSchema
	r.rerankFunction = funcSchema
	return r
}

func (r *ResultInfo) GetNq() int64 {
	return r.nq
}

func (r *ResultInfo) GetTopK() int64 {
	return r.topK
}

func (r *ResultInfo) GetMetricType() string {
	return r.metricType
}

func (r *ResultInfo) GetPkType() schemapb.DataType {
	return r.pkType
}

func (r *ResultInfo) GetOffset() int64 {
	return r.offset
}

func (r *ResultInfo) GetGroupByFieldId() int64 {
	return r.groupByFieldId
}

func (r *ResultInfo) GetGroupSize() int64 {
	return r.groupSize
}

func (r *ResultInfo) GetIsAdvance() bool {
	return r.isAdvance
}

func (r *ResultInfo) SetMetricType(metricType string) {
	r.metricType = metricType
}

func (r *ResultInfo) GetRerankFunction() *schemapb.FunctionSchema {
	return r.rerankFunction
}

func (r *ResultInfo) GetCollectionSchema() *schemapb.CollectionSchema {
	return r.collectionSchema
}

type IReduceType int32

const (
	IReduceNoOrder IReduceType = iota
	IReduceInOrder
	IReduceInOrderForBest
)

func ShouldStopWhenDrained(reduceType IReduceType) bool {
	return reduceType == IReduceInOrder || reduceType == IReduceInOrderForBest
}

func ToReduceType(val int32) IReduceType {
	switch val {
	case 1:
		return IReduceInOrder
	case 2:
		return IReduceInOrderForBest
	default:
		return IReduceNoOrder
	}
}

func ShouldUseInputLimit(reduceType IReduceType) bool {
	return reduceType == IReduceNoOrder || reduceType == IReduceInOrder
}
