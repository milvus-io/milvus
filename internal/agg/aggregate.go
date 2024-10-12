package agg

import (
	"fmt"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"hash/fnv"
	"regexp"
	"strings"
)

const (
	kSum   = "sum"
	kCount = "count"
	kAvg   = "avg"
	kMin   = "min"
	kMax   = "max"
)

var (
	// Define the regular expression pattern once to avoid repeated concatenation.
	aggregationTypes   = kSum + `|` + kCount + `|` + kAvg + `|` + kMin + `|` + kMax
	aggregationPattern = regexp.MustCompile(`(?i)^(` + aggregationTypes + `)\s*\(\s*([\w\*]*)\s*\)$`)
)

// MatchAggregationExpression return isAgg, operator name, operator parameter
func MatchAggregationExpression(expression string) (bool, string, string) {
	// FindStringSubmatch returns the full match and submatches.
	matches := aggregationPattern.FindStringSubmatch(expression)
	if len(matches) > 0 {
		// Return true, the operator, and the captured parameter.
		return true, strings.ToLower(matches[1]), strings.TrimSpace(matches[2])
	}
	return false, "", ""
}

type AggID uint64

type AggregateBase interface {
	Reduce()
	Name() string
	Compute()
	Decompose() []AggregateBase
	ID() AggID
	ToPB() *planpb.Aggregate
}

type Aggregate struct {
	fieldID int64
	aggID   AggID
}

func (agg *Aggregate) aggFieldID() int64 {
	return agg.fieldID
}
func (agg *Aggregate) Name() string {
	return ""
}
func (agg *Aggregate) Compute() {
}
func (agg *Aggregate) Reduce() {
}
func (agg *Aggregate) Decompose() []AggregateBase {
	return nil
}
func (agg *Aggregate) ToPB() *planpb.Aggregate {
	return nil
}

func (agg *Aggregate) ID() AggID {
	if agg.aggID == 0 {
		h := fnv.New64a()
		h.Write([]byte(fmt.Sprintf("%s-%d", agg.Name(), agg.fieldID)))
		agg.aggID = AggID(h.Sum64())
	}
	return agg.aggID
}

func NewAggregate(aggregateName string, aggFieldID int64) (AggregateBase, error) {
	switch aggregateName {
	case kCount:
		return &Aggregate{fieldID: aggFieldID}, nil
	case kSum:
		return &SumAggregate{Aggregate: Aggregate{fieldID: aggFieldID}}, nil
	case kAvg:
		return &AverageAggregate{Aggregate: Aggregate{fieldID: aggFieldID}}, nil
	case kMin:
		return &MinAggregate{Aggregate: Aggregate{fieldID: aggFieldID}}, nil
	case kMax:
		return &MaxAggregate{Aggregate: Aggregate{fieldID: aggFieldID}}, nil
	default:
		return nil, fmt.Errorf("invalid Aggregation operator %s", aggregateName)
	}
}

type SumAggregate struct {
	Aggregate
}

func (sum *SumAggregate) Name() string {
	return kSum
}
func (sum *SumAggregate) Compute() {
}
func (sum *SumAggregate) Reduce() {
}
func (sum *SumAggregate) ToPB() *planpb.Aggregate {
	return &planpb.Aggregate{Op: planpb.AggregateOp_sum, FieldId: sum.aggFieldID()}
}

type CountAggregate struct {
	Aggregate
}

func (count *CountAggregate) Name() string {
	return kCount
}
func (count *CountAggregate) Compute() {
}
func (count *CountAggregate) Reduce() {
}
func (count *CountAggregate) ToPB() *planpb.Aggregate {
	return &planpb.Aggregate{Op: planpb.AggregateOp_count, FieldId: count.aggFieldID()}
}

type AverageAggregate struct {
	Aggregate
	subAggs []AggregateBase
}

func (avg *AverageAggregate) Name() string {
	return kAvg
}
func (avg *AverageAggregate) Compute() {
}
func (avg *AverageAggregate) Reduce() {
}

func (avg *AverageAggregate) Decompose() []AggregateBase {
	if avg.subAggs == nil {
		avg.subAggs = make([]AggregateBase, 2)
		sumSub, _ := NewAggregate(kSum, avg.aggFieldID())
		countSub, _ := NewAggregate(kCount, avg.aggFieldID())
		avg.subAggs[0] = sumSub
		avg.subAggs[1] = countSub
	}
	return avg.subAggs
}

type MinAggregate struct {
	Aggregate
}

func (min *MinAggregate) Name() string {
	return kMin
}
func (min *MinAggregate) Compute() {
}
func (min *MinAggregate) Reduce() {
}

func (min *MinAggregate) ToPB() *planpb.Aggregate {
	return &planpb.Aggregate{Op: planpb.AggregateOp_min, FieldId: min.aggFieldID()}
}

type MaxAggregate struct {
	Aggregate
}

func (max *MaxAggregate) Name() string {
	return kMax
}
func (max *MaxAggregate) Compute() {
}
func (max *MaxAggregate) Reduce() {
}

func (max *MaxAggregate) ToPB() *planpb.Aggregate {
	return &planpb.Aggregate{Op: planpb.AggregateOp_max, FieldId: max.aggFieldID()}
}

func OrganizeAggregates(userAggregates []AggregateBase) map[AggID]AggregateBase {
	realAggregates := make(map[AggID]AggregateBase, 0)
	for _, userAgg := range userAggregates {
		subAggs := userAgg.Decompose()
		for _, subAgg := range subAggs {
			if _, exist := realAggregates[subAgg.ID()]; !exist {
				realAggregates[subAgg.ID()] = subAgg
			}
		}
	}
	return realAggregates
}

func AggregatesToPB(aggregates map[AggID]AggregateBase) []*planpb.Aggregate {
	ret := make([]*planpb.Aggregate, len(aggregates))
	if aggregates != nil {
		for idx, agg := range aggregates {
			ret[idx] = agg.ToPB()
		}
	}
	return ret
}
