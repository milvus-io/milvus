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

package delegator

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// PKFilterTarget is implemented by both local Segment objects (worker-side)
// and pkoracle.Candidate objects (delegator-side), allowing the same PK
// predicate IR to be evaluated at either layer.
type PKFilterTarget interface {
	ID() int64
	PkCandidateExist() bool
	BatchPkExist(*storage.BatchLocationsCache) []bool
	GetMinPk() *storage.PrimaryKey
	GetMaxPk() *storage.PrimaryKey
}

// PKFilterExpr is the compiled IR for a PK predicate extracted from a query plan.
type PKFilterExpr interface {
	isPKFilterExpr()
}

type pkFilterLogicalOp int

const (
	pkFilterLogicalAnd pkFilterLogicalOp = iota
	pkFilterLogicalOr
)

type pkFilterLogicalExpr struct {
	op    pkFilterLogicalOp
	left  PKFilterExpr
	right PKFilterExpr
}

func (*pkFilterLogicalExpr) isPKFilterExpr() {}

type pkFilterTermExpr struct {
	values []storage.PrimaryKey
}

func (*pkFilterTermExpr) isPKFilterExpr() {}

type pkFilterUnaryRangeExpr struct {
	op    planpb.OpType
	value storage.PrimaryKey
}

func (*pkFilterUnaryRangeExpr) isPKFilterExpr() {}

type pkFilterBound struct {
	value     storage.PrimaryKey
	inclusive bool
	present   bool
}

type pkFilterBinaryRangeExpr struct {
	lower pkFilterBound
	upper pkFilterBound
}

func (*pkFilterBinaryRangeExpr) isPKFilterExpr() {}

type pkFilterUnsupportedExpr struct{}

func (*pkFilterUnsupportedExpr) isPKFilterExpr() {}

type pkMatchSet struct {
	all bool
	ids typeutil.Set[int64]
}

func extractPKFilterPredicates(plan *planpb.PlanNode) *planpb.Expr {
	if plan == nil {
		return nil
	}
	if queryPlan := plan.GetQuery(); queryPlan != nil {
		return queryPlan.GetPredicates()
	}
	if vectorAnns := plan.GetVectorAnns(); vectorAnns != nil {
		return vectorAnns.GetPredicates()
	}
	return nil
}

// BuildPKFilterExpr compiles the PK predicate from plan into a PKFilterExpr.
// Returns nil when pkFilter == PkFilterNoPkFilter (proxy confirmed no PK predicate).
func BuildPKFilterExpr(plan *planpb.PlanNode, pkFilter int32) PKFilterExpr {
	if pkFilter == common.PkFilterNoPkFilter {
		return nil
	}
	return buildPKFilterExpr(extractPKFilterPredicates(plan))
}

func buildPKFilterExpr(expr *planpb.Expr) PKFilterExpr {
	if expr == nil {
		return nil
	}
	switch inner := expr.GetExpr().(type) {
	case *planpb.Expr_BinaryExpr:
		return buildPKFilterBinaryExpr(inner.BinaryExpr)
	case *planpb.Expr_UnaryRangeExpr:
		return buildPKFilterUnaryRangeExpr(inner.UnaryRangeExpr)
	case *planpb.Expr_BinaryRangeExpr:
		return buildPKFilterBinaryRangeExpr(inner.BinaryRangeExpr)
	case *planpb.Expr_TermExpr:
		return buildPKFilterTermExpr(inner.TermExpr)
	default:
		return &pkFilterUnsupportedExpr{}
	}
}

func buildPKFilterBinaryExpr(expr *planpb.BinaryExpr) PKFilterExpr {
	if expr == nil {
		return &pkFilterUnsupportedExpr{}
	}
	var op pkFilterLogicalOp
	switch expr.GetOp() {
	case planpb.BinaryExpr_LogicalAnd:
		op = pkFilterLogicalAnd
	case planpb.BinaryExpr_LogicalOr:
		op = pkFilterLogicalOr
	default:
		return &pkFilterUnsupportedExpr{}
	}
	return &pkFilterLogicalExpr{
		op:    op,
		left:  buildPKFilterExpr(expr.GetLeft()),
		right: buildPKFilterExpr(expr.GetRight()),
	}
}

func buildPKFilterUnaryRangeExpr(expr *planpb.UnaryRangeExpr) PKFilterExpr {
	if expr == nil ||
		expr.GetColumnInfo() == nil ||
		!expr.GetColumnInfo().GetIsPrimaryKey() ||
		expr.GetValue() == nil {
		return &pkFilterUnsupportedExpr{}
	}
	pk, ok := pkFromGenericValue(expr.GetColumnInfo().GetDataType(), expr.GetValue())
	if !ok {
		return &pkFilterUnsupportedExpr{}
	}
	if expr.GetOp() == planpb.OpType_Equal {
		return &pkFilterTermExpr{values: []storage.PrimaryKey{pk}}
	}
	switch expr.GetOp() {
	case planpb.OpType_GreaterThan, planpb.OpType_GreaterEqual, planpb.OpType_LessThan, planpb.OpType_LessEqual:
		return &pkFilterUnaryRangeExpr{op: expr.GetOp(), value: pk}
	default:
		return &pkFilterUnsupportedExpr{}
	}
}

func buildPKFilterBinaryRangeExpr(expr *planpb.BinaryRangeExpr) PKFilterExpr {
	if expr == nil ||
		expr.GetColumnInfo() == nil ||
		!expr.GetColumnInfo().GetIsPrimaryKey() ||
		expr.GetLowerValue() == nil ||
		expr.GetUpperValue() == nil {
		return &pkFilterUnsupportedExpr{}
	}
	lower, ok := pkFromGenericValue(expr.GetColumnInfo().GetDataType(), expr.GetLowerValue())
	if !ok {
		return &pkFilterUnsupportedExpr{}
	}
	upper, ok := pkFromGenericValue(expr.GetColumnInfo().GetDataType(), expr.GetUpperValue())
	if !ok {
		return &pkFilterUnsupportedExpr{}
	}
	return &pkFilterBinaryRangeExpr{
		lower: pkFilterBound{value: lower, inclusive: expr.GetLowerInclusive(), present: true},
		upper: pkFilterBound{value: upper, inclusive: expr.GetUpperInclusive(), present: true},
	}
}

func buildPKFilterTermExpr(expr *planpb.TermExpr) PKFilterExpr {
	if expr == nil ||
		expr.GetColumnInfo() == nil ||
		!expr.GetColumnInfo().GetIsPrimaryKey() {
		return &pkFilterUnsupportedExpr{}
	}
	values := expr.GetValues()
	if len(values) == 0 {
		return &pkFilterTermExpr{}
	}
	converted := make([]storage.PrimaryKey, 0, len(values))
	for _, value := range values {
		pk, ok := pkFromGenericValue(expr.GetColumnInfo().GetDataType(), value)
		if !ok {
			return &pkFilterUnsupportedExpr{}
		}
		converted = append(converted, pk)
	}
	return &pkFilterTermExpr{values: converted}
}

func pkFromGenericValue(dataType schemapb.DataType, value *planpb.GenericValue) (storage.PrimaryKey, bool) {
	switch dataType {
	case schemapb.DataType_Int64:
		return storage.NewInt64PrimaryKey(value.GetInt64Val()), true
	case schemapb.DataType_VarChar:
		return storage.NewVarCharPrimaryKey(value.GetStringVal()), true
	default:
		log.Warn("unknown pk type", zap.Int("type", int(dataType)))
		return nil, false
	}
}

// CheckPKFilter evaluates expr against each target and returns the matching IDs.
// If all targets match (or expr is nil/targets is empty), returns (nil, true).
func CheckPKFilter(expr PKFilterExpr, targets []PKFilterTarget) (typeutil.Set[int64], bool) {
	if expr == nil || len(targets) == 0 {
		return nil, true
	}
	matched := evalPKFilterExpr(expr, targets)
	return matched.ids, matched.all
}

func evalPKFilterExpr(expr PKFilterExpr, targets []PKFilterTarget) pkMatchSet {
	switch inner := expr.(type) {
	case *pkFilterLogicalExpr:
		left := evalPKFilterExpr(inner.left, targets)
		if inner.op == pkFilterLogicalAnd {
			if !left.all && len(left.ids) == 0 {
				return left
			}
			right := evalPKFilterExpr(inner.right, targets)
			return intersectPKMatches(left, right)
		}
		if left.all {
			return left
		}
		right := evalPKFilterExpr(inner.right, targets)
		return unionPKMatches(left, right)
	case *pkFilterTermExpr:
		return evalPKFilterTermExpr(inner, targets)
	case *pkFilterUnaryRangeExpr:
		return evalPKFilterUnaryRangeExpr(inner, targets)
	case *pkFilterBinaryRangeExpr:
		return evalPKFilterBinaryRangeExpr(inner, targets)
	case *pkFilterUnsupportedExpr:
		return pkMatchSet{all: true}
	default:
		return pkMatchSet{all: true}
	}
}

func evalPKFilterTermExpr(expr *pkFilterTermExpr, targets []PKFilterTarget) pkMatchSet {
	matched := typeutil.NewSet[int64]()
	cache := storage.NewBatchLocationsCache(expr.values)
	for _, seg := range targets {
		var hits []bool
		if seg.PkCandidateExist() {
			hits = seg.BatchPkExist(cache)
		}
		for idx, value := range expr.values {
			batchHit := true
			if hits != nil {
				batchHit = idx < len(hits) && hits[idx]
			}
			if pkEqualMatchesTarget(seg, value, batchHit) {
				matched.Insert(seg.ID())
				break
			}
		}
	}
	return pkMatchSet{ids: matched}
}

func evalPKFilterUnaryRangeExpr(expr *pkFilterUnaryRangeExpr, targets []PKFilterTarget) pkMatchSet {
	matched := typeutil.NewSet[int64]()
	for _, seg := range targets {
		if pkUnaryRangeMatchesTarget(seg, expr.op, expr.value) {
			matched.Insert(seg.ID())
		}
	}
	return pkMatchSet{ids: matched}
}

func evalPKFilterBinaryRangeExpr(expr *pkFilterBinaryRangeExpr, targets []PKFilterTarget) pkMatchSet {
	matched := typeutil.NewSet[int64]()
	for _, seg := range targets {
		if pkBinaryRangeMatchesTarget(seg, expr) {
			matched.Insert(seg.ID())
		}
	}
	return pkMatchSet{ids: matched}
}

func intersectPKMatches(left, right pkMatchSet) pkMatchSet {
	if left.all {
		return right
	}
	if right.all {
		return left
	}
	if len(left.ids) > len(right.ids) {
		left, right = right, left
	}
	ids := typeutil.NewSet[int64]()
	for id := range left.ids {
		if right.ids.Contain(id) {
			ids.Insert(id)
		}
	}
	return pkMatchSet{ids: ids}
}

func unionPKMatches(left, right pkMatchSet) pkMatchSet {
	if left.all || right.all {
		return pkMatchSet{all: true}
	}
	ids := typeutil.NewSet[int64]()
	for id := range left.ids {
		ids.Insert(id)
	}
	for id := range right.ids {
		ids.Insert(id)
	}
	return pkMatchSet{ids: ids}
}

func pkEqualMatchesTarget(seg PKFilterTarget, value storage.PrimaryKey, batchHit bool) bool {
	if seg.PkCandidateExist() && !batchHit {
		return false
	}
	minPk, maxPk, ok := pkTargetMinMax(seg)
	if !ok {
		return true
	}
	return minPk.LE(value) && maxPk.GE(value)
}

func pkUnaryRangeMatchesTarget(seg PKFilterTarget, op planpb.OpType, value storage.PrimaryKey) bool {
	minPk, maxPk, ok := pkTargetMinMax(seg)
	if !ok {
		return true
	}
	switch op {
	case planpb.OpType_GreaterThan:
		return !maxPk.LE(value)
	case planpb.OpType_GreaterEqual:
		return !maxPk.LT(value)
	case planpb.OpType_LessThan:
		return !minPk.GE(value)
	case planpb.OpType_LessEqual:
		return !minPk.GT(value)
	default:
		return true
	}
}

func pkBinaryRangeMatchesTarget(seg PKFilterTarget, expr *pkFilterBinaryRangeExpr) bool {
	minPk, maxPk, ok := pkTargetMinMax(seg)
	if !ok {
		return true
	}
	if expr.lower.present {
		if expr.lower.inclusive {
			if maxPk.LT(expr.lower.value) {
				return false
			}
		} else if maxPk.LE(expr.lower.value) {
			return false
		}
	}
	if expr.upper.present {
		if expr.upper.inclusive {
			if minPk.GT(expr.upper.value) {
				return false
			}
		} else if minPk.GE(expr.upper.value) {
			return false
		}
	}
	return true
}

func pkTargetMinMax(seg PKFilterTarget) (storage.PrimaryKey, storage.PrimaryKey, bool) {
	minPk := seg.GetMinPk()
	maxPk := seg.GetMaxPk()
	if minPk == nil || maxPk == nil {
		return nil, nil, false
	}
	return *minPk, *maxPk, true
}
