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
	"strconv"
	"strings"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"

	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	SegmentPkFilterModeSegment = "segment"
	SegmentPkFilterModeHints   = "hints"
)

// SegmentHint holds per-segment PK hint results for bloom-filter-based optimization.
type SegmentHint struct {
	hints           map[int64]*planpb.SegmentPkHint
	skippedSegments map[int64]struct{}
	filteredPkCount int
}

func collectUniqueSegmentIDs(sealed []SnapshotItem, growing []SegmentEntry) []int64 {
	segmentSet := typeutil.NewSet[int64]()
	segmentIDs := make([]int64, 0, len(growing))
	for _, item := range sealed {
		for _, seg := range item.Segments {
			if !segmentSet.Contain(seg.SegmentID) {
				segmentSet.Insert(seg.SegmentID)
				segmentIDs = append(segmentIDs, seg.SegmentID)
			}
		}
	}
	for _, seg := range growing {
		if !segmentSet.Contain(seg.SegmentID) {
			segmentSet.Insert(seg.SegmentID)
			segmentIDs = append(segmentIDs, seg.SegmentID)
		}
	}
	return segmentIDs
}

func buildSegmentHintFromPlan(plan *planpb.PlanNode, partitionIDs []int64, sealed []SnapshotItem, growing []SegmentEntry) *SegmentHint {
	// Extract optimizable PK values from plan (supports IN, =, AND intersection, OR union)
	originalValues, pkValues, hasPkConstraint := extractOptimizablePkValues(plan)
	if !hasPkConstraint {
		return nil
	}

	segmentIDs := collectUniqueSegmentIDs(sealed, growing)
	if len(segmentIDs) == 0 {
		return nil
	}

	// Empty intersection (impossible predicate) — skip all segments.
	if len(originalValues) == 0 {
		skippedSegments := make(map[int64]struct{}, len(segmentIDs))
		for _, segID := range segmentIDs {
			skippedSegments[segID] = struct{}{}
		}
		return &SegmentHint{skippedSegments: skippedSegments}
	}

	// Compute single partition ID for bloom filter check.
	// For multi-partition queries, we fall back to AllPartitionsID which checks
	// all segments regardless of partition. This is safe (no false negatives)
	// but less precise than per-partition checking.
	partitionID := partitionForPkFilter(partitionIDs)

	// Batch check bloom filter for all PKs
	bfResults := BatchGetFromSegments(pkValues, partitionID, sealed, growing)
	if len(bfResults) == 0 {
		return nil
	}
	hints := make(map[int64]*planpb.SegmentPkHint, len(segmentIDs))
	skippedSegments := make(map[int64]struct{})
	filteredPkCount := 0

	for _, segID := range segmentIDs {
		hits, exists := bfResults[segID]
		if !exists || len(hits) != len(originalValues) {
			// Segment not in BF results or unexpected length, emit empty hint as fallback marker.
			hints[segID] = &planpb.SegmentPkHint{SegmentId: segID}
			continue
		}

		filteredValues := make([]*planpb.GenericValue, 0, len(originalValues))
		allHit := true
		for i, hit := range hits {
			if hit {
				filteredValues = append(filteredValues, originalValues[i])
			} else {
				allHit = false
			}
		}

		if len(filteredValues) == 0 {
			skippedSegments[segID] = struct{}{}
			continue
		}

		if allHit {
			// All PKs matched, emit empty hint (fallback to original values).
			hints[segID] = &planpb.SegmentPkHint{SegmentId: segID}
			continue
		}

		hints[segID] = &planpb.SegmentPkHint{
			SegmentId:        segID,
			FilteredPkValues: filteredValues,
		}
		filteredPkCount += len(filteredValues)
	}

	return &SegmentHint{
		hints:           hints,
		skippedSegments: skippedSegments,
		filteredPkCount: filteredPkCount,
	}
}

type candidateMinMaxProvider interface {
	GetMinPk() *storage.PrimaryKey
	GetMaxPk() *storage.PrimaryKey
}

type candidateStatsProvider interface {
	Stats() *storage.PkStatistics
	BloomFilterExist() bool
}

// extractPlanPredicates returns scalar predicates from either Query or VectorAnns plan.
// This is the common entry for both segment-level pruning and per-segment hint generation.
func extractPlanPredicates(plan *planpb.PlanNode) *planpb.Expr {
	if plan == nil {
		return nil
	}
	if query := plan.GetQuery(); query != nil {
		return query.GetPredicates()
	}
	if vectorAnns := plan.GetVectorAnns(); vectorAnns != nil {
		return vectorAnns.GetPredicates()
	}
	return nil
}

// partitionForPkFilter follows the same partition behavior as existing PK hint logic:
// only a single partition can be used for precise BF lookup.
// For multi-partition queries we conservatively fall back to all partitions, which
// is correctness-safe but may be less selective.
func partitionForPkFilter(partitionIDs []int64) int64 {
	partitionID := int64(common.AllPartitionsID)
	if len(partitionIDs) == 1 {
		partitionID = partitionIDs[0]
	}
	return partitionID
}

// filterSegmentsByPartitions explicitly filters snapshots by partitionIDs.
// This makes multi-partition queries more selective when callers provide a wider
// segment list than requested partitions.
func filterSegmentsByPartitions(partitionIDs []int64, sealed []SnapshotItem, growing []SegmentEntry) ([]SnapshotItem, []SegmentEntry) {
	if len(partitionIDs) == 0 {
		return sealed, growing
	}
	// AllPartitionsID means "no partition filtering".
	for _, partitionID := range partitionIDs {
		if partitionID == int64(common.AllPartitionsID) {
			return sealed, growing
		}
	}

	partitionSet := typeutil.NewSet(partitionIDs...)

	filteredSealed := make([]SnapshotItem, 0, len(sealed))
	for _, item := range sealed {
		filtered := make([]SegmentEntry, 0, len(item.Segments))
		for _, entry := range item.Segments {
			if partitionSet.Contain(entry.PartitionID) {
				filtered = append(filtered, entry)
			}
		}
		if len(filtered) > 0 {
			filteredSealed = append(filteredSealed, SnapshotItem{
				NodeID:   item.NodeID,
				Segments: filtered,
			})
		}
	}

	filteredGrowing := make([]SegmentEntry, 0, len(growing))
	for _, entry := range growing {
		if partitionSet.Contain(entry.PartitionID) {
			filteredGrowing = append(filteredGrowing, entry)
		}
	}

	return filteredSealed, filteredGrowing
}

// buildSkippedSegmentsByPredicates performs segment-level pruning based on PK-related predicates.
// It is intentionally conservative: unsupported expressions keep segment as "possible hit".
//
// For PK TermExpr, this pass only uses min/max pruning and skips BF checks to avoid
// duplicated BF work with pass-2 hint building (which already runs BatchPkExist).
func buildSkippedSegmentsByPredicates(predicates *planpb.Expr, partitionIDs []int64, sealed []SnapshotItem, growing []SegmentEntry) map[int64]struct{} {
	if predicates == nil {
		return nil
	}

	targetPartitionID := partitionForPkFilter(partitionIDs)
	visited := typeutil.NewSet[int64]()
	skipped := make(map[int64]struct{})

	evalEntry := func(entry SegmentEntry) {
		segID := entry.SegmentID
		if visited.Contain(segID) {
			return
		}
		if targetPartitionID != common.AllPartitionsID && entry.PartitionID != targetPartitionID {
			return
		}
		if entry.Offline || entry.Candidate == nil {
			return
		}

		visited.Insert(segID)
		if !evalExprWithCandidate(predicates, entry.Candidate) {
			skipped[segID] = struct{}{}
		}
	}

	for _, item := range sealed {
		for _, entry := range item.Segments {
			evalEntry(entry)
		}
	}
	for _, entry := range growing {
		evalEntry(entry)
	}

	if len(skipped) == 0 {
		return nil
	}
	return skipped
}

// evalExprWithCandidate evaluates whether a segment candidate may satisfy the predicate.
// Returning false means the segment can be safely skipped.
//
// Note: by product decision we only optimize conjunction (AND). OR/other binary ops
// are treated as "unknown => possible hit" to avoid aggressive pruning.
func evalExprWithCandidate(expr *planpb.Expr, candidate pkoracle.Candidate) bool {
	if expr == nil || candidate == nil {
		return true
	}

	switch e := expr.GetExpr().(type) {
	case *planpb.Expr_BinaryExpr:
		switch e.BinaryExpr.GetOp() {
		case planpb.BinaryExpr_LogicalAnd:
			return evalExprWithCandidate(e.BinaryExpr.GetLeft(), candidate) &&
				evalExprWithCandidate(e.BinaryExpr.GetRight(), candidate)
		default:
			// Only optimize conjunction for segment-level PKFilter.
			// For OR/other ops, keep conservative behavior (no pruning).
			return true
		}
	case *planpb.Expr_UnaryRangeExpr:
		unary := e.UnaryRangeExpr
		if unary.GetColumnInfo() == nil || !unary.GetColumnInfo().GetIsPrimaryKey() || unary.GetValue() == nil {
			return true
		}
		pk, ok := genericValueToPrimaryKey(unary.GetColumnInfo().GetDataType(), unary.GetValue())
		if !ok {
			return true
		}
		return evalPkOpWithCandidateMinMaxOnly(candidate, pk, unary.GetOp())
	case *planpb.Expr_TermExpr:
		term := e.TermExpr
		if term.GetColumnInfo() == nil || !term.GetColumnInfo().GetIsPrimaryKey() {
			return true
		}
		values := term.GetValues()
		// Keep historical sparse-filter semantics for empty IN list.
		if values == nil {
			return false
		}
		shouldKeep := true
		for _, value := range values {
			pk, ok := genericValueToPrimaryKey(term.GetColumnInfo().GetDataType(), value)
			if !ok {
				return true
			}
			// Pass-1 term pruning uses only min/max. BF is deferred to pass-2 hint building.
			shouldKeep = evalPkOpWithCandidateMinMaxOnly(candidate, pk, planpb.OpType_Equal)
			if shouldKeep {
				break
			}
		}
		return shouldKeep
	default:
		return true
	}
}

// genericValueToPrimaryKey converts plan generic value to primary key used by BF/min-max checks.
func genericValueToPrimaryKey(dt schemapb.DataType, value *planpb.GenericValue) (storage.PrimaryKey, bool) {
	switch dt {
	case schemapb.DataType_Int64:
		return storage.NewInt64PrimaryKey(value.GetInt64Val()), true
	case schemapb.DataType_VarChar:
		return storage.NewVarCharPrimaryKey(value.GetStringVal()), true
	default:
		log.Warn("unknown pk type, skip segment-level PKFilter",
			zap.Int("type", int(dt)))
		return nil, false
	}
}

// candidateMinMax extracts PK min/max from candidate for range pruning.
// It supports both segment candidates and BloomFilterSet candidates.
func candidateMinMax(candidate pkoracle.Candidate) (storage.PrimaryKey, storage.PrimaryKey, bool) {
	if provider, ok := candidate.(candidateMinMaxProvider); ok {
		minPk := provider.GetMinPk()
		maxPk := provider.GetMaxPk()
		if minPk != nil && maxPk != nil {
			return *minPk, *maxPk, true
		}
	}

	if provider, ok := candidate.(candidateStatsProvider); ok {
		stats := provider.Stats()
		if stats != nil && stats.MinPK != nil && stats.MaxPK != nil {
			return stats.MinPK, stats.MaxPK, true
		}
	}
	return nil, nil, false
}

func mayMatchByMinMax(op planpb.OpType, pk storage.PrimaryKey, minPk storage.PrimaryKey, maxPk storage.PrimaryKey, hasMinMax bool) bool {
	switch op {
	case planpb.OpType_Equal:
		return !(hasMinMax && (minPk.GT(pk) || maxPk.LT(pk)))
	case planpb.OpType_GreaterThan:
		return !(hasMinMax && maxPk.LE(pk))
	case planpb.OpType_GreaterEqual:
		return !(hasMinMax && maxPk.LT(pk))
	case planpb.OpType_LessThan:
		return !(hasMinMax && minPk.GE(pk))
	case planpb.OpType_LessEqual:
		return !(hasMinMax && minPk.GT(pk))
	default:
		return true
	}
}

// evalPkOpWithCandidateMinMaxOnly is the range-only variant used in pass-1 pruning.
// It avoids duplicate BF checks for PK TermExpr because pass-2 already performs BF-based
// pruning while constructing per-segment hints.
func evalPkOpWithCandidateMinMaxOnly(candidate pkoracle.Candidate, pk storage.PrimaryKey, op planpb.OpType) bool {
	minPk, maxPk, hasMinMax := candidateMinMax(candidate)
	return mayMatchByMinMax(op, pk, minPk, maxPk, hasMinMax)
}

func observeSegmentPkHintMetrics(collectionID int64, queryType string, hitSegments int, skippedSegments int, filteredPkCount int) {
	nodeID := strconv.FormatInt(paramtable.GetNodeID(), 10)
	collectionIDLabel := strconv.FormatInt(collectionID, 10)

	metrics.QueryNodeSegmentPkHintHitSegmentNum.WithLabelValues(nodeID, collectionIDLabel, queryType).Observe(float64(hitSegments))
	metrics.QueryNodeSegmentPkHintSkippedSegmentNum.WithLabelValues(nodeID, collectionIDLabel, queryType).Observe(float64(skippedSegments))
	metrics.QueryNodeSegmentPkHintFilteredPkNum.WithLabelValues(nodeID, collectionIDLabel, queryType).Observe(float64(filteredPkCount))
}

// FilterSegments removes skipped segments from sealed and growing lists.
func (h *SegmentHint) FilterSegments(sealed []SnapshotItem, growing []SegmentEntry) ([]SnapshotItem, []SegmentEntry) {
	if len(h.skippedSegments) == 0 {
		return sealed, growing
	}

	filteredSealed := make([]SnapshotItem, 0, len(sealed))
	for _, item := range sealed {
		filteredSegs := make([]SegmentEntry, 0, len(item.Segments))
		for _, seg := range item.Segments {
			if _, skip := h.skippedSegments[seg.SegmentID]; !skip {
				filteredSegs = append(filteredSegs, seg)
			}
		}
		if len(filteredSegs) > 0 {
			filteredSealed = append(filteredSealed, SnapshotItem{
				NodeID:   item.NodeID,
				Segments: filteredSegs,
			})
		}
	}

	filteredGrowing := make([]SegmentEntry, 0, len(growing))
	for _, seg := range growing {
		if _, skip := h.skippedSegments[seg.SegmentID]; !skip {
			filteredGrowing = append(filteredGrowing, seg)
		}
	}

	return filteredSealed, filteredGrowing
}

// GetHintsForSegments looks up per-segment hints by segment IDs
// and returns a SegmentPkHintList proto message.
func (h *SegmentHint) GetHintsForSegments(segmentIDs []int64) *planpb.SegmentPkHintList {
	if len(h.hints) == 0 {
		return nil
	}

	var filtered []*planpb.SegmentPkHint
	for _, id := range segmentIDs {
		if hint, ok := h.hints[id]; ok {
			filtered = append(filtered, hint)
		}
	}

	if len(filtered) == 0 {
		return nil
	}

	return &planpb.SegmentPkHintList{Hints: filtered}
}

// pkConstraint represents a set of specific PK values extracted from the expression tree.
// nil means "no PK constraint found" (unconstrained / any PK can match).
type pkConstraint struct {
	dataType schemapb.DataType
	values   []*planpb.GenericValue
}

// extractOptimizablePkValues finds optimizable PK values from plan predicates.
// Supports: pk IN [...], pk = value, OR (union), AND (intersection), NOT (ignore).
//
// Returns:
//   - (nil, nil, false): no PK constraint found (unconstrained)
//   - ([], [], true):    impossible predicate (empty intersection) — all segments skippable
//   - (vals, pks, true): valid PK values extracted
func extractOptimizablePkValues(plan *planpb.PlanNode) ([]*planpb.GenericValue, []storage.PrimaryKey, bool) {
	predicates := extractPlanPredicates(plan)
	if predicates == nil {
		return nil, nil, false
	}

	constraint := extractPkConstraint(predicates)
	if constraint == nil {
		return nil, nil, false
	}
	if len(constraint.values) == 0 {
		// Empty intersection (e.g. pk IN [1,2] AND pk IN [3,4]) — impossible predicate.
		return nil, nil, true
	}

	pks := convertGenericValuesToPKs(constraint.dataType, constraint.values)
	if len(pks) == 0 {
		return nil, nil, false
	}
	return constraint.values, pks, true
}

// extractPkConstraint recursively walks the expression tree and extracts a PK value constraint.
//
// Returns:
//   - non-nil: PK is constrained to specific values
//   - nil: no PK constraint (unconstrained), or can't determine
//
// Semantics for each expression type:
//   - TermExpr(pk, values):      pk IN [values]
//   - UnaryRangeExpr(pk, Equal): pk = value  (treated as pk IN [value])
//   - AND(left, right):          intersect(left, right)
//   - OR(left, right):           union(left, right) — only if both sides have PK constraints
//   - NOT(inner):                ignore inner PK constraint (safe: non-PK NOT doesn't block optimization)
//   - Other:                     no PK constraint (nil)
func extractPkConstraint(expr *planpb.Expr) *pkConstraint {
	if expr == nil {
		return nil
	}

	switch e := expr.GetExpr().(type) {
	case *planpb.Expr_TermExpr:
		termExpr := e.TermExpr
		col := termExpr.GetColumnInfo()
		if col == nil || !col.GetIsPrimaryKey() {
			return nil
		}
		dt := col.GetDataType()
		if dt != schemapb.DataType_Int64 && dt != schemapb.DataType_VarChar {
			return nil
		}
		if len(termExpr.GetValues()) == 0 {
			return nil
		}
		return &pkConstraint{dataType: dt, values: termExpr.GetValues()}

	case *planpb.Expr_UnaryRangeExpr:
		unary := e.UnaryRangeExpr
		if unary.GetOp() != planpb.OpType_Equal {
			return nil
		}
		col := unary.GetColumnInfo()
		if col == nil || !col.GetIsPrimaryKey() || unary.GetValue() == nil {
			return nil
		}
		dt := col.GetDataType()
		if dt != schemapb.DataType_Int64 && dt != schemapb.DataType_VarChar {
			return nil
		}
		return &pkConstraint{dataType: dt, values: []*planpb.GenericValue{unary.GetValue()}}

	case *planpb.Expr_BinaryExpr:
		left := extractPkConstraint(e.BinaryExpr.GetLeft())
		right := extractPkConstraint(e.BinaryExpr.GetRight())

		switch e.BinaryExpr.GetOp() {
		case planpb.BinaryExpr_LogicalAnd:
			return intersectPkConstraints(left, right)
		case planpb.BinaryExpr_LogicalOr:
			return unionPkConstraints(left, right)
		default:
			return nil
		}

	case *planpb.Expr_UnaryExpr:
		// NOT: ignore PK constraint from inner expression.
		// This allows "pk IN [1,2] AND NOT(status=1)" to still optimize on pk.
		return nil

	default:
		return nil
	}
}

// intersectPkConstraints computes the intersection of two PK constraints.
//
//	AND(Some(A), Some(B)) → Some(A ∩ B)
//	AND(Some(A), nil)     → Some(A)   — non-PK side doesn't narrow PK
//	AND(nil, Some(B))     → Some(B)
//	AND(nil, nil)          → nil
func intersectPkConstraints(a, b *pkConstraint) *pkConstraint {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	bSet := make(map[string]struct{}, len(b.values))
	for _, v := range b.values {
		bSet[pkValueKey(b.dataType, v)] = struct{}{}
	}
	var result []*planpb.GenericValue
	for _, v := range a.values {
		if _, ok := bSet[pkValueKey(a.dataType, v)]; ok {
			result = append(result, v)
		}
	}
	// Empty intersection means the predicate is impossible (no rows can match).
	// Return an empty constraint (not nil) so callers can distinguish
	// "empty set" from "unconstrained".
	return &pkConstraint{dataType: a.dataType, values: result}
}

// unionPkConstraints computes the union of two PK constraints.
//
//	OR(Some(A), Some(B)) → Some(A ∪ B)
//	OR(Some(A), nil)     → nil  — OR with unconstrained = unconstrained
//	OR(nil, Some(B))     → nil
//	OR(nil, nil)          → nil
func unionPkConstraints(a, b *pkConstraint) *pkConstraint {
	if a == nil || b == nil {
		return nil
	}
	seen := make(map[string]struct{}, len(a.values)+len(b.values))
	var result []*planpb.GenericValue
	for _, v := range a.values {
		key := pkValueKey(a.dataType, v)
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			result = append(result, v)
		}
	}
	for _, v := range b.values {
		key := pkValueKey(b.dataType, v)
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			result = append(result, v)
		}
	}
	return &pkConstraint{dataType: a.dataType, values: result}
}

// pkValueKey returns a comparable string key for a GenericValue under a given PK data type.
func pkValueKey(dt schemapb.DataType, v *planpb.GenericValue) string {
	switch dt {
	case schemapb.DataType_Int64:
		return strconv.FormatInt(v.GetInt64Val(), 10)
	case schemapb.DataType_VarChar:
		return v.GetStringVal()
	default:
		return ""
	}
}

// buildAndApplySegmentHints builds PK hints from bloom filters and removes
// fully-skippable segments from sealed/growing lists.
// Returns the hint (nil if mode is segment-only or not applicable) and the filtered segment lists.
func buildAndApplySegmentHints(
	serializedPlan []byte,
	pkFilterHint int32,
	partitionIDs []int64,
	sealed []SnapshotItem,
	growing []SegmentEntry,
	collectionID int64,
	queryType string,
) (*SegmentHint, []SnapshotItem, []SegmentEntry) {
	if !paramtable.Get().QueryNodeCfg.EnableSegmentPkHint.GetAsBool() {
		return nil, sealed, growing
	}

	if pkFilterHint == common.PkFilterHintNoPkFilter {
		// Proxy confirmed no PK term filter in this query, skip unmarshal entirely.
		return nil, sealed, growing
	}

	plan := &planpb.PlanNode{}
	if err := proto.Unmarshal(serializedPlan, plan); err != nil {
		log.Warn("buildAndApplySegmentHints: failed to unmarshal plan, skip PKFilter", zap.Error(err))
		return nil, sealed, growing
	}

	// Apply explicit partition filtering for both pass-1 and pass-2.
	// This keeps multi-partition queries selective even if upstream segment lists are wider.
	sealed, growing = filterSegmentsByPartitions(partitionIDs, sealed, growing)

	// Stage 1: do segment-level PK pruning for all supported AND-based PK expressions.
	skippedByPredicate := buildSkippedSegmentsByPredicates(extractPlanPredicates(plan), partitionIDs, sealed, growing)
	if len(skippedByPredicate) > 0 {
		sealed, growing = (&SegmentHint{skippedSegments: skippedByPredicate}).FilterSegments(sealed, growing)
	}

	// Stage 2: for single optimizable PK TermExpr, build per-segment hints on the remaining segments.
	segHint := buildSegmentHintFromPlan(plan, partitionIDs, sealed, growing)
	if segHint != nil {
		sealed, growing = segHint.FilterSegments(sealed, growing)
	}

	totalSkippedSegmentCount := len(skippedByPredicate)
	if segHint != nil {
		totalSkippedSegmentCount += len(segHint.skippedSegments)
	}

	if segHint != nil || totalSkippedSegmentCount > 0 {
		hitSegments := 0
		filteredPkCount := 0
		if segHint != nil {
			hitSegments = len(segHint.hints)
			filteredPkCount = segHint.filteredPkCount
		}
		observeSegmentPkHintMetrics(collectionID, queryType, hitSegments, totalSkippedSegmentCount, filteredPkCount)
	}

	mode := strings.ToLower(strings.TrimSpace(paramtable.Get().QueryNodeCfg.SegmentPkFilterMode.GetValue()))
	switch mode {
	case SegmentPkFilterModeSegment:
		// Segment-only mode: only prune skipped segments, don't pass hints to C++.
		return nil, sealed, growing
	case "", SegmentPkFilterModeHints:
		// Default mode: emit per-segment PK hints.
		return segHint, sealed, growing
	default:
		log.Warn("invalid queryNode.hintMode, fallback to hints mode",
			zap.String("mode", mode))
		return segHint, sealed, growing
	}
}

// convertGenericValuesToPKs converts GenericValue slice to PrimaryKey slice.
func convertGenericValuesToPKs(dataType schemapb.DataType, values []*planpb.GenericValue) []storage.PrimaryKey {
	pks := make([]storage.PrimaryKey, 0, len(values))
	for _, v := range values {
		switch dataType {
		case schemapb.DataType_Int64:
			pks = append(pks, storage.NewInt64PrimaryKey(v.GetInt64Val()))
		case schemapb.DataType_VarChar:
			pks = append(pks, storage.NewVarCharPrimaryKey(v.GetStringVal()))
		default:
			return nil
		}
	}
	return pks
}
