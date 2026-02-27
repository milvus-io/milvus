package benchmark

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// buildHints replicates the core PK hint-building logic for benchmark use.
func buildHints(
	pkOracle pkoracle.PkOracle,
	partitionID int64,
	plan *planpb.PlanNode,
	segmentIDs []int64,
) (segmentPkHints []*planpb.SegmentPkHint, skippedSegments []int64, hasPkTermExpr bool) {
	termExpr, pkValues := extractPkTermExpr(plan)
	if termExpr == nil || len(pkValues) == 0 {
		return nil, nil, false
	}

	bfResults := pkOracle.BatchGet(pkValues, pkoracle.WithPartitionID(partitionID))
	if len(bfResults) == 0 {
		return nil, nil, false
	}

	originalValues := termExpr.GetValues()

	for _, segID := range segmentIDs {
		hits, exists := bfResults[segID]
		if !exists {
			continue
		}

		var filteredValues []*planpb.GenericValue
		for i, hit := range hits {
			if hit && i < len(originalValues) {
				filteredValues = append(filteredValues, originalValues[i])
			}
		}

		if len(filteredValues) == 0 {
			skippedSegments = append(skippedSegments, segID)
		} else if len(filteredValues) < len(originalValues) {
			segmentPkHints = append(segmentPkHints, &planpb.SegmentPkHint{
				SegmentId:        segID,
				FilteredPkValues: filteredValues,
			})
		}
	}

	return segmentPkHints, skippedSegments, true
}

func extractPkTermExpr(plan *planpb.PlanNode) (*planpb.TermExpr, []storage.PrimaryKey) {
	var predicates *planpb.Expr
	if query := plan.GetQuery(); query != nil {
		predicates = query.GetPredicates()
	} else if vectorAnns := plan.GetVectorAnns(); vectorAnns != nil {
		predicates = vectorAnns.GetPredicates()
	}
	if predicates == nil {
		return nil, nil
	}
	return findPkTermExpr(predicates)
}

func findPkTermExpr(expr *planpb.Expr) (*planpb.TermExpr, []storage.PrimaryKey) {
	if expr == nil {
		return nil, nil
	}
	switch e := expr.GetExpr().(type) {
	case *planpb.Expr_TermExpr:
		termExpr := e.TermExpr
		if termExpr.GetColumnInfo().GetIsPrimaryKey() {
			pks := convertToPKs(termExpr)
			if len(pks) > 0 {
				return termExpr, pks
			}
		}
	case *planpb.Expr_BinaryExpr:
		if e.BinaryExpr.GetOp() == planpb.BinaryExpr_LogicalAnd {
			if term, pks := findPkTermExpr(e.BinaryExpr.GetLeft()); term != nil {
				return term, pks
			}
			return findPkTermExpr(e.BinaryExpr.GetRight())
		}
	}
	return nil, nil
}

func convertToPKs(termExpr *planpb.TermExpr) []storage.PrimaryKey {
	values := termExpr.GetValues()
	if len(values) == 0 {
		return nil
	}
	dataType := termExpr.GetColumnInfo().GetDataType()
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

// TestPkHintFilteringStats shows per-query, per-segment PK filtering details.
//
//	100 segments × 10K PKs each = 1M entities
//	Query batch = 100 PKs
//	20 random queries
func TestPkHintFilteringStats(t *testing.T) {
	paramtable.Init()

	const (
		numSegments   = 100
		pksPerSegment = 10_000
		totalEntities = numSegments * pksPerSegment
		queryBatch    = 100
		numTrials     = 20
		partitionID   = int64(1)
	)

	// ── Build 100 segments with sequential, non-overlapping PKs ──
	pkOracle := pkoracle.NewPkOracle()
	segmentIDs := make([]int64, numSegments)
	for seg := 0; seg < numSegments; seg++ {
		segID := int64(seg + 1)
		segmentIDs[seg] = segID
		startPK := int64(seg * pksPerSegment)

		pks := make([]storage.PrimaryKey, pksPerSegment)
		for i := 0; i < pksPerSegment; i++ {
			pks[i] = storage.NewInt64PrimaryKey(startPK + int64(i))
		}
		bfs := pkoracle.NewBloomFilterSet(segID, partitionID, commonpb.SegmentState_Sealed)
		bfs.UpdateBloomFilter(pks)
		pkOracle.Register(bfs, partitionID)
	}

	fmt.Println()
	fmt.Println("================================================================")
	fmt.Printf(" PKFilter Per-Query Stats\n")
	fmt.Printf(" %d segments × %d PKs = %d total entities\n", numSegments, pksPerSegment, totalEntities)
	fmt.Printf(" Query batch = %d PKs,  %d queries\n", queryBatch, numTrials)
	fmt.Println("================================================================")

	rng := rand.New(rand.NewSource(42))

	// Accumulators
	var totalSkipped, totalHinted, totalUntouched int
	var totalPKChecksWith, totalPKChecksWithout int

	for trial := 0; trial < numTrials; trial++ {
		// Random 100 PKs from [0, totalEntities)
		queryPKs := make([]int64, queryBatch)
		for i := range queryPKs {
			queryPKs[i] = rng.Int63n(int64(totalEntities))
		}

		// Build Plan
		values := make([]*planpb.GenericValue, queryBatch)
		for i, pk := range queryPKs {
			values[i] = &planpb.GenericValue{
				Val: &planpb.GenericValue_Int64Val{Int64Val: pk},
			}
		}
		plan := &planpb.PlanNode{
			Node: &planpb.PlanNode_Query{
				Query: &planpb.QueryPlanNode{
					Predicates: &planpb.Expr{
						Expr: &planpb.Expr_TermExpr{
							TermExpr: &planpb.TermExpr{
								ColumnInfo: &planpb.ColumnInfo{
									FieldId:      100,
									DataType:     schemapb.DataType_Int64,
									IsPrimaryKey: true,
								},
								Values: values,
							},
						},
					},
				},
			},
		}

		// Run buildHints (replicated inline in this package)
		hints, skipped, ok := buildHints(pkOracle, partitionID, plan, segmentIDs)
		if !ok {
			t.Fatal("expected hasPkTerm = true")
		}

		skippedN := len(skipped)
		hintedN := len(hints)
		untouchedN := numSegments - skippedN - hintedN

		totalSkipped += skippedN
		totalHinted += hintedN
		totalUntouched += untouchedN

		// Compute PK checks
		pksWith := 0
		for _, h := range hints {
			pksWith += len(h.FilteredPkValues)
		}
		pksWith += untouchedN * queryBatch
		pksWithout := numSegments * queryBatch

		totalPKChecksWith += pksWith
		totalPKChecksWithout += pksWithout

		// ── Print per-query details ──────────────────────────────
		fmt.Printf("\n── Query #%02d ────────────────────────────────────────────\n", trial+1)
		fmt.Printf("  Skipped (0 PKs):    %3d segments\n", skippedN)
		fmt.Printf("  Hinted  (partial):  %3d segments\n", hintedN)
		fmt.Printf("  Untouched (all):    %3d segments\n", untouchedN)
		fmt.Printf("  Queried total:      %3d / %d  (%.0f%% segments skipped)\n",
			hintedN+untouchedN, numSegments,
			float64(skippedN)/float64(numSegments)*100)

		// Per-segment detail (only show hinted segments)
		if hintedN > 0 {
			fmt.Printf("  Hinted segment details:\n")
			for _, h := range hints {
				fmt.Printf("    seg %3d:  %d / %d PKs\n",
					h.SegmentId, len(h.FilteredPkValues), queryBatch)
			}
		}

		fmt.Printf("  PK checks: WITHOUT=%d  WITH=%d  reduction=%.1f%%\n",
			pksWithout, pksWith,
			(1-float64(pksWith)/float64(pksWithout))*100)
	}

	// ── Grand Summary ────────────────────────────────────────────
	fmt.Println()
	fmt.Println("================================================================")
	fmt.Println(" GRAND SUMMARY")
	fmt.Println("================================================================")
	n := float64(numTrials)
	fmt.Printf("  Avg skipped segments:   %5.1f / %d  (%.1f%%)\n",
		float64(totalSkipped)/n, numSegments,
		float64(totalSkipped)/n/float64(numSegments)*100)
	fmt.Printf("  Avg hinted segments:    %5.1f / %d\n",
		float64(totalHinted)/n, numSegments)
	fmt.Printf("  Avg untouched segments: %5.1f / %d\n",
		float64(totalUntouched)/n, numSegments)
	fmt.Printf("  Avg queried segments:   %5.1f / %d\n",
		float64(totalHinted+totalUntouched)/n, numSegments)
	fmt.Println()
	fmt.Printf("  WITHOUT PKFilter: %d checks/query  (%d segments × %d PKs)\n",
		numSegments*queryBatch, numSegments, queryBatch)
	fmt.Printf("  WITH    PKFilter: %.0f checks/query  (avg)\n",
		float64(totalPKChecksWith)/n)
	fmt.Printf("  Reduction:        %.1f%%\n",
		(1-float64(totalPKChecksWith)/float64(totalPKChecksWithout))*100)
}
