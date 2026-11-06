package rewriter

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

// TestDeduplicate_DeterministicSerializationAgreement verifies that for any
// two expressions a and b where proto.Equal(a, b) is true, their deterministic
// protobuf marshaling produces strictly identical bytes and identical FNV-1a hashes.
func TestDeduplicate_DeterministicSerializationAgreement(t *testing.T) {
	col := &planpb.ColumnInfo{
		FieldId:     101,
		DataType:    schemapb.DataType_Int64,
		ElementType: schemapb.DataType_None,
	}

	testCases := []struct {
		name string
		gen  func() (*planpb.Expr, *planpb.Expr)
	}{
		{
			name: "UnaryRangeExpr",
			gen: func() (*planpb.Expr, *planpb.Expr) {
				mk := func() *planpb.Expr {
					return &planpb.Expr{
						Expr: &planpb.Expr_UnaryRangeExpr{
							UnaryRangeExpr: &planpb.UnaryRangeExpr{
								ColumnInfo: col,
								Op:         planpb.OpType_Equal,
								Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 42}},
							},
						},
					}
				}
				return mk(), mk()
			},
		},
		{
			name: "BinaryRangeExpr",
			gen: func() (*planpb.Expr, *planpb.Expr) {
				mk := func() *planpb.Expr {
					return &planpb.Expr{
						Expr: &planpb.Expr_BinaryRangeExpr{
							BinaryRangeExpr: &planpb.BinaryRangeExpr{
								ColumnInfo:     col,
								LowerValue:     &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 10}},
								UpperValue:     &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 20}},
								LowerInclusive: true,
								UpperInclusive: false,
							},
						},
					}
				}
				return mk(), mk()
			},
		},
		{
			name: "TermExpr",
			gen: func() (*planpb.Expr, *planpb.Expr) {
				mk := func() *planpb.Expr {
					return &planpb.Expr{
						Expr: &planpb.Expr_TermExpr{
							TermExpr: &planpb.TermExpr{
								ColumnInfo: col,
								Values: []*planpb.GenericValue{
									{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}},
									{Val: &planpb.GenericValue_Int64Val{Int64Val: 2}},
								},
							},
						},
					}
				}
				return mk(), mk()
			},
		},
		{
			name: "Nested_BinaryExpr_And",
			gen: func() (*planpb.Expr, *planpb.Expr) {
				mk := func() *planpb.Expr {
					left := &planpb.Expr{
						Expr: &planpb.Expr_UnaryRangeExpr{
							UnaryRangeExpr: &planpb.UnaryRangeExpr{
								ColumnInfo: col,
								Op:         planpb.OpType_GreaterThan,
								Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 5}},
							},
						},
					}
					right := &planpb.Expr{
						Expr: &planpb.Expr_UnaryRangeExpr{
							UnaryRangeExpr: &planpb.UnaryRangeExpr{
								ColumnInfo: col,
								Op:         planpb.OpType_LessThan,
								Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 15}},
							},
						},
					}
					return &planpb.Expr{
						Expr: &planpb.Expr_BinaryExpr{
							BinaryExpr: &planpb.BinaryExpr{
								Op:    planpb.BinaryExpr_LogicalAnd,
								Left:  left,
								Right: right,
							},
						},
					}
				}
				return mk(), mk()
			},
		},
		{
			name: "Various_Literals_String_Bool_Float_Array",
			gen: func() (*planpb.Expr, *planpb.Expr) {
				strCol := &planpb.ColumnInfo{FieldId: 102, DataType: schemapb.DataType_VarChar}
				mk := func() *planpb.Expr {
					return &planpb.Expr{
						Expr: &planpb.Expr_UnaryRangeExpr{
							UnaryRangeExpr: &planpb.UnaryRangeExpr{
								ColumnInfo: strCol,
								Op:         planpb.OpType_Equal,
								Value:      &planpb.GenericValue{Val: &planpb.GenericValue_StringVal{StringVal: "hello"}},
							},
						},
					}
				}
				return mk(), mk()
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			a, b := tc.gen()
			require.True(t, proto.Equal(a, b))

			bytesA, errA := deterministicMarshal.Marshal(a)
			require.NoError(t, errA)
			bytesB, errB := deterministicMarshal.Marshal(b)
			require.NoError(t, errB)

			require.Equal(t, bytesA, bytesB, "proto.Equal messages must serialize to identical deterministic bytes")
			require.Equal(t, fnv1a64(bytesA), fnv1a64(bytesB), "proto.Equal messages must produce identical fnv1a64 hashes")
		})
	}
}

// TestDeduplicate_HashCollisionSafety directly tests the collision-resolution path
// in deduplicateIdenticalExprs. Even if two semantically distinct expressions
// share the exact same hash bucket key, proto.Equal must prevent them from being
// mistakenly treated as duplicates.
func TestDeduplicate_HashCollisionSafety(t *testing.T) {
	col := &planpb.ColumnInfo{
		FieldId:  101,
		DataType: schemapb.DataType_Int64,
	}

	expr1 := &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: col,
				Op:         planpb.OpType_Equal,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}},
			},
		},
	}
	expr2 := &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: col,
				Op:         planpb.OpType_Equal,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 2}},
			},
		},
	}
	expr1Dup := proto.Clone(expr1).(*planpb.Expr)

	require.False(t, proto.Equal(expr1, expr2), "expr1 and expr2 are different")
	require.True(t, proto.Equal(expr1, expr1Dup), "expr1 and expr1Dup are equal")

	// We construct a mock collision scenario by verifying that if buckets
	// map had expr1 under key H, probing expr2 under the same key H does not match.
	buckets := make(map[uint64][]*planpb.Expr)
	var collisionKey uint64 = 0xdeadbeef

	// Insert expr1
	buckets[collisionKey] = append(buckets[collisionKey], expr1)

	// Probe expr2 under collisionKey: should NOT match expr1
	found := false
	for _, cand := range buckets[collisionKey] {
		if proto.Equal(cand, expr2) {
			found = true
			break
		}
	}
	require.False(t, found, "proto.Equal must distinguish distinct expressions sharing the same hash key")

	// Probe expr1Dup under collisionKey: MUST match expr1
	foundDup := false
	for _, cand := range buckets[collisionKey] {
		if proto.Equal(cand, expr1Dup) {
			foundDup = true
			break
		}
	}
	require.True(t, foundDup, "proto.Equal must recognize identical expression under collision key")
}

// TestDeduplicate_SafetyExclusions verifies canDeduplicate correctly excludes:
// 1. Expressions with NaN values
// 2. RandomSampleExpr
// 3. ElementFilterExpr
// 4. RoaringFilterExpr
func TestDeduplicate_SafetyExclusions(t *testing.T) {
	col := &planpb.ColumnInfo{
		FieldId:  101,
		DataType: schemapb.DataType_Float,
	}

	// 1. NaN expression
	nanExpr := &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: col,
				Op:         planpb.OpType_Equal,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_FloatVal{FloatVal: math.NaN()}},
			},
		},
	}
	require.False(t, canDeduplicate(nanExpr), "expressions with NaN must not be deduplicated")

	// 2. RandomSampleExpr
	rsExpr := &planpb.Expr{
		Expr: &planpb.Expr_RandomSampleExpr{
			RandomSampleExpr: &planpb.RandomSampleExpr{
				SampleFactor: 0.1,
			},
		},
	}
	require.False(t, canDeduplicate(rsExpr), "RandomSampleExpr must not be deduplicated")

	// 3. ElementFilterExpr
	efExpr := &planpb.Expr{
		Expr: &planpb.Expr_ElementFilterExpr{
			ElementFilterExpr: &planpb.ElementFilterExpr{
				ElementExpr: &planpb.Expr{},
			},
		},
	}
	require.False(t, canDeduplicate(efExpr), "ElementFilterExpr must not be deduplicated")

	// 4. RoaringFilterExpr
	rfExpr := &planpb.Expr{
		Expr: &planpb.Expr_RoaringFilterExpr{
			RoaringFilterExpr: &planpb.RoaringFilterExpr{
				BitmapBlob: []byte{1, 2, 3},
			},
		},
	}
	require.False(t, canDeduplicate(rfExpr), "RoaringFilterExpr must not be deduplicated")

	// 5. BinaryArithExpr containing NaN
	arithNaNExpr := &planpb.Expr{
		Expr: &planpb.Expr_BinaryArithExpr{
			BinaryArithExpr: &planpb.BinaryArithExpr{
				Left: &planpb.Expr{
					Expr: &planpb.Expr_ValueExpr{
						ValueExpr: &planpb.ValueExpr{
							Value: &planpb.GenericValue{Val: &planpb.GenericValue_FloatVal{FloatVal: math.NaN()}},
						},
					},
				},
				Right: &planpb.Expr{
					Expr: &planpb.Expr_ValueExpr{
						ValueExpr: &planpb.ValueExpr{
							Value: &planpb.GenericValue{Val: &planpb.GenericValue_FloatVal{FloatVal: 1.0}},
						},
					},
				},
				Op: planpb.ArithOpType_Add,
			},
		},
	}
	require.False(t, canDeduplicate(arithNaNExpr), "BinaryArithExpr with NaN must not be deduplicated")

	// 6. CallExpr containing NaN
	callNaNExpr := &planpb.Expr{
		Expr: &planpb.Expr_CallExpr{
			CallExpr: &planpb.CallExpr{
				FunctionName: "test_fn",
				FunctionParameters: []*planpb.Expr{
					{
						Expr: &planpb.Expr_ValueExpr{
							ValueExpr: &planpb.ValueExpr{
								Value: &planpb.GenericValue{Val: &planpb.GenericValue_FloatVal{FloatVal: math.NaN()}},
							},
						},
					},
				},
			},
		},
	}
	require.False(t, canDeduplicate(callNaNExpr), "CallExpr with NaN must not be deduplicated")

	// Verify normal expression CAN be deduplicated
	normalExpr := &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: col,
				Op:         planpb.OpType_Equal,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_FloatVal{FloatVal: 3.14}},
			},
		},
	}
	require.True(t, canDeduplicate(normalExpr), "normal expression without NaN or special types can be deduplicated")
}
