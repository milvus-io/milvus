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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	storage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// mockPKTarget is a simple PKFilterTarget for testing.
type mockPKTarget struct {
	id           int64
	minPk        storage.PrimaryKey
	maxPk        storage.PrimaryKey
	contained    typeutil.Set[int64]
	strContained typeutil.Set[string]
}

func newInt64Target(id, min, max int64, contained []int64) *mockPKTarget {
	return &mockPKTarget{
		id:        id,
		minPk:     storage.NewInt64PrimaryKey(min),
		maxPk:     storage.NewInt64PrimaryKey(max),
		contained: typeutil.NewSet(contained...),
	}
}

func newStringTarget(id int64, min, max string, contained []string) *mockPKTarget {
	return &mockPKTarget{
		id:           id,
		minPk:        storage.NewVarCharPrimaryKey(min),
		maxPk:        storage.NewVarCharPrimaryKey(max),
		strContained: typeutil.NewSet(contained...),
	}
}

func (m *mockPKTarget) ID() int64 { return m.id }

func (m *mockPKTarget) PkCandidateExist() bool { return true }

func (m *mockPKTarget) GetMinPk() *storage.PrimaryKey { return &m.minPk }

func (m *mockPKTarget) GetMaxPk() *storage.PrimaryKey { return &m.maxPk }

func (m *mockPKTarget) BatchPkExist(lc *storage.BatchLocationsCache) []bool {
	hits := make([]bool, 0, lc.Size())
	for _, pk := range lc.PKs() {
		switch v := pk.GetValue().(type) {
		case int64:
			hits = append(hits, m.contained.Contain(v))
		case string:
			hits = append(hits, m.strContained.Contain(v))
		default:
			hits = append(hits, false)
		}
	}
	return hits
}

func targetIDs(ids typeutil.Set[int64]) []int64 {
	result := make([]int64, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}
	return result
}

func TestCheckPKFilter(t *testing.T) {
	schema := mock_segcore.GenTestCollectionSchema("pk-filter", schemapb.DataType_Int64, true)
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)

	targets := []PKFilterTarget{
		newInt64Target(1, 0, 4, []int64{1, 2, 4}),
		newInt64Target(2, 5, 8, []int64{5, 7}),
		newInt64Target(3, 9, 12, []int64{9, 10}),
	}

	tests := []struct {
		name          string
		expr          string
		expectKept    []int64
		expectAllKept bool
	}{
		{
			name:       "equal",
			expr:       "int64Field == 5",
			expectKept: []int64{2},
		},
		{
			name:       "in",
			expr:       "int64Field IN [7, 9]",
			expectKept: []int64{2, 3},
		},
		{
			name:       "range and",
			expr:       "int64Field > 5 and int64Field < 9",
			expectKept: []int64{2},
		},
		{
			name:       "unary range greater",
			expr:       "int64Field > 8",
			expectKept: []int64{3},
		},
		{
			name:       "unary range less equal",
			expr:       "int64Field <= 4",
			expectKept: []int64{1},
		},
		{
			name:       "pk and unsupported",
			expr:       "int64Field == 5 and int8Field == -10000",
			expectKept: []int64{2},
		},
		{
			name:       "nested and or",
			expr:       "(int64Field == 5 or int64Field == 9) and (int64Field == 5 or int64Field == 10)",
			expectKept: []int64{2},
		},
		{
			name:          "pk or unsupported keeps all",
			expr:          "int64Field == 5 or int8Field == -10000",
			expectAllKept: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := planparserv2.CreateRetrievePlan(schemaHelper, tc.expr, nil)
			require.NoError(t, err)

			expr := BuildPKFilterExpr(plan, common.PkFilterHasPkFilter)
			ids, all := CheckPKFilter(expr, targets)
			if tc.expectAllKept {
				assert.True(t, all)
				return
			}
			assert.ElementsMatch(t, tc.expectKept, targetIDs(ids))
		})
	}
}

func TestBuildPKFilterExprFromSearchPlan(t *testing.T) {
	schema := mock_segcore.GenTestCollectionSchema("pk-filter-search", schemapb.DataType_Int64, true)
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)

	plan, err := planparserv2.CreateSearchPlan(schemaHelper, "int64Field == 5", "floatVectorField", nil, nil, nil)
	require.NoError(t, err)

	expr := BuildPKFilterExpr(plan, common.PkFilterHasPkFilter)
	ids, all := CheckPKFilter(expr, []PKFilterTarget{
		newInt64Target(1, 0, 4, []int64{1, 2, 4}),
		newInt64Target(2, 5, 8, []int64{5, 7}),
	})
	assert.False(t, all)
	assert.ElementsMatch(t, []int64{2}, targetIDs(ids))
}

func TestExtractPKFilterPredicates(t *testing.T) {
	t.Run("nil plan", func(t *testing.T) {
		assert.Nil(t, extractPKFilterPredicates(nil))
	})

	t.Run("query plan", func(t *testing.T) {
		schema := mock_segcore.GenTestCollectionSchema("pk-filter-query", schemapb.DataType_Int64, true)
		schemaHelper, err := typeutil.CreateSchemaHelper(schema)
		require.NoError(t, err)

		plan, err := planparserv2.CreateRetrievePlan(schemaHelper, "int64Field == 5", nil)
		require.NoError(t, err)
		require.NotNil(t, plan.GetQuery())
		assert.NotNil(t, extractPKFilterPredicates(plan))
	})
}

func TestBuildPKFilterExpr(t *testing.T) {
	schema := mock_segcore.GenTestCollectionSchema("pk-filter-plan", schemapb.DataType_Int64, true)
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)

	plan, err := planparserv2.CreateRetrievePlan(schemaHelper, "int64Field == 5", nil)
	require.NoError(t, err)

	t.Run("skip parsing when proxy says no pk filter", func(t *testing.T) {
		assert.Nil(t, BuildPKFilterExpr(plan, common.PkFilterNoPkFilter))
	})

	t.Run("parse when proxy says has pk filter", func(t *testing.T) {
		assert.NotNil(t, BuildPKFilterExpr(plan, common.PkFilterHasPkFilter))
	})

	t.Run("parse when proxy did not check", func(t *testing.T) {
		assert.NotNil(t, BuildPKFilterExpr(plan, common.PkFilterNotChecked))
	})
}

func TestCheckPKFilterVarChar(t *testing.T) {
	schema := mock_segcore.GenTestCollectionSchema("pk-filter-varchar", schemapb.DataType_VarChar, true)
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)

	plan, err := planparserv2.CreateRetrievePlan(schemaHelper, `varCharField IN ["beta", "gamma"]`, nil)
	require.NoError(t, err)

	expr := BuildPKFilterExpr(plan, common.PkFilterHasPkFilter)
	ids, all := CheckPKFilter(expr, []PKFilterTarget{
		newStringTarget(1, "alpha", "alpha", []string{"alpha"}),
		newStringTarget(2, "beta", "delta", []string{"beta"}),
		newStringTarget(3, "omega", "zeta", []string{"theta"}),
	})
	assert.False(t, all)
	assert.ElementsMatch(t, []int64{2}, targetIDs(ids))
}

func TestPkFromGenericValueUnsupportedType(t *testing.T) {
	pk, ok := pkFromGenericValue(schemapb.DataType_Bool, &planpb.GenericValue{
		Val: &planpb.GenericValue_BoolVal{BoolVal: true},
	})
	assert.False(t, ok)
	assert.Nil(t, pk)
}

func TestCheckPKFilterEmptyInputs(t *testing.T) {
	target := newInt64Target(1, 0, 4, []int64{1, 2, 4})

	_, all := CheckPKFilter(nil, []PKFilterTarget{target})
	assert.True(t, all)

	_, all = CheckPKFilter(&pkFilterUnsupportedExpr{}, nil)
	assert.True(t, all)
}

func TestCheckPKFilterAllMatched(t *testing.T) {
	expr := &pkFilterTermExpr{
		values: []storage.PrimaryKey{storage.NewInt64PrimaryKey(5)},
	}

	ids, all := CheckPKFilter(expr, []PKFilterTarget{
		newInt64Target(1, 5, 8, []int64{5}),
		newInt64Target(2, 5, 9, []int64{5, 7}),
	})
	assert.False(t, all)
	assert.True(t, ids.Contain(1))
	assert.True(t, ids.Contain(2))
}

func TestIntersectPKMatches(t *testing.T) {
	t.Run("left all returns right", func(t *testing.T) {
		right := pkMatchSet{ids: typeutil.NewSet[int64](2, 3)}
		result := intersectPKMatches(pkMatchSet{all: true}, right)
		assert.False(t, result.all)
		assert.True(t, result.ids.Contain(2))
		assert.True(t, result.ids.Contain(3))
	})

	t.Run("right all returns left", func(t *testing.T) {
		left := pkMatchSet{ids: typeutil.NewSet[int64](1)}
		result := intersectPKMatches(left, pkMatchSet{all: true})
		assert.False(t, result.all)
		assert.True(t, result.ids.Contain(1))
	})

	t.Run("empty intersection", func(t *testing.T) {
		result := intersectPKMatches(
			pkMatchSet{ids: typeutil.NewSet[int64](1)},
			pkMatchSet{ids: typeutil.NewSet[int64](2)},
		)
		assert.False(t, result.all)
		assert.Empty(t, result.ids)
	})
}

func TestPKUnaryRangeMatchesTargetWithoutMinMax(t *testing.T) {
	seg := &mockPKTarget{id: 1} // nil min/max
	seg.minPk = nil
	seg.maxPk = nil

	// When minPk/maxPk are nil pointers (GetMinPk returns pointer to nil), but here
	// our mock returns &minPk where minPk is zero value. Use a target with no candidate.
	noMinMax := &mockPKTarget{id: 1}
	noMinMax.minPk = nil
	noMinMax.maxPk = nil

	// Use a target that returns nil from GetMinPk/GetMaxPk
	nilTarget := &nilMinMaxTarget{id: 1}
	assert.True(t, pkUnaryRangeMatchesTarget(nilTarget, planpb.OpType_GreaterThan, storage.NewInt64PrimaryKey(5)))
	assert.True(t, pkUnaryRangeMatchesTarget(nilTarget, planpb.OpType_GreaterEqual, storage.NewInt64PrimaryKey(5)))
	assert.True(t, pkBinaryRangeMatchesTarget(nilTarget, &pkFilterBinaryRangeExpr{
		lower: pkFilterBound{value: storage.NewInt64PrimaryKey(1), inclusive: true, present: true},
		upper: pkFilterBound{value: storage.NewInt64PrimaryKey(10), inclusive: false, present: true},
	}))
}

type nilMinMaxTarget struct{ id int64 }

func (n *nilMinMaxTarget) ID() int64                                        { return n.id }
func (n *nilMinMaxTarget) PkCandidateExist() bool                           { return false }
func (n *nilMinMaxTarget) BatchPkExist(*storage.BatchLocationsCache) []bool { return nil }
func (n *nilMinMaxTarget) GetMinPk() *storage.PrimaryKey                    { return nil }
func (n *nilMinMaxTarget) GetMaxPk() *storage.PrimaryKey                    { return nil }

func TestPKUnaryRangeMatchesTargetBoundaries(t *testing.T) {
	seg := newInt64Target(1, 5, 9, []int64{5, 7, 9})
	assert.True(t, pkUnaryRangeMatchesTarget(seg, planpb.OpType_GreaterEqual, storage.NewInt64PrimaryKey(9)))
	assert.False(t, pkUnaryRangeMatchesTarget(seg, planpb.OpType_GreaterThan, storage.NewInt64PrimaryKey(9)))
	assert.True(t, pkUnaryRangeMatchesTarget(seg, planpb.OpType_LessEqual, storage.NewInt64PrimaryKey(5)))
	assert.False(t, pkUnaryRangeMatchesTarget(seg, planpb.OpType_LessThan, storage.NewInt64PrimaryKey(5)))
}

func TestPKBinaryRangeMatchesTargetBoundaries(t *testing.T) {
	t.Run("exclusive lower filters equal max", func(t *testing.T) {
		seg := newInt64Target(1, 0, 5, []int64{1, 2, 5})
		assert.False(t, pkBinaryRangeMatchesTarget(seg, &pkFilterBinaryRangeExpr{
			lower: pkFilterBound{value: storage.NewInt64PrimaryKey(5), inclusive: false, present: true},
		}))
	})

	t.Run("inclusive lower keeps equal max", func(t *testing.T) {
		seg := newInt64Target(1, 0, 5, []int64{1, 2, 5})
		assert.True(t, pkBinaryRangeMatchesTarget(seg, &pkFilterBinaryRangeExpr{
			lower: pkFilterBound{value: storage.NewInt64PrimaryKey(5), inclusive: true, present: true},
		}))
	})

	t.Run("exclusive upper filters equal min", func(t *testing.T) {
		seg := newInt64Target(1, 5, 9, []int64{5, 7, 9})
		assert.False(t, pkBinaryRangeMatchesTarget(seg, &pkFilterBinaryRangeExpr{
			upper: pkFilterBound{value: storage.NewInt64PrimaryKey(5), inclusive: false, present: true},
		}))
	})

	t.Run("inclusive upper keeps equal min", func(t *testing.T) {
		seg := newInt64Target(1, 5, 9, []int64{5, 7, 9})
		assert.True(t, pkBinaryRangeMatchesTarget(seg, &pkFilterBinaryRangeExpr{
			upper: pkFilterBound{value: storage.NewInt64PrimaryKey(5), inclusive: true, present: true},
		}))
	})
}

func TestCheckPKFilterEmptyTermExpr(t *testing.T) {
	// An IN [] predicate (empty values list) should match no segments.
	targets := []PKFilterTarget{
		newInt64Target(1, 0, 4, []int64{1, 2, 4}),
		newInt64Target(2, 5, 8, []int64{5, 7}),
	}
	emptyTerm := &pkFilterTermExpr{values: []storage.PrimaryKey{}}
	ids, all := CheckPKFilter(emptyTerm, targets)
	assert.False(t, all)
	assert.Empty(t, ids)
}
