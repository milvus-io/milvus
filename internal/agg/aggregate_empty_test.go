package agg

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	emptyAggGroupFieldID int64 = 1
	emptyAggValueFieldID int64 = 2
)

func emptyAggTestSchema(valueType schemapb.DataType) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: emptyAggGroupFieldID, Name: "g", DataType: schemapb.DataType_Int64},
			{FieldID: emptyAggValueFieldID, Name: "x", DataType: valueType, Nullable: true},
		},
	}
}

// makeAggColumn builds a one-column partial of the given type. vals holds
// the dense values (as int64 or float64 for convenience); valid is optional.
func makeAggColumn(t *testing.T, typ schemapb.DataType, vals []float64, valid []bool) *schemapb.FieldData {
	t.Helper()
	scalars := &schemapb.ScalarField{}
	switch typ {
	case schemapb.DataType_Int64:
		data := make([]int64, len(vals))
		for i, v := range vals {
			data[i] = int64(v)
		}
		scalars.Data = &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data}}
	case schemapb.DataType_Double:
		scalars.Data = &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: vals}}
	default:
		t.Fatalf("unsupported test column type %s", typ)
	}
	field := &schemapb.FieldData{Type: typ, Field: &schemapb.FieldData_Scalars{Scalars: scalars}}
	if valid != nil {
		typeutil.SetFieldDataValidData(field, valid)
	}
	return field
}

// readColumn returns the rows of a column; nil entries are NULL.
func readColumn(t *testing.T, field *schemapb.FieldData) []any {
	t.Helper()
	accessor, err := NewFieldAccessor(field.GetType())
	require.NoError(t, err)
	accessor.SetVals(field)
	validity := typeutil.GetFieldDataValidData(field)
	if len(validity) != 0 {
		require.Len(t, validity, accessor.RowCount(), "validity must match the row count")
	}
	rows := make([]any, accessor.RowCount())
	for i := range rows {
		if !accessor.IsNullAt(i) {
			rows[i] = accessor.ValAt(i)
		}
	}
	return rows
}

func typedValue(typ schemapb.DataType, v float64) any {
	if typ == schemapb.DataType_Int64 {
		return int64(v)
	}
	return v
}

// sumResultType mirrors getAggregateResultType for the types used here.
func sumResultType(typ schemapb.DataType) schemapb.DataType {
	if typ == schemapb.DataType_Int64 {
		return schemapb.DataType_Int64
	}
	return schemapb.DataType_Double
}

// A global aggregate over no contributors must be COUNT=0 and NULL for
// MIN/MAX/SUM, never valid zeros. See issue #54744.
func TestEmptyAggResultGlobalIsCountZeroAndNull(t *testing.T) {
	for _, typ := range []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_Double} {
		t.Run(typ.String(), func(t *testing.T) {
			reducer := NewGroupAggReducer(nil, []*planpb.Aggregate{
				{Op: planpb.AggregateOp_min, FieldId: emptyAggValueFieldID},
				{Op: planpb.AggregateOp_max, FieldId: emptyAggValueFieldID},
				{Op: planpb.AggregateOp_sum, FieldId: emptyAggValueFieldID},
				{Op: planpb.AggregateOp_count, FieldId: emptyAggValueFieldID},
			}, -1, emptyAggTestSchema(typ))

			out, err := reducer.Reduce(context.Background(), nil)
			require.NoError(t, err)
			fields := out.GetFieldDatas()
			require.Len(t, fields, 4)
			require.Equal(t, []any{nil}, readColumn(t, fields[0]), "MIN over no rows is NULL")
			require.Equal(t, []any{nil}, readColumn(t, fields[1]), "MAX over no rows is NULL")
			require.Equal(t, []any{nil}, readColumn(t, fields[2]), "SUM over no rows is NULL")
			require.Equal(t, []any{int64(0)}, readColumn(t, fields[3]), "COUNT over no rows is 0")
		})
	}
}

// An empty contribution must be neutral when merged with real partials.
func TestEmptyAggResultIsNeutralInGlobalMerge(t *testing.T) {
	cases := []struct {
		name     string
		op       planpb.AggregateOp
		value    float64
		expected float64
	}{
		{"min", planpb.AggregateOp_min, 150, 150},
		{"max", planpb.AggregateOp_max, -20, -20},
		{"sum", planpb.AggregateOp_sum, 7, 7},
		{"count", planpb.AggregateOp_count, 5, 5},
	}
	for _, typ := range []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_Double} {
		for _, tc := range cases {
			t.Run(typ.String()+"/"+tc.name, func(t *testing.T) {
				reducer := NewGroupAggReducer(nil, []*planpb.Aggregate{
					{Op: tc.op, FieldId: emptyAggValueFieldID},
				}, -1, emptyAggTestSchema(typ))
				resultType := typ
				switch tc.op {
				case planpb.AggregateOp_count:
					resultType = schemapb.DataType_Int64
				case planpb.AggregateOp_sum:
					resultType = sumResultType(typ)
				}

				empty, err := reducer.EmptyAggResult()
				require.NoError(t, err)
				value := NewAggregationResult([]*schemapb.FieldData{
					makeAggColumn(t, resultType, []float64{tc.value}, nil),
				}, 1)
				want := []any{typedValue(resultType, tc.expected)}

				for _, inputs := range [][]*AggregationResult{{empty, value}, {value, empty}, {empty, value, empty}} {
					out, err := reducer.Reduce(context.Background(), inputs)
					require.NoError(t, err)
					require.Equal(t, want, readColumn(t, out.GetFieldDatas()[0]))
				}

				if tc.op == planpb.AggregateOp_count {
					return
				}
				// Empty plus a NULL-only partial (e.g. a segment whose rows are all
				// NULL) must stay NULL rather than turn into a valid zero.
				nullOnly := NewAggregationResult([]*schemapb.FieldData{
					makeAggColumn(t, resultType, []float64{0}, []bool{false}),
				}, 1)
				for _, inputs := range [][]*AggregationResult{{empty, nullOnly}, {nullOnly, empty}, {empty, empty}} {
					out, err := reducer.Reduce(context.Background(), inputs)
					require.NoError(t, err)
					require.Equal(t, []any{nil}, readColumn(t, out.GetFieldDatas()[0]))
				}
			})
		}
	}
}

// GROUP BY over no contributors has zero rows in every column and merges
// with other partials. See issue #54744.
func TestEmptyAggResultGroupByHasZeroRows(t *testing.T) {
	for _, typ := range []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_Double} {
		t.Run(typ.String(), func(t *testing.T) {
			reducer := func() *GroupAggReducer {
				return NewGroupAggReducer([]int64{emptyAggGroupFieldID}, []*planpb.Aggregate{
					{Op: planpb.AggregateOp_count, FieldId: emptyAggValueFieldID},
					{Op: planpb.AggregateOp_min, FieldId: emptyAggValueFieldID},
				}, -1, emptyAggTestSchema(typ))
			}

			e1, err := reducer().EmptyAggResult()
			require.NoError(t, err)
			e2, err := reducer().EmptyAggResult()
			require.NoError(t, err)
			out, err := reducer().Reduce(context.Background(), []*AggregationResult{e1, e2})
			require.NoError(t, err)
			require.Len(t, out.GetFieldDatas(), 3)
			for i, field := range out.GetFieldDatas() {
				require.Empty(t, readColumn(t, field), "column %d of merged empty GROUP BY must have zero rows", i)
			}

			nonEmpty := NewAggregationResult([]*schemapb.FieldData{
				makeAggColumn(t, schemapb.DataType_Int64, []float64{10}, nil),
				makeAggColumn(t, schemapb.DataType_Int64, []float64{3}, nil),
				makeAggColumn(t, typ, []float64{-4}, nil),
			}, 3)
			for _, inputs := range [][]*AggregationResult{{e1, nonEmpty}, {nonEmpty, e2}} {
				out, err := reducer().Reduce(context.Background(), inputs)
				require.NoError(t, err)
				fields := out.GetFieldDatas()
				require.Len(t, fields, 3)
				require.Equal(t, []any{int64(10)}, readColumn(t, fields[0]))
				require.Equal(t, []any{int64(3)}, readColumn(t, fields[1]))
				require.Equal(t, []any{typedValue(typ, -4)}, readColumn(t, fields[2]))
			}

			empty, err := reducer().Reduce(context.Background(), nil)
			require.NoError(t, err)
			require.Len(t, empty.GetFieldDatas(), 3)
			for i, field := range empty.GetFieldDatas() {
				require.Empty(t, readColumn(t, field), "column %d of an empty GROUP BY must have zero rows", i)
			}
		})
	}
}

// Every MIN/MAX result type supported on 3.0 must materialize a single NULL
// row for a global empty result, including VarChar and Timestamptz.
func TestEmptyAggResultGlobalMinLayouts(t *testing.T) {
	for _, typ := range []schemapb.DataType{
		schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32,
		schemapb.DataType_Int64, schemapb.DataType_Float, schemapb.DataType_Double,
		schemapb.DataType_VarChar, schemapb.DataType_String, schemapb.DataType_Timestamptz,
	} {
		t.Run(typ.String(), func(t *testing.T) {
			reducer := NewGroupAggReducer(nil, []*planpb.Aggregate{
				{Op: planpb.AggregateOp_min, FieldId: emptyAggValueFieldID},
			}, -1, emptyAggTestSchema(typ))
			empty, err := reducer.EmptyAggResult()
			require.NoError(t, err)
			require.Len(t, empty.GetFieldDatas(), 1)
			require.Equal(t, typ, empty.GetFieldDatas()[0].GetType())
			require.Equal(t, []any{nil}, readColumn(t, empty.GetFieldDatas()[0]))

			out, err := reducer.Reduce(context.Background(), []*AggregationResult{empty, empty})
			require.NoError(t, err)
			require.Equal(t, []any{nil}, readColumn(t, out.GetFieldDatas()[0]))
		})
	}
}
