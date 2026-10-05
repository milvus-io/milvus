package planparserv2

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// These words have a dedicated token in the expression grammar, but may also
// identify fields. Keep both lexer spellings in the matrix: accepting only the
// mixed-case Identifier spelling would leave the original regression intact.
var contextualKeywordNames = []string{
	"text_match_fuzzy", "element_filter",
	"match_all", "match_any", "match_least", "match_most", "match_exact",
	"st_equals", "st_touches", "st_overlaps", "st_crosses", "st_contains",
	"st_intersects", "st_within", "st_dwithin", "st_isvalid",
	"iso", "interval", "minimum_should_match", "threshold",
}

const contextualKeywordFieldID int64 = 2000

func contextualKeywordSpellings(name string) []string {
	return []string{name, strings.ToUpper(name), strings.ToUpper(name[:1]) + name[1:]}
}

func contextualKeywordSchema(t *testing.T, name string, dataType schemapb.DataType, dynamic bool) *typeutil.SchemaHelper {
	t.Helper()
	schema := newTestSchema(dynamic)
	field := &schemapb.FieldSchema{
		FieldID: contextualKeywordFieldID, Name: name, DataType: dataType, Nullable: true,
	}
	if dataType == schemapb.DataType_Array {
		field.ElementType = schemapb.DataType_Int64
	}
	schema.Fields = append(schema.Fields, field)
	enableMatch(schema)
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	return helper
}

func requireContextualKeywordColumn(t *testing.T, column *planpb.ColumnInfo, name string) {
	t.Helper()
	require.NotNil(t, column, name)
	require.Equal(t, contextualKeywordFieldID, column.GetFieldId(), name)
	require.Empty(t, column.GetNestedPath(), name)
}

func TestContextualKeywordsScalarFields(t *testing.T) {
	for _, dynamic := range []bool{false, true} {
		for _, word := range contextualKeywordNames {
			for _, name := range contextualKeywordSpellings(word) {
				t.Run(fmt.Sprintf("dynamic_%t/%s", dynamic, name), func(t *testing.T) {
					helper := contextualKeywordSchema(t, name, schemapb.DataType_Int64, dynamic)
					parse := func(format string) *planpb.Expr {
						t.Helper()
						expr, err := ParseExpr(helper, fmt.Sprintf(format, name), nil)
						require.NoError(t, err)
						require.NotNil(t, expr)
						return expr
					}

					for _, format := range []string{"%s == 400", "400 == %s"} {
						unary := parse(format).GetUnaryRangeExpr()
						require.NotNil(t, unary)
						requireContextualKeywordColumn(t, unary.GetColumnInfo(), name)
						require.Equal(t, planpb.OpType_Equal, unary.GetOp())
						require.Equal(t, int64(400), unary.GetValue().GetInt64Val())
					}

					term := parse("%s in [1, 400]").GetTermExpr()
					require.NotNil(t, term)
					requireContextualKeywordColumn(t, term.GetColumnInfo(), name)
					require.Len(t, term.GetValues(), 2)
					require.Equal(t, int64(400), term.GetValues()[1].GetInt64Val())

					arith := parse("%s + 1 > 400").GetBinaryArithOpEvalRangeExpr()
					require.NotNil(t, arith)
					requireContextualKeywordColumn(t, arith.GetColumnInfo(), name)
					require.Equal(t, planpb.ArithOpType_Add, arith.GetArithOp())
					require.Equal(t, int64(1), arith.GetRightOperand().GetInt64Val())
					require.Equal(t, planpb.OpType_GreaterThan, arith.GetOp())
					require.Equal(t, int64(400), arith.GetValue().GetInt64Val())

					for _, format := range []string{"5 < %s <= 400", "400 >= %s > 5"} {
						rangeExpr := parse(format).GetBinaryRangeExpr()
						require.NotNil(t, rangeExpr)
						requireContextualKeywordColumn(t, rangeExpr.GetColumnInfo(), name)
						require.Equal(t, int64(5), rangeExpr.GetLowerValue().GetInt64Val())
						require.Equal(t, int64(400), rangeExpr.GetUpperValue().GetInt64Val())
						require.False(t, rangeExpr.GetLowerInclusive())
						require.True(t, rangeExpr.GetUpperInclusive())
					}

					for _, tc := range []struct {
						format string
						op     planpb.NullExpr_NullOp
					}{
						{"%s is null", planpb.NullExpr_IsNull},
						{"%s is not null", planpb.NullExpr_IsNotNull},
					} {
						nullExpr := parse(tc.format).GetNullExpr()
						require.NotNil(t, nullExpr)
						requireContextualKeywordColumn(t, nullExpr.GetColumnInfo(), name)
						require.Equal(t, tc.op, nullExpr.GetOp())
						require.True(t, nullExpr.GetColumnInfo().GetNullable())
					}

					// The same token must be usable in a template name, independently
					// of its use as the column on the left.
					expr, err := ParseExpr(helper, fmt.Sprintf("%s == {%s}", name, name), map[string]*schemapb.TemplateValue{
						name: generateTemplateValue(schemapb.DataType_Int64, int64(400)),
					})
					require.NoError(t, err)
					unary := expr.GetUnaryRangeExpr()
					require.NotNil(t, unary)
					requireContextualKeywordColumn(t, unary.GetColumnInfo(), name)
					require.Equal(t, int64(400), unary.GetValue().GetInt64Val())

					call := parse("custom_predicate(%s, 1)").GetCallExpr()
					require.NotNil(t, call)
					require.Equal(t, "custom_predicate", call.GetFunctionName())
					require.Len(t, call.GetFunctionParameters(), 2)
					requireContextualKeywordColumn(t, call.GetFunctionParameters()[0].GetColumnExpr().GetInfo(), name)
				})
			}
		}
	}
}

func TestContextualKeywordsCaseSensitiveLookup(t *testing.T) {
	for _, word := range contextualKeywordNames {
		t.Run(word, func(t *testing.T) {
			upper := strings.ToUpper(word)
			helper := contextualKeywordSchema(t, word, schemapb.DataType_Int64, true)
			expr, err := ParseExpr(helper, word+" == 400", nil)
			require.NoError(t, err)
			requireContextualKeywordColumn(t, expr.GetUnaryRangeExpr().GetColumnInfo(), word)

			// A differently-cased name stays a different dynamic JSON key. It
			// must not resolve to the lower-case declaration by normalization.
			expr, err = ParseExpr(helper, upper+" == 400", nil)
			require.NoError(t, err)
			column := expr.GetUnaryRangeExpr().GetColumnInfo()
			require.Equal(t, int64(dynamicFieldID), column.GetFieldId())
			require.Equal(t, []string{upper}, column.GetNestedPath())

			helper = contextualKeywordSchema(t, word, schemapb.DataType_Int64, false)
			_, err = ParseExpr(helper, upper+" == 400", nil)
			require.Error(t, err)

			schema := newTestSchema(true)
			schema.Fields = append(schema.Fields,
				&schemapb.FieldSchema{FieldID: contextualKeywordFieldID, Name: word, DataType: schemapb.DataType_Int64},
				&schemapb.FieldSchema{FieldID: contextualKeywordFieldID + 1, Name: upper, DataType: schemapb.DataType_Int64},
			)
			helper, err = typeutil.CreateSchemaHelper(schema)
			require.NoError(t, err)
			expr, err = ParseExpr(helper, upper+" == 400", nil)
			require.NoError(t, err)
			require.Equal(t, contextualKeywordFieldID+1, expr.GetUnaryRangeExpr().GetColumnInfo().GetFieldId())
			require.Empty(t, expr.GetUnaryRangeExpr().GetColumnInfo().GetNestedPath())
		})
	}
}

func TestContextualKeywordsNestedFields(t *testing.T) {
	for _, word := range contextualKeywordNames {
		for _, name := range contextualKeywordSpellings(word) {
			t.Run(name, func(t *testing.T) {
				helper := contextualKeywordSchema(t, name, schemapb.DataType_JSON, true)
				expr, err := ParseExpr(helper, fmt.Sprintf(`%s["value"] == 400`, name), nil)
				require.NoError(t, err)
				column := expr.GetUnaryRangeExpr().GetColumnInfo()
				require.Equal(t, contextualKeywordFieldID, column.GetFieldId())
				require.Equal(t, []string{"value"}, column.GetNestedPath())

				helper = contextualKeywordSchema(t, name, schemapb.DataType_Array, true)
				expr, err = ParseExpr(helper, fmt.Sprintf("%s[0] == 400", name), nil)
				require.NoError(t, err)
				column = expr.GetUnaryRangeExpr().GetColumnInfo()
				require.Equal(t, contextualKeywordFieldID, column.GetFieldId())
				require.Equal(t, []string{"0"}, column.GetNestedPath())

				// Parent and sub-field names both use contextual tokens, while
				// element predicates retain their dedicated scope and plan node.
				schema := newTestSchema(true)
				schema.StructArrayFields = append(schema.StructArrayFields, &schemapb.StructArrayFieldSchema{
					FieldID: contextualKeywordFieldID, Name: name, Nullable: true,
					Fields: []*schemapb.FieldSchema{{
						FieldID: contextualKeywordFieldID + 1, Name: fmt.Sprintf("%s[%s]", name, name),
						DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64,
					}},
				})
				helper, err = typeutil.CreateSchemaHelper(schema)
				require.NoError(t, err)
				expr, err = ParseExpr(helper, fmt.Sprintf("element_filter(%s, $[%s] > 5)", name, name), nil)
				require.NoError(t, err)
				element := expr.GetElementFilterExpr()
				require.NotNil(t, element)
				require.Equal(t, name, element.GetStructName())
				require.Equal(t, contextualKeywordFieldID+1, element.GetElementExpr().GetUnaryRangeExpr().GetColumnInfo().GetFieldId())
				if word == "element_filter" {
					expr, err = ParseExpr(helper, fmt.Sprintf("ELEMENT_FILTER(%s, $[%s] > 5)", name, name), nil)
					require.NoError(t, err)
					require.NotNil(t, expr.GetElementFilterExpr())
					require.Equal(t, name, expr.GetElementFilterExpr().GetStructName())
				}

				expr, err = ParseExpr(helper, fmt.Sprintf("%s is null", name), nil)
				require.NoError(t, err)
				require.NotNil(t, expr.GetNullExpr())
				require.Equal(t, contextualKeywordFieldID+1, expr.GetNullExpr().GetColumnInfo().GetFieldId())

				ret := handleExpr(helper, fmt.Sprintf("array_length(%s[%s])", name, name))
				require.NoError(t, getError(ret))
				length := getExpr(ret).expr.GetBinaryArithExpr()
				require.NotNil(t, length)
				require.Equal(t, planpb.ArithOpType_ArrayLength, length.GetOp())
				require.Equal(t, contextualKeywordFieldID+1, length.GetLeft().GetColumnExpr().GetInfo().GetFieldId())
			})
		}
	}
}

func TestContextualKeywordsTextMatchPlans(t *testing.T) {
	for _, name := range []string{"text_match_fuzzy", "TEXT_MATCH_FUZZY"} {
		t.Run(name, func(t *testing.T) {
			helper := contextualKeywordSchema(t, name, schemapb.DataType_VarChar, true)
			expr, err := ParseExpr(helper, fmt.Sprintf(`%s(%s, "hello", max_edit_distance=1)`, name, name), nil)
			require.NoError(t, err)
			require.Nil(t, expr.GetCallExpr())
			unary := expr.GetUnaryRangeExpr()
			require.NotNil(t, unary)
			requireContextualKeywordColumn(t, unary.GetColumnInfo(), name)
			require.Equal(t, planpb.OpType_TextMatchFuzzy, unary.GetOp())
			require.Equal(t, "hello", unary.GetValue().GetStringVal())
			require.Len(t, unary.GetExtraValues(), 1)
			require.Equal(t, int64(1), unary.GetExtraValues()[0].GetInt64Val())

			expr, err = ParseExpr(helper, fmt.Sprintf(`starts_with(%s, "hello")`, name), nil)
			require.NoError(t, err)
			require.NotNil(t, expr.GetCallExpr())
			require.Equal(t, "starts_with", expr.GetCallExpr().GetFunctionName())
			requireContextualKeywordColumn(t, expr.GetCallExpr().GetFunctionParameters()[0].GetColumnExpr().GetInfo(), name)
		})
	}

	for _, name := range []string{"minimum_should_match", "MINIMUM_SHOULD_MATCH"} {
		helper := contextualKeywordSchema(t, name, schemapb.DataType_VarChar, true)
		expr, err := ParseExpr(helper, fmt.Sprintf(`text_match(%s, "hello", %s=2)`, name, name), nil)
		require.NoError(t, err)
		unary := expr.GetUnaryRangeExpr()
		require.NotNil(t, unary)
		requireContextualKeywordColumn(t, unary.GetColumnInfo(), name)
		require.Equal(t, planpb.OpType_TextMatch, unary.GetOp())
		require.Len(t, unary.GetExtraValues(), 1)
		require.Equal(t, int64(2), unary.GetExtraValues()[0].GetInt64Val())
	}
}

func TestContextualKeywordsSpatialPlans(t *testing.T) {
	for _, tc := range []struct {
		name string
		op   planpb.GISFunctionFilterExpr_GISOp
	}{
		{"st_equals", planpb.GISFunctionFilterExpr_Equals},
		{"st_touches", planpb.GISFunctionFilterExpr_Touches},
		{"st_overlaps", planpb.GISFunctionFilterExpr_Overlaps},
		{"st_crosses", planpb.GISFunctionFilterExpr_Crosses},
		{"st_contains", planpb.GISFunctionFilterExpr_Contains},
		{"st_intersects", planpb.GISFunctionFilterExpr_Intersects},
		{"st_within", planpb.GISFunctionFilterExpr_Within},
		{"st_dwithin", planpb.GISFunctionFilterExpr_DWithin},
		{"st_isvalid", planpb.GISFunctionFilterExpr_STIsValid},
	} {
		for _, name := range []string{tc.name, strings.ToUpper(tc.name)} {
			t.Run(name, func(t *testing.T) {
				helper := contextualKeywordSchema(t, name, schemapb.DataType_Geometry, true)
				query := fmt.Sprintf(`%s(%s, "POINT(0 0)")`, name, name)
				if tc.op == planpb.GISFunctionFilterExpr_DWithin {
					query = fmt.Sprintf(`%s(%s, "POINT(0 0)", 5)`, name, name)
				} else if tc.op == planpb.GISFunctionFilterExpr_STIsValid {
					query = fmt.Sprintf("%s(%s)", name, name)
				}
				expr, err := ParseExpr(helper, query, nil)
				require.NoError(t, err)
				require.Nil(t, expr.GetCallExpr())
				spatial := expr.GetGisfunctionFilterExpr()
				require.NotNil(t, spatial)
				requireContextualKeywordColumn(t, spatial.GetColumnInfo(), name)
				require.Equal(t, tc.op, spatial.GetOp())
				if tc.op != planpb.GISFunctionFilterExpr_STIsValid {
					require.Equal(t, "POINT(0 0)", spatial.GetWktString())
				}
				if tc.op == planpb.GISFunctionFilterExpr_DWithin {
					require.Equal(t, float64(5), spatial.GetDistance())
				}
			})
		}
	}
}

func TestContextualKeywordsMatchPlans(t *testing.T) {
	for _, tc := range []struct {
		name      string
		matchType planpb.MatchType
		count     int64
	}{
		{"match_all", planpb.MatchType_MatchAll, 0},
		{"match_any", planpb.MatchType_MatchAny, 0},
		{"match_least", planpb.MatchType_MatchLeast, 2},
		{"match_most", planpb.MatchType_MatchMost, 2},
		{"match_exact", planpb.MatchType_MatchExact, 2},
	} {
		for _, name := range []string{tc.name, strings.ToUpper(tc.name)} {
			t.Run(name, func(t *testing.T) {
				schema := newTestSchema(true)
				schema.StructArrayFields = append(schema.StructArrayFields, &schemapb.StructArrayFieldSchema{
					FieldID: contextualKeywordFieldID, Name: name,
					Fields: []*schemapb.FieldSchema{{
						FieldID: contextualKeywordFieldID + 1, Name: name + "[threshold]",
						DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64,
					}},
				})
				helper, err := typeutil.CreateSchemaHelper(schema)
				require.NoError(t, err)
				query := fmt.Sprintf("%s(%s, $[threshold] > 5)", name, name)
				if tc.count != 0 {
					option := "threshold"
					if name == strings.ToUpper(name) {
						option = "THRESHOLD"
					}
					query = fmt.Sprintf("%s(%s, $[threshold] > 5, %s=2)", name, name, option)
				}
				expr, err := ParseExpr(helper, query, nil)
				require.NoError(t, err)
				require.Nil(t, expr.GetCallExpr())
				match := expr.GetMatchExpr()
				require.NotNil(t, match)
				require.Equal(t, name, match.GetStructName())
				require.Equal(t, tc.matchType, match.GetMatchType())
				require.Equal(t, tc.count, match.GetCount())
				require.Equal(t, contextualKeywordFieldID+1, match.GetPredicate().GetUnaryRangeExpr().GetColumnInfo().GetFieldId())
			})
		}
	}
}

func TestContextualKeywordsTimePlans(t *testing.T) {
	const date = "'2025-01-01T00:00:00Z'"
	wantMicros := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	for _, word := range []string{"iso", "interval", "minimum_should_match", "threshold"} {
		for _, name := range []string{word, strings.ToUpper(word)} {
			t.Run(name, func(t *testing.T) {
				helper := contextualKeywordSchema(t, name, schemapb.DataType_Timestamptz, true)
				for _, query := range []string{name + " > iso " + date, "ISO " + date + " < " + name} {
					expr, err := ParseExpr(helper, query, nil)
					require.NoError(t, err, query)
					unary := expr.GetUnaryRangeExpr()
					require.NotNil(t, unary, query)
					requireContextualKeywordColumn(t, unary.GetColumnInfo(), name)
					require.Equal(t, planpb.OpType_GreaterThan, unary.GetOp())
					require.Equal(t, wantMicros, unary.GetValue().GetInt64Val())
				}
				for _, tc := range []struct {
					query string
					arith planpb.ArithOpType
					op    planpb.OpType
					span  *planpb.Interval
				}{
					{name + " + interval 'P1D' > ISO " + date, planpb.ArithOpType_Add, planpb.OpType_GreaterThan, &planpb.Interval{Days: 1}},
					{"iso " + date + " >= " + name + " - INTERVAL 'PT2H'", planpb.ArithOpType_Sub, planpb.OpType_LessEqual, &planpb.Interval{Hours: 2}},
				} {
					expr, err := ParseExpr(helper, tc.query, nil)
					require.NoError(t, err, tc.query)
					arith := expr.GetTimestamptzArithCompareExpr()
					require.NotNil(t, arith)
					requireContextualKeywordColumn(t, arith.GetTimestamptzColumn(), name)
					require.Equal(t, tc.arith, arith.GetArithOp())
					require.Equal(t, tc.op, arith.GetCompareOp())
					require.Equal(t, wantMicros, arith.GetCompareValue().GetInt64Val())
					require.Equal(t, tc.span.GetDays(), arith.GetInterval().GetDays())
					require.Equal(t, tc.span.GetHours(), arith.GetInterval().GetHours())
				}
			})
		}
	}
}

func TestContextualKeywordsMalformedCalls(t *testing.T) {
	schema := newTestSchema(true)
	enableMatch(schema)
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	for _, word := range contextualKeywordNames {
		for _, name := range []string{word, strings.ToUpper(word)} {
			// A keyword token followed by parentheses must never fall through
			// to the generic Call visitor and evade its dedicated validation.
			_, err := ParseExpr(helper, name+"()", nil)
			require.Error(t, err, name)
		}
	}
	for _, query := range []string{
		`text_match_fuzzy(VarCharField, "hello", typo=1)`,
		`text_match_fuzzy(VarCharField, "hello", max_edit_distance=3)`,
		`text_match(VarCharField, "hello", threshold=1)`,
		`match_least(struct_array, $[sub_int] > 0, count=1)`,
		`match_least(struct_array, $[sub_int] > 0, threshold=0)`,
		`match_all(struct_array, $[sub_int] > 0, threshold=1)`,
		`element_filter(struct_array, $[missing] > 0)`,
		`$[threshold] > 0`,
		`st_isvalid(GeometryField, "POINT(0 0)")`,
		`st_dwithin(GeometryField, "POINT(0 0)")`,
		`st_equals(Int64Field, "POINT(0 0)")`,
		`TimestamptzField > timezone '2025-01-01T00:00:00Z'`,
		`timezone '2025-01-01T00:00:00Z' < TimestamptzField`,
		`TimestamptzField + duration 'P1D' > ISO '2025-01-01T00:00:00Z'`,
		`TimestamptzField > ISO 'invalid-date'`,
		`ISO 'invalid-date' < TimestamptzField`,
		`TimestamptzField + INTERVAL '1D' > ISO '2025-01-01T00:00:00Z'`,
		`ISO '2025-01-01T00:00:00Z' < TimestamptzField + INTERVAL '1D'`,
	} {
		_, err := ParseExpr(helper, query, nil)
		require.Error(t, err, query)
	}
}

func TestContextualKeywordsTwoStageMatchesLL(t *testing.T) {
	corpus := []string{
		`text_match_fuzzy(text_match_fuzzy, "hello", max_edit_distance=1)`,
		`text_match(minimum_should_match, "hello", minimum_should_match=2)`,
		`st_equals(st_equals, "POINT(0 0)")`,
		`ST_DWITHIN(ST_DWITHIN, "POINT(0 0)", 5)`,
		`match_least(match_least, $[threshold] > 0, threshold=2)`,
		`element_filter(element_filter, $[iso] > 0)`,
		`iso > iso '2025-01-01T00:00:00Z'`,
		`ISO '2025-01-01T00:00:00Z' < ISO`,
		`interval + interval 'P1D' >= iso '2025-01-01T00:00:00Z'`,
		`ISO '2025-01-01T00:00:00Z' <= INTERVAL - INTERVAL 'PT2H'`,
		`text_match_fuzzy(VarCharField)`,
		`match_least(struct_array, $[threshold] > 0, count=2)`,
		`iso > timezone '2025-01-01T00:00:00Z'`,
	}
	for _, word := range contextualKeywordNames {
		for _, name := range []string{word, strings.ToUpper(word)} {
			corpus = append(corpus,
				name+" == 400", "5 < "+name+" <= 400", "400 >= "+name+" > 5",
				name+" is null", name+" == {"+name+"}", "custom_predicate("+name+", 1)",
			)
		}
	}
	for _, query := range corpus {
		llTree, llErr := parseTree(query, false)
		tsTree, tsErr := parseTree(query, true)
		require.Equal(t, llErr == nil, tsErr == nil, "%s: LL=%v, two-stage=%v", query, llErr, tsErr)
		if llErr == nil && tsErr == nil {
			require.Equal(t, llTree, tsTree, query)
		}
	}
}
