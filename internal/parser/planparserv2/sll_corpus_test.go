package planparserv2

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// corpusExprs is a broad set of syntactically-valid expressions, simple to
// complex, used to (a) prove the SLL-first two-stage parse yields the exact same
// tree as a pure LL parse for every shape, and (b) benchmark parse throughput.
// Grown in batches.
var corpusExprs = []struct {
	name string
	expr string
}{
	// ---- batch 1: comparisons / arithmetic / bitwise / shift / unary / range ----
	{"cmp_eq", "Int64Field == 100"},
	{"cmp_ne", "Int64Field != 100"},
	{"cmp_lt", "Int64Field < 100"},
	{"cmp_le", "Int64Field <= 100"},
	{"cmp_gt", "Int64Field > 100"},
	{"cmp_ge", "Int64Field >= 100"},
	{"cmp_float", "FloatField >= 1.5"},
	{"cmp_neg", "Int64Field > -5"},
	{"arith_add", "Int64Field + 5 == 100"},
	{"arith_sub", "Int64Field - 5 == 100"},
	{"arith_mul", "Int64Field * 2 < 200"},
	{"arith_div", "Int64Field / 2 < 200"},
	{"arith_mod", "Int64Field % 10 == 0"},
	{"arith_pow", "Int64Field ** 2 < 1000"},
	{"arith_chain", "Int64Field * 2 + 3 - 1 == 10"},
	{"arith_paren", "(Int64Field + 5) * 2 == 30"},
	{"bit_and", "Int64Field & 3 == 1"},
	{"bit_or", "Int64Field | 3 == 7"},
	{"bit_xor", "Int64Field ^ 3 == 5"},
	{"bit_not", "~Int64Field == -1"},
	{"shift_l", "Int64Field << 2 == 16"},
	{"shift_r", "Int64Field >> 2 == 1"},
	{"unary_minus", "-Int64Field < 0"},
	{"unary_plus", "+Int64Field > 0"},
	{"unary_not", "!(Int64Field > 0)"},
	{"range_lt_lt", "10 < Int64Field < 100"},
	{"range_le_le", "10 <= Int64Field <= 100"},
	{"range_rev_gt", "100 > Int64Field > 10"},
	{"range_rev_ge", "100 >= Int64Field >= 10"},
	{"literal_led_eq", "100 == Int64Field"},
	{"literal_led_lt", "1.5 <= FloatField"},

	// ---- batch 2: in / string / regex / array / json / struct / null / exists ----
	{"in_small", "Int64Field in [1, 2, 3]"},
	{"in_large", "Int64Field in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]"},
	{"not_in", "Int64Field not in [1, 2, 3]"},
	{"in_str", `StringField in ["a", "b", "c"]`},
	{"in_float", "FloatField in [1.0, 2.5, 3.7]"},
	{"empty_array", "Int64Field in []"},
	{"str_eq", `StringField == "hello"`},
	{"str_like_prefix", `StringField like "hello%"`},
	{"str_like_infix", `StringField like "%mid%"`},
	{"str_regex", `StringField =~ "ab.*"`},
	{"str_regex_not", `StringField !~ "ab.*"`},
	{"arr_contains", "array_contains(ArrayField, 1)"},
	{"arr_contains_all", "array_contains_all(ArrayField, [1, 2, 3])"},
	{"arr_contains_any", "array_contains_any(ArrayField, [1, 2, 3])"},
	{"arr_length", "array_length(ArrayField) == 10"},
	{"arr_length_cmp", "array_length(ArrayField) > 0 && array_length(ArrayField) < 5"},
	{"json_eq", `JSONField["key"] == 100`},
	{"json_nested", `JSONField["a"]["b"] == "value"`},
	{"json_idx", `JSONField[0] == 1`},
	{"json_contains", `json_contains(JSONField["arr"], 1)`},
	{"json_contains_all", `json_contains_all(JSONField["arr"], [1, 2])`},
	{"json_contains_any", `json_contains_any(JSONField["arr"], [1, 2])`},
	{"struct_field", "struct_array[sub_int] == 1"},
	{"is_null", "Int64Field is null"},
	{"is_not_null", "Int64Field is not null"},
	{"is_null_upper", "Int64Field IS NULL"},
	{"json_is_null", `JSONField["k"] is null`},
	{"exists", `exists JSONField["key"]`},
	{"exists_field", "exists Int64Field"},

	// ---- batch 3: text/phrase/match, sample, spatial, timestamptz, template, call, deep nesting ----
	{"text_match", `text_match(VarCharField, "query")`},
	{"text_match_msm", `text_match(VarCharField, "query", minimum_should_match=2)`},
	{"phrase_match", `phrase_match(VarCharField, "a b c")`},
	{"phrase_match_slop", `phrase_match(VarCharField, "a b c", 2)`},
	{"match_all", `match_all(VarCharField, "x")`},
	{"match_any", `match_any(VarCharField, "x")`},
	{"match_least", `match_least(VarCharField, "x", threshold=2)`},
	{"match_most", `match_most(VarCharField, "x", threshold=3)`},
	{"match_exact", `match_exact(VarCharField, "x", threshold=1)`},
	{"random_sample", "random_sample(0.01)"},
	{"element_filter", "element_filter(ArrayField, x > 1)"},
	{"st_within", `st_within(GeoField, "POLYGON((0 0,1 0,1 1,0 1,0 0))")`},
	{"st_contains", `st_contains(GeoField, "POINT(1 1)")`},
	{"st_intersects", `st_intersects(GeoField, "POINT(1 1)")`},
	{"st_dwithin", `st_dwithin(GeoField, "POINT(1 1)", 10)`},
	{"st_isvalid", "st_isvalid(GeoField)"},
	{"ts_fwd", `TsField > iso "2020-01-01T00:00:00Z"`},
	{"ts_fwd_interval", `TsField + interval "1d" > iso "2020-01-01T00:00:00Z"`},
	{"ts_rev", `iso "2020-01-01T00:00:00Z" < TsField`},
	{"ts_rev_interval", `iso "2020-01-01T00:00:00Z" <= TsField - interval "1h"`},
	{"template_var", "Int64Field == {age}"},
	{"template_in", "Int64Field in {id_list}"},
	{"call_fn", "my_func(Int64Field, 1, 2)"},
	{"call_noargs", "now()"},

	// ---- batch 3: deeply nested / mixed-precedence complex ----
	{"deep_logical", "(Int64Field > 10 && Int64Field < 100) || (FloatField > 1.0 && FloatField < 10.0)"},
	{"deep_mixed", `Int64Field > 10 && StringField like "test%" && array_length(ArrayField) > 0`},
	{"deep_json_logic", `JSONField["status"] == "active" && Int64Field > 0 && Int64Field is not null`},
	{"deep_arith_logic", "Int64Field * 2 + 1 > 10 || (Int64Field - 3) % 2 == 0 && FloatField <= 9.9"},
	{"deep_nested_in", `(Int64Field in [1,2,3] || FloatField > 1.5) && StringField != "exclude"`},
	{"deep_bit_logic", "(Int64Field & 1) == 1 && (Int64Field >> 2) < 8 || ~Int64Field == -1"},
	{"deep_paren_chain", "((((Int64Field + 1) * 2 - 3) / 4) % 5) == 0"},
	{"deep_many_or", "Int64Field == 1 || Int64Field == 2 || Int64Field == 3 || Int64Field == 4 || Int64Field == 5"},
	{"deep_text_logic", `text_match(VarCharField, "a") && (Int64Field > 0 || exists JSONField["k"])`},

	// ---- batch 4: literal forms / escapes / pathological nesting ----
	{"int_hex", "Int64Field == 0xFF"},
	{"int_oct", "Int64Field == 0o17"},
	{"int_bin", "Int64Field == 0b1010"},
	{"float_exp", "FloatField == 1.5e3"},
	{"float_negexp", "FloatField == 1.5e-3"},
	{"float_lead_dot", "FloatField == .5"},
	{"bool_true", "BoolField == true"},
	{"bool_false", "BoolField == false"},
	{"str_escape", `StringField == "a\"b\tc"`},
	{"str_single", `StringField == 'single quoted'`},
	{"str_unicode", `StringField == "éè"`},
	{"deep_parens_10", "((((((((((Int64Field)))))))))) == 1"},
	{"in_long_50", "Int64Field in [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50]"},
	{"nested_call", "outer(inner(Int64Field, 1), mid(2, 3))"},
	{"multi_template", "Int64Field > {lo} && Int64Field < {hi}"},
	{"json_deep5", `JSONField["a"]["b"]["c"]["d"]["e"] == 1`},
	{"mixed_everything", `(Int64Field + 1) * 2 >= {t} && StringField like "x%" && array_contains(ArrayField, 3) || JSONField["k"] is not null`},
}

// invalidExprs are syntactically malformed; both a pure LL parse and the
// two-stage parse must reject them identically (exercises the SLL->LL fallback
// and the error-reporting path).
var invalidExprs = []string{
	"Int64Field ==",
	"Int64Field >> ",
	"(Int64Field > 1",
	"Int64Field > 1)",
	"Int64Field in [1, 2,",
	"&& Int64Field > 1",
	"Int64Field 100",
	"text_match(VarCharField)",
	"text_match(VarCharField, \"q\", minimum_should_match={min})",
	"st_dwithin(GeoField, \"POINT(1 1)\")",
	"Int64Field ** ** 2",
	"random_sample()",
	"100 == == Int64Field",
}

func TestTwoStageMatchesLL_Invalid(t *testing.T) {
	for _, e := range invalidExprs {
		_, llErr := benchLexParseLL(e)
		_, tsErr := benchLexParseTwoStage(e)
		require.Error(t, llErr, "expected LL to reject %q", e)
		require.Equal(t, llErr == nil, tsErr == nil, "accept/reject differs for invalid %q (LL=%v two-stage=%v)", e, llErr, tsErr)
	}
}

func TestTwoStageMatchesLL_Corpus(t *testing.T) {
	for _, tc := range corpusExprs {
		llAst, llErr := benchLexParseLL(tc.expr)
		tsAst, tsErr := benchLexParseTwoStage(tc.expr)
		require.Equal(t, llErr == nil, tsErr == nil, "%s: accept/reject differs for %q (LL=%v two-stage=%v)", tc.name, tc.expr, llErr, tsErr)
		if llErr == nil && tsErr == nil {
			require.Equal(t, llAst.GetText(), tsAst.GetText(), "%s: parse tree differs for %q", tc.name, tc.expr)
		}
	}
}

func corpusList() []string {
	out := make([]string, len(corpusExprs))
	for i, tc := range corpusExprs {
		out[i] = tc.expr
	}
	return out
}

func BenchmarkCorpus_LL(b *testing.B)       { benchParseSet(b, corpusList(), benchLexParseLL) }
func BenchmarkCorpus_TwoStage(b *testing.B) { benchParseSet(b, corpusList(), benchLexParseTwoStage) }
