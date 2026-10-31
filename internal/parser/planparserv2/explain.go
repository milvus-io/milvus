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

package planparserv2

// Static explanation of a constructed search or query plan: ExplainPlan reads
// the planpb.PlanNode it is given and never executes or mutates it, and
// anything it does not recognize becomes a counted "unknown" node. See
// docs/design-docs/design_docs/20261015-static-plan-explain.md.

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ExplainVersion changes only when an existing key of the explanation is
// renamed, removed, or changes type. Adding keys keeps the version.
const ExplainVersion = 1

// Values of PlanExplanation.PlanType.
const (
	PlanTypeVectorSearch = "vector_search"
	PlanTypeQuery        = "query"
	PlanTypeUnknown      = "unknown"
)

// Values of PredicateNode.Node, one per planpb.Expr member plus "unknown".
const (
	NodeTerm                    = "term"
	NodeUnary                   = "unary"
	NodeBinary                  = "binary"
	NodeCompare                 = "compare"
	NodeUnaryRange              = "unary_range"
	NodeBinaryRange             = "binary_range"
	NodeArithCompare            = "arith_compare"
	NodeArith                   = "arith"
	NodeValue                   = "value"
	NodeColumn                  = "column"
	NodeExists                  = "exists"
	NodeAlwaysTrue              = "always_true"
	NodeJSONContains            = "json_contains"
	NodeCall                    = "call"
	NodeNull                    = "null"
	NodeRandomSample            = "random_sample"
	NodeGIS                     = "gis"
	NodeTimestamptzArithCompare = "timestamptz_arith_compare"
	NodeElementFilter           = "element_filter"
	NodeMatch                   = "match"
	NodeBloomFilter             = "bloom_filter"
	NodeRoaringFilter           = "roaring_filter"
	NodeUnknown                 = "unknown"
)

// ExplainOptions tunes the explanation. The zero value explains every value
// verbatim and reports no candidate indexes.
type ExplainOptions struct {
	// RedactTemplateValues replaces template-supplied values with the markers
	// RedactPlanForLog uses, keyed on the node's template variable name, so
	// literals typed into the expression stay visible. A templated GIS WKT,
	// which keeps only Expr.is_template, is elided as well. Nodes the rewriter
	// rebuilt without a variable name explain as literals, as RedactPlanForLog
	// logs them.
	RedactTemplateValues bool
	// FieldIndexes lists the indexes that exist on each field, keyed by field
	// id. A plan carries no index metadata, so the caller supplies it (the
	// proxy has it from the collection's index descriptions); the explainer
	// then attaches the matching entries to the vector field and to every
	// predicate column as candidate_indexes. Nil leaves them out entirely.
	FieldIndexes map[int64][]IndexCandidate
}

// IndexCandidate is an index that exists on a field the plan reads. It is a
// candidate, not a decision: whether segcore uses it depends on the segment
// state at execution time, which a static explanation cannot know.
//
// NestedPath scopes a JSON path index to one path inside the field, in the
// form the plan's ColumnInfo uses. The caller derives it from the index's
// json_path parameter, which datacoord stores as an escaped JSON pointer
// ("/a/b", "~1" for "/" and "~0" for "~", "" for the root; see
// internal/util/typeutil.ParseAndVerifyNestedPath); the explainer does not
// parse that parameter itself. A candidate with a NestedPath is attached only
// to columns reading that path, since a typed JSON path index on `f["a"]`
// cannot serve a predicate on `f["b"]`; an empty, non-nil path means the
// field's root value. Nil attaches the candidate to every column of the field.
//
// CoversSubpaths, set from the index's cast type for a flat JSON index
// (json_cast_type JSON), also attaches the candidate to the paths below
// NestedPath, as exec/expression/IndexPathSelection.h PathMatchScore does.
// The query node additionally skips a flat index when the path below
// NestedPath has an integer component (`f["a"][0]`); the candidate list does
// not model that.
type IndexCandidate struct {
	Name           string            `json:"name"`
	Type           string            `json:"type,omitempty"`
	Params         map[string]string `json:"params,omitempty"`
	NestedPath     []string          `json:"nested_path,omitzero"`
	CoversSubpaths bool              `json:"covers_subpaths,omitempty"`
}

// coversPath reports whether c attaches to a column reading nestedPath.
func (c IndexCandidate) coversPath(nestedPath []string) bool {
	if c.NestedPath == nil {
		return true
	}
	if c.CoversSubpaths && len(nestedPath) > len(c.NestedPath) {
		nestedPath = nestedPath[:len(c.NestedPath)]
	}
	return slices.Equal(c.NestedPath, nestedPath)
}

// PlanExplanation is the top-level explanation of one plan. Exactly one of
// VectorSearch and Query is set for a well-formed plan.
//
// Plan fields outside the contract in this version, execution tuning and
// bookkeeping scalars rather than predicate or output structure:
// QueryInfo.materialized_view_involved, bm25_avgdl, query_field_id,
// search_topk_ratio, refine_topk_ratio, search_iterator_v2_info.token,
// VectorANNS.placeholder_tag, JSONContainsExpr.elements_same_type (a hint
// the parser derives from the values it already reports), and the
// expressions inside function chain operations (the chains themselves are
// summarized).
type PlanExplanation struct {
	Version        int                        `json:"version"`
	PlanType       string                     `json:"plan_type"`
	VectorSearch   *VectorSearchExplanation   `json:"vector_search"`
	Query          *QueryExplanation          `json:"query"`
	Predicates     *PredicateNode             `json:"predicates"`
	OutputFields   []FieldRef                 `json:"output_fields"`
	DynamicFields  []string                   `json:"dynamic_fields"`
	Scorers        []ScorerExplanation        `json:"scorers"`
	ScoreOption    *ScoreOptionExplanation    `json:"score_option,omitempty"`
	FunctionChains []FunctionChainExplanation `json:"function_chains,omitempty"`
	Options        *PlanOptionsExplanation    `json:"options,omitempty"`
	// Namespace is set whenever the plan carries one, an empty string
	// included: segcore adds a `$namespace == value` predicate on presence,
	// so a present empty namespace is not the same plan as an absent one.
	Namespace *string `json:"namespace,omitempty"`
	// UnknownNodeCount counts expression nodes, value kinds, and plan node
	// kinds this version does not recognize. Zero means every node of the
	// plan was recognized; it says nothing about the plan fields listed above
	// as outside the contract.
	UnknownNodeCount int `json:"unknown_node_count"`
	// Detail says why PlanType is "unknown": the Go type of an unrecognized
	// plan node kind, or that none is set.
	Detail string `json:"detail,omitempty"`
}

// FieldRef identifies a field referenced by the plan. Name is resolved from the
// schema when one is supplied and knows the id; otherwise only ID (and the
// type carried by the plan, if any) is set, so a plan built against another
// schema version still explains. The boolean flags and NestedPath come from
// the plan's own ColumnInfo and are therefore only present on predicate
// columns; CandidateIndexes is filled from ExplainOptions.FieldIndexes for
// predicate columns and the vector field.
type FieldRef struct {
	ID               int64            `json:"id"`
	Name             string           `json:"name,omitempty"`
	Type             string           `json:"type,omitempty"`
	ElementType      string           `json:"element_type,omitempty"`
	NestedPath       []string         `json:"nested_path,omitempty"`
	PrimaryKey       bool             `json:"primary_key,omitempty"`
	AutoID           bool             `json:"auto_id,omitempty"`
	PartitionKey     bool             `json:"partition_key,omitempty"`
	ClusteringKey    bool             `json:"clustering_key,omitempty"`
	Nullable         bool             `json:"nullable,omitempty"`
	ElementLevel     bool             `json:"element_level,omitempty"`
	CandidateIndexes []IndexCandidate `json:"candidate_indexes,omitempty"`
}

// VectorSearchExplanation describes the ANN part of a search plan.
type VectorSearchExplanation struct {
	Field      FieldRef `json:"field"`
	VectorType string   `json:"vector_type"`
	MetricType string   `json:"metric_type"`
	TopK       int64    `json:"topk"`
	// SearchParams is the decoded search_params object when the plan carries
	// valid JSON, the raw string when it does not, and null when empty.
	SearchParams any                  `json:"search_params"`
	RoundDecimal int64                `json:"round_decimal"`
	GroupBy      *GroupByExplanation  `json:"group_by"`
	Iterator     *IteratorExplanation `json:"iterator"`
	JSONPath     *JSONPathExplanation `json:"json_path,omitempty"`
	Hints        string               `json:"hints,omitempty"`
}

// GroupByExplanation describes search grouping.
type GroupByExplanation struct {
	Fields          []FieldRef `json:"fields"`
	GroupSize       int64      `json:"group_size"`
	StrictGroupSize bool       `json:"strict_group_size"`
}

// IteratorExplanation describes search-iterator paging. The session token is
// deliberately not reproduced.
type IteratorExplanation struct {
	BatchSize uint32   `json:"batch_size"`
	LastBound *Float32 `json:"last_bound"`
}

// JSONPathExplanation describes how a group_by on a JSON field reads its key:
// the path inside the document (empty means the whole value), the type the
// value is cast to, and whether a failed cast fails the search (strict) or
// yields a null group key. It is reported whenever the plan carries any of
// the three; segcore consults them only when the plan groups.
type JSONPathExplanation struct {
	Path       string `json:"path"`
	Type       string `json:"type"`
	StrictCast bool   `json:"strict_cast"`
}

// QueryExplanation describes a scalar query (retrieve) plan.
type QueryExplanation struct {
	IsCount    bool                    `json:"is_count"`
	Limit      int64                   `json:"limit"`
	GroupBy    []FieldRef              `json:"group_by,omitempty"`
	Aggregates []AggregateExplanation  `json:"aggregates,omitempty"`
	OrderBy    []OrderByExplanation    `json:"order_by,omitempty"`
	Cursor     *QueryCursorExplanation `json:"cursor,omitempty"`
}

// QueryCursorExplanation is the position a query iterator resumes from.
// Exactly one of LastIntPK and LastStrPK is set, matching the primary key type.
type QueryCursorExplanation struct {
	LastIntPK         *int64  `json:"last_int_pk,omitempty"`
	LastStrPK         *string `json:"last_str_pk,omitempty"`
	LastElementOffset int64   `json:"last_element_offset"`
}

// PlanOptionsExplanation carries the execution hints the parser attached to
// the plan.
type PlanOptionsExplanation struct {
	ExprUseJSONStats bool `json:"expr_use_json_stats"`
}

// AggregateExplanation is one aggregate of a query plan.
type AggregateExplanation struct {
	Op    string   `json:"op"`
	Field FieldRef `json:"field"`
}

// OrderByExplanation is one ORDER BY entry of a query plan.
type OrderByExplanation struct {
	Field      FieldRef `json:"field"`
	Ascending  bool     `json:"ascending"`
	NullsFirst bool     `json:"nulls_first"`
}

// ScorerExplanation is one score function attached to the plan.
type ScorerExplanation struct {
	Type   string            `json:"type"`
	Weight Float32           `json:"weight"`
	Params map[string]string `json:"params,omitempty"`
	Filter *PredicateNode    `json:"filter"`
}

// ScoreOptionExplanation describes how scorer results are combined.
type ScoreOptionExplanation struct {
	BoostMode    string `json:"boost_mode"`
	FunctionMode string `json:"function_mode"`
}

// FunctionChainExplanation summarizes one query-node rerank chain: its name
// (may be empty), its stage as the proto enum name, and its operations by
// name in execution order.
type FunctionChainExplanation struct {
	Name  string   `json:"name,omitempty"`
	Stage string   `json:"stage"`
	Ops   []string `json:"ops"`
}

// PredicateNode is one node of a filter tree. Node is always set; every other
// field is present only when it applies to that node kind. The two list
// fields, Values and Children, are present whenever the node kind has a list,
// an empty one included: `f in []` matches nothing and must not read like a
// term node without a list, and a call with no arguments is still a call.
type PredicateNode struct {
	Node string `json:"node"`
	// Op is the operator spelled as its proto enum name, e.g. GreaterThan,
	// LogicalAnd, IsNull, ContainsAny.
	Op          string           `json:"op,omitempty"`
	Field       *FieldRef        `json:"field,omitempty"`
	RightField  *FieldRef        `json:"right_field,omitempty"`
	Value       any              `json:"value,omitempty"`
	Values      []any            `json:"values,omitzero"`
	ExtraValues []any            `json:"extra_values,omitempty"`
	Lower       *RangeBound      `json:"lower,omitempty"`
	Upper       *RangeBound      `json:"upper,omitempty"`
	ArithOp     string           `json:"arith_op,omitempty"`
	Operand     any              `json:"operand,omitempty"`
	Left        *PredicateNode   `json:"left,omitempty"`
	Right       *PredicateNode   `json:"right,omitempty"`
	Child       *PredicateNode   `json:"child,omitempty"`
	Children    []*PredicateNode `json:"children,omitzero"`
	Element     *PredicateNode   `json:"element,omitempty"`
	Function    string           `json:"function,omitempty"`
	StructName  string           `json:"struct_name,omitempty"`
	// Count is the threshold of a match_least, match_most or match_exact
	// node; match_all and match_any take none. Zero is a valid threshold for
	// match_most and match_exact, hence the pointer.
	Count        *int64  `json:"count,omitempty"`
	SampleFactor Float32 `json:"sample_factor,omitempty"`
	// WKT is the geometry of a gis node. Under RedactTemplateValues a
	// template-supplied WKT reads "<template elided>" without a variable name,
	// because the plan does not keep one for GIS templates.
	WKT string `json:"wkt,omitempty"`
	// Distance is the radius of a gis DWithin node, the only gis operator that
	// takes one. Zero is a valid radius, hence the pointer.
	Distance *Float64             `json:"distance,omitempty"`
	Interval *IntervalExplanation `json:"interval,omitempty"`
	Blob     *BlobSummary         `json:"blob,omitempty"`
	// InField marks the reversed membership form of a term node, literal in
	// field: the values are tested for membership in the array or JSON field,
	// instead of the field's value being tested against the list.
	InField bool `json:"in_field,omitempty"`
	// Template is the expression-template variable the value(s) came from.
	// For arith_compare it names the template behind Value; OperandTemplate
	// names the one behind Operand.
	Template        string `json:"template,omitempty"`
	OperandTemplate string `json:"operand_template,omitempty"`
	// Detail says why Node is "unknown": the Go type of an unrecognized
	// expression kind, or that the expression is missing or not set.
	Detail string `json:"detail,omitempty"`
}

// RangeBound is one side of a binary range.
type RangeBound struct {
	Value     any    `json:"value"`
	Inclusive bool   `json:"inclusive"`
	Template  string `json:"template,omitempty"`
}

// IntervalExplanation is a calendar interval of a timestamptz arithmetic node.
type IntervalExplanation struct {
	Years   int64 `json:"years,omitempty"`
	Months  int64 `json:"months,omitempty"`
	Days    int64 `json:"days,omitempty"`
	Hours   int64 `json:"hours,omitempty"`
	Minutes int64 `json:"minutes,omitempty"`
	Seconds int64 `json:"seconds,omitempty"`
}

// BlobSummary stands in for a binary payload (membership filter blobs can be
// tens of MiB) so an explanation stays small enough to log or return.
type BlobSummary struct {
	Bytes int `json:"bytes"`
}

func (b *BlobSummary) String() string {
	return fmt.Sprintf("<%d bytes>", b.Bytes)
}

// Float32 and Float64 are the explanation's float types. They marshal as JSON
// numbers that always carry a decimal point or an exponent (2.0, 1e+21), so an
// integral float is not read as an integer: on a JSON field the plan's numeric
// kind decides between integer and floating-point arithmetic, and `f / 2 == 1`
// and `f / 2.0 == 1` are different plans. NaN and ±Inf, which JSON cannot
// carry and which the parser does let through (a scorer weight of "NaN"
// parses, and the constant `1e308 * 10` folds to +Inf), become the object
// {"non_finite": "NaN"} (or "+Inf", "-Inf") so one such value cannot fail the
// whole explanation. An object rather than a bare string because a JSON field
// can be compared with either the number NaN or the string "NaN".
type Float32 float32

// Float64 is the 64-bit counterpart of Float32.
type Float64 float64

// NonFinite is the JSON form of a NaN or infinite float.
type NonFinite struct {
	Value string `json:"non_finite"`
}

func (f Float32) MarshalJSON() ([]byte, error) { return marshalFloat(float64(f), 32) }

func (f Float64) MarshalJSON() ([]byte, error) { return marshalFloat(float64(f), 64) }

func marshalFloat(v float64, bits int) ([]byte, error) {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return json.Marshal(NonFinite{Value: strconv.FormatFloat(v, 'g', -1, bits)})
	}
	return []byte(formatFloat(v, bits)), nil
}

// formatFloat spells a finite float the way encoding/json does (shortest
// digits at the value's own width, so a float32 0.1 stays 0.1; fixed notation
// unless the exponent is below -6 or at least 21), then gives an integral
// value a decimal point so 2.0 is not read as the integer 2. Exponent forms
// already read as floats.
func formatFloat(v float64, bits int) string {
	format := byte('f')
	if abs := math.Abs(v); abs != 0 {
		if bits == 64 && (abs < 1e-6 || abs >= 1e21) || bits == 32 && (float32(abs) < 1e-6 || float32(abs) >= 1e21) {
			format = 'e'
		}
	}
	s := strconv.FormatFloat(v, format, -1, bits)
	if format == 'e' {
		// encoding/json also drops the exponent's leading zero: e-09 to e-9.
		if n := len(s); n >= 4 && s[n-4] == 'e' && s[n-3] == '-' && s[n-2] == '0' {
			s = s[:n-2] + s[n-1:]
		}
		return s
	}
	if !strings.Contains(s, ".") {
		s += ".0"
	}
	return s
}

// floatText is the text-view spelling of a float: the JSON spelling for
// finite values, and NaN, +Inf or -Inf otherwise.
func floatText(v float64, bits int) string {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return strconv.FormatFloat(v, 'g', -1, bits)
	}
	return formatFloat(v, bits)
}

// ExplainPlan describes a plan without executing it. schema may be nil, in
// which case fields are reported by id only. opts may be nil.
func ExplainPlan(plan *planpb.PlanNode, schema *typeutil.SchemaHelper, opts *ExplainOptions) (*PlanExplanation, error) {
	if plan == nil {
		// Reaching here is a caller bug, never something a request can cause.
		return nil, merr.WrapErrServiceInternalMsg("cannot explain a nil plan")
	}
	e := &explainer{schema: schema}
	if opts != nil {
		e.opts = *opts
	}
	out := &PlanExplanation{
		Version:       ExplainVersion,
		OutputFields:  e.fields(plan.GetOutputFieldIds()),
		DynamicFields: append([]string{}, plan.GetDynamicFields()...),
		Scorers:       make([]ScorerExplanation, 0, len(plan.GetScorers())),
	}
	if plan.Namespace != nil {
		namespace := plan.GetNamespace()
		out.Namespace = &namespace
	}

	switch node := plan.GetNode().(type) {
	case *planpb.PlanNode_VectorAnns:
		out.PlanType = PlanTypeVectorSearch
		out.VectorSearch = e.vectorSearch(node.VectorAnns)
		out.Predicates = e.expr(node.VectorAnns.GetPredicates())
	case *planpb.PlanNode_Query:
		out.PlanType = PlanTypeQuery
		out.Query = e.query(node.Query)
		out.Predicates = e.expr(node.Query.GetPredicates())
	case *planpb.PlanNode_Predicates:
		// Deprecated bare-predicate form, still accepted by segcore as a
		// retrieve without limit.
		out.PlanType = PlanTypeQuery
		out.Query = &QueryExplanation{}
		out.Predicates = e.expr(node.Predicates)
	default:
		e.unknown++
		out.PlanType = PlanTypeUnknown
		out.Detail = fmt.Sprintf("%T", node)
		if node == nil {
			out.Detail = "plan node not set"
		}
	}

	for _, scorer := range plan.GetScorers() {
		out.Scorers = append(out.Scorers, ScorerExplanation{
			Type:   scorer.GetType().String(),
			Weight: Float32(scorer.GetWeight()),
			Params: keyValuePairs(scorer.GetParams()),
			Filter: e.expr(scorer.GetFilter()),
		})
	}
	if option := plan.GetScoreOption(); option != nil {
		out.ScoreOption = &ScoreOptionExplanation{
			BoostMode:    option.GetBoostMode().String(),
			FunctionMode: option.GetFunctionMode().String(),
		}
	}
	for _, chain := range plan.GetQuerynodeFunctionChains() {
		ops := make([]string, 0, len(chain.GetOps()))
		for _, op := range chain.GetOps() {
			ops = append(ops, op.GetOp())
		}
		out.FunctionChains = append(out.FunctionChains, FunctionChainExplanation{
			Name:  chain.GetName(),
			Stage: chain.GetStage().String(),
			Ops:   ops,
		})
	}
	if options := plan.GetPlanOptions(); options != nil {
		out.Options = &PlanOptionsExplanation{ExprUseJSONStats: options.GetExprUseJsonStats()}
	}
	out.UnknownNodeCount = e.unknown
	return out, nil
}

// JSON renders the explanation in its stable wire form.
func (p *PlanExplanation) JSON() ([]byte, error) {
	return json.Marshal(p)
}

// Text renders a compact human-readable view: one header line per plan
// section and one indented line per predicate node. It is meant for people
// and logs; its layout is not covered by ExplainVersion, the JSON form is.
func (p *PlanExplanation) Text() string {
	var b strings.Builder
	if p.Detail != "" {
		fmt.Fprintf(&b, "plan: %s (%s)\n", p.PlanType, p.Detail)
	} else {
		fmt.Fprintf(&b, "plan: %s\n", p.PlanType)
	}
	if p.Namespace != nil {
		fmt.Fprintf(&b, "namespace: %q\n", *p.Namespace)
	}
	if vs := p.VectorSearch; vs != nil {
		fmt.Fprintf(&b, "search: %s (%s) metric=%s topk=%d round_decimal=%d%s\n",
			fieldLabel(vs.Field), vs.VectorType, vs.MetricType, vs.TopK, vs.RoundDecimal, indexLabel(vs.Field))
		if jp := vs.JSONPath; jp != nil {
			fmt.Fprintf(&b, "json_path: %q type=%s strict_cast=%t\n", jp.Path, jp.Type, jp.StrictCast)
		}
		if vs.SearchParams != nil {
			if params, err := json.Marshal(vs.SearchParams); err == nil {
				fmt.Fprintf(&b, "search_params: %s\n", params)
			}
		}
		if g := vs.GroupBy; g != nil {
			fmt.Fprintf(&b, "group_by: %s group_size=%d strict=%t\n",
				strings.Join(fieldLabels(g.Fields), ", "), g.GroupSize, g.StrictGroupSize)
		}
		if it := vs.Iterator; it != nil {
			fmt.Fprintf(&b, "iterator: batch_size=%d", it.BatchSize)
			if it.LastBound != nil {
				fmt.Fprintf(&b, " last_bound=%s", floatText(float64(*it.LastBound), 32))
			}
			b.WriteByte('\n')
		}
		if vs.Hints != "" {
			fmt.Fprintf(&b, "hints: %s\n", vs.Hints)
		}
	}
	if q := p.Query; q != nil {
		fmt.Fprintf(&b, "query: is_count=%t limit=%d\n", q.IsCount, q.Limit)
		if len(q.GroupBy) > 0 {
			fmt.Fprintf(&b, "group_by: %s\n", strings.Join(fieldLabels(q.GroupBy), ", "))
		}
		for _, agg := range q.Aggregates {
			fmt.Fprintf(&b, "aggregate: %s(%s)\n", agg.Op, fieldLabel(agg.Field))
		}
		for _, ob := range q.OrderBy {
			fmt.Fprintf(&b, "order_by: %s ascending=%t nulls_first=%t\n", fieldLabel(ob.Field), ob.Ascending, ob.NullsFirst)
		}
		if c := q.Cursor; c != nil {
			switch {
			case c.LastIntPK != nil:
				fmt.Fprintf(&b, "cursor: last_pk=%d last_element_offset=%d\n", *c.LastIntPK, c.LastElementOffset)
			case c.LastStrPK != nil:
				fmt.Fprintf(&b, "cursor: last_pk=%q last_element_offset=%d\n", *c.LastStrPK, c.LastElementOffset)
			default:
				fmt.Fprintf(&b, "cursor: last_element_offset=%d\n", c.LastElementOffset)
			}
		}
	}
	if p.Predicates != nil {
		b.WriteString("filter:\n")
		writeNode(&b, p.Predicates, 1)
	} else {
		b.WriteString("filter: none\n")
	}
	if len(p.OutputFields) > 0 {
		fmt.Fprintf(&b, "output_fields: %s\n", strings.Join(fieldLabels(p.OutputFields), ", "))
	}
	if len(p.DynamicFields) > 0 {
		// Dynamic field names are JSON keys and may contain the separator.
		names := make([]string, 0, len(p.DynamicFields))
		for _, name := range p.DynamicFields {
			names = append(names, strconv.Quote(name))
		}
		fmt.Fprintf(&b, "dynamic_fields: %s\n", strings.Join(names, ", "))
	}
	for i, s := range p.Scorers {
		fmt.Fprintf(&b, "scorer[%d]: %s weight=%s", i, s.Type, floatText(float64(s.Weight), 32))
		if len(s.Params) > 0 {
			// Keys are sorted by the JSON encoder, so the line is stable.
			if params, err := json.Marshal(s.Params); err == nil {
				fmt.Fprintf(&b, " params=%s", params)
			}
		}
		b.WriteByte('\n')
		if s.Filter != nil {
			writeNode(&b, s.Filter, 1)
		}
	}
	if so := p.ScoreOption; so != nil {
		fmt.Fprintf(&b, "score_option: boost_mode=%s function_mode=%s\n", so.BoostMode, so.FunctionMode)
	}
	for i, chain := range p.FunctionChains {
		ops := make([]string, 0, len(chain.Ops))
		for _, op := range chain.Ops {
			ops = append(ops, strconv.Quote(op))
		}
		fmt.Fprintf(&b, "function_chain[%d]: name=%q stage=%s ops=[%s]\n", i, chain.Name, chain.Stage, strings.Join(ops, ", "))
	}
	if o := p.Options; o != nil {
		fmt.Fprintf(&b, "options: expr_use_json_stats=%t\n", o.ExprUseJSONStats)
	}
	fmt.Fprintf(&b, "unknown_nodes: %d\n", p.UnknownNodeCount)
	return b.String()
}

type explainer struct {
	schema  *typeutil.SchemaHelper
	opts    ExplainOptions
	unknown int
}

func (e *explainer) vectorSearch(anns *planpb.VectorANNS) *VectorSearchExplanation {
	info := anns.GetQueryInfo()
	field := e.field(anns.GetFieldId())
	field.CandidateIndexes = e.candidateIndexes(anns.GetFieldId(), nil)
	vs := &VectorSearchExplanation{
		Field:        field,
		VectorType:   anns.GetVectorType().String(),
		MetricType:   info.GetMetricType(),
		TopK:         info.GetTopk(),
		SearchParams: decodeSearchParams(info.GetSearchParams()),
		RoundDecimal: info.GetRoundDecimal(),
		Hints:        info.GetHints(),
	}
	groupIDs := info.GetGroupByFieldIds()
	// Older producers fill the single-field group_by_field_id instead of the
	// list; -1 and 0 both mean "no grouping" there.
	if len(groupIDs) == 0 && info.GetGroupByFieldId() > 0 {
		groupIDs = []int64{info.GetGroupByFieldId()}
	}
	if len(groupIDs) > 0 {
		vs.GroupBy = &GroupByExplanation{
			Fields:          e.fields(groupIDs),
			GroupSize:       info.GetGroupSize(),
			StrictGroupSize: info.GetStrictGroupSize(),
		}
	}
	if it := info.GetSearchIteratorV2Info(); it != nil {
		vs.Iterator = &IteratorExplanation{BatchSize: it.GetBatchSize()}
		if it.LastBound != nil {
			bound := Float32(it.GetLastBound())
			vs.Iterator.LastBound = &bound
		}
	}
	// Grouping by a bare JSON field leaves the path empty while the type and
	// strict_cast still apply (segcore casts the whole value), so the path
	// alone does not decide whether there is anything to report.
	if info.GetJsonPath() != "" || info.GetJsonType() != schemapb.DataType_None || info.GetStrictCast() {
		vs.JSONPath = &JSONPathExplanation{
			Path:       info.GetJsonPath(),
			Type:       info.GetJsonType().String(),
			StrictCast: info.GetStrictCast(),
		}
	}
	return vs
}

func (e *explainer) query(q *planpb.QueryPlanNode) *QueryExplanation {
	out := &QueryExplanation{
		IsCount: q.GetIsCount(),
		Limit:   q.GetLimit(),
	}
	if ids := q.GetGroupByFieldIds(); len(ids) > 0 {
		out.GroupBy = e.fields(ids)
	}
	for _, agg := range q.GetAggregates() {
		out.Aggregates = append(out.Aggregates, AggregateExplanation{
			Op:    agg.GetOp().String(),
			Field: e.field(agg.GetFieldId()),
		})
	}
	for _, ob := range q.GetOrderByFields() {
		out.OrderBy = append(out.OrderBy, OrderByExplanation{
			Field:      e.field(ob.GetFieldId()),
			Ascending:  ob.GetAscending(),
			NullsFirst: ob.GetNullsFirst(),
		})
	}
	if cursor := q.GetQueryIteratorCursor(); cursor != nil {
		out.Cursor = &QueryCursorExplanation{LastElementOffset: cursor.GetLastElementOffset()}
		if cursor.LastIntPk != nil {
			pk := cursor.GetLastIntPk()
			out.Cursor.LastIntPK = &pk
		}
		if cursor.LastStrPk != nil {
			pk := cursor.GetLastStrPk()
			out.Cursor.LastStrPK = &pk
		}
	}
	return out
}

// expr explains a predicate root. A nil root means "no filter" and yields nil;
// nil children inside the tree are reported by child instead.
func (e *explainer) expr(expr *planpb.Expr) *PredicateNode {
	if expr == nil {
		return nil
	}
	switch node := expr.GetExpr().(type) {
	case *planpb.Expr_TermExpr:
		return e.term(node.TermExpr)
	case *planpb.Expr_UnaryExpr:
		return &PredicateNode{
			Node:  NodeUnary,
			Op:    node.UnaryExpr.GetOp().String(),
			Child: e.child(node.UnaryExpr.GetChild()),
		}
	case *planpb.Expr_BinaryExpr:
		return &PredicateNode{
			Node:  NodeBinary,
			Op:    node.BinaryExpr.GetOp().String(),
			Left:  e.child(node.BinaryExpr.GetLeft()),
			Right: e.child(node.BinaryExpr.GetRight()),
		}
	case *planpb.Expr_CompareExpr:
		return &PredicateNode{
			Node:       NodeCompare,
			Op:         node.CompareExpr.GetOp().String(),
			Field:      e.column(node.CompareExpr.GetLeftColumnInfo()),
			RightField: e.column(node.CompareExpr.GetRightColumnInfo()),
		}
	case *planpb.Expr_UnaryRangeExpr:
		return e.unaryRange(node.UnaryRangeExpr)
	case *planpb.Expr_BinaryRangeExpr:
		return e.binaryRange(node.BinaryRangeExpr)
	case *planpb.Expr_BinaryArithOpEvalRangeExpr:
		return e.arithCompare(node.BinaryArithOpEvalRangeExpr)
	case *planpb.Expr_BinaryArithExpr:
		arith := node.BinaryArithExpr
		n := &PredicateNode{
			Node: NodeArith,
			Op:   arith.GetOp().String(),
			Left: e.child(arith.GetLeft()),
		}
		// ArrayLength is the one unary arithmetic op; the parser leaves its
		// right operand nil on purpose, so that is not a missing child.
		if arith.GetOp() != planpb.ArithOpType_ArrayLength || arith.GetRight() != nil {
			n.Right = e.child(arith.GetRight())
		}
		return n
	case *planpb.Expr_ValueExpr:
		return &PredicateNode{
			Node:     NodeValue,
			Value:    e.value(node.ValueExpr.GetValue(), node.ValueExpr.GetTemplateVariableName()),
			Template: node.ValueExpr.GetTemplateVariableName(),
		}
	case *planpb.Expr_ColumnExpr:
		return &PredicateNode{
			Node:  NodeColumn,
			Field: e.column(node.ColumnExpr.GetInfo()),
		}
	case *planpb.Expr_ExistsExpr:
		return &PredicateNode{
			Node:  NodeExists,
			Field: e.column(node.ExistsExpr.GetInfo()),
		}
	case *planpb.Expr_AlwaysTrueExpr:
		return &PredicateNode{Node: NodeAlwaysTrue}
	case *planpb.Expr_JsonContainsExpr:
		return &PredicateNode{
			Node:     NodeJSONContains,
			Op:       node.JsonContainsExpr.GetOp().String(),
			Field:    e.column(node.JsonContainsExpr.GetColumnInfo()),
			Values:   e.values(node.JsonContainsExpr.GetElements(), node.JsonContainsExpr.GetTemplateVariableName()),
			Template: node.JsonContainsExpr.GetTemplateVariableName(),
		}
	case *planpb.Expr_CallExpr:
		params := node.CallExpr.GetFunctionParameters()
		children := make([]*PredicateNode, 0, len(params))
		for _, param := range params {
			children = append(children, e.child(param))
		}
		return &PredicateNode{
			Node:     NodeCall,
			Function: node.CallExpr.GetFunctionName(),
			Children: children,
		}
	case *planpb.Expr_NullExpr:
		return &PredicateNode{
			Node:  NodeNull,
			Op:    node.NullExpr.GetOp().String(),
			Field: e.column(node.NullExpr.GetColumnInfo()),
		}
	case *planpb.Expr_RandomSampleExpr:
		// The parser folds `X && random_sample(f)` into one RandomSampleExpr
		// whose predicate is X, and a bare random_sample(f) carries none. A nil
		// predicate therefore means "sample every row", not a malformed tree.
		sample := node.RandomSampleExpr
		return &PredicateNode{
			Node:         NodeRandomSample,
			SampleFactor: Float32(sample.GetSampleFactor()),
			Child:        e.expr(sample.GetPredicate()),
		}
	case *planpb.Expr_GisfunctionFilterExpr:
		gis := node.GisfunctionFilterExpr
		n := &PredicateNode{
			Node:  NodeGIS,
			Op:    gis.GetOp().String(),
			Field: e.column(gis.GetColumnInfo()),
			WKT:   gis.GetWktString(),
		}
		// A GIS template keeps no variable name: the parser stores the name in
		// wkt_string and filling replaces it with validated WKT, leaving only
		// Expr.is_template. An unparsable wkt_string on a template node is
		// therefore the unfilled placeholder.
		if expr.GetIsTemplate() && n.WKT != "" {
			if checkValidWKT(n.WKT) != nil {
				n.Template, n.WKT = n.WKT, ""
			} else if e.opts.RedactTemplateValues {
				n.WKT = "<template elided>"
			}
		}
		if gis.GetOp() == planpb.GISFunctionFilterExpr_DWithin {
			distance := Float64(gis.GetDistance())
			n.Distance = &distance
		}
		return n
	case *planpb.Expr_TimestamptzArithCompareExpr:
		ts := node.TimestamptzArithCompareExpr
		return &PredicateNode{
			Node:     NodeTimestamptzArithCompare,
			Op:       ts.GetCompareOp().String(),
			Field:    e.column(ts.GetTimestamptzColumn()),
			ArithOp:  ts.GetArithOp().String(),
			Interval: interval(ts.GetInterval()),
			Value:    e.value(ts.GetCompareValue(), ""),
		}
	case *planpb.Expr_ElementFilterExpr:
		// Element is the per-element condition and is always present. Child is
		// the optional doc-level predicate the parser folds in from
		// `X && element_filter(...)`; a bare element_filter has none.
		ef := node.ElementFilterExpr
		return &PredicateNode{
			Node:       NodeElementFilter,
			StructName: ef.GetStructName(),
			Element:    e.child(ef.GetElementExpr()),
			Child:      e.expr(ef.GetPredicate()),
		}
	case *planpb.Expr_MatchExpr:
		m := node.MatchExpr
		n := &PredicateNode{
			Node:       NodeMatch,
			Op:         m.GetMatchType().String(),
			StructName: m.GetStructName(),
			Child:      e.child(m.GetPredicate()),
		}
		switch m.GetMatchType() {
		case planpb.MatchType_MatchLeast, planpb.MatchType_MatchMost, planpb.MatchType_MatchExact:
			count := m.GetCount()
			n.Count = &count
		}
		return n
	case *planpb.Expr_BloomFilterExpr:
		return &PredicateNode{
			Node:  NodeBloomFilter,
			Field: e.column(node.BloomFilterExpr.GetColumnInfo()),
			Blob:  &BlobSummary{Bytes: len(node.BloomFilterExpr.GetFilterBlob())},
		}
	case *planpb.Expr_RoaringFilterExpr:
		return &PredicateNode{
			Node:  NodeRoaringFilter,
			Field: e.column(node.RoaringFilterExpr.GetColumnInfo()),
			Blob:  &BlobSummary{Bytes: len(node.RoaringFilterExpr.GetBitmapBlob())},
		}
	default:
		e.unknown++
		detail := fmt.Sprintf("%T", node)
		if node == nil {
			detail = "expression not set"
		}
		return &PredicateNode{Node: NodeUnknown, Detail: detail}
	}
}

// child explains a nested expression. A nil child is malformed rather than
// "no filter", so it is reported as unknown instead of being dropped.
func (e *explainer) child(expr *planpb.Expr) *PredicateNode {
	if expr == nil {
		e.unknown++
		return &PredicateNode{Node: NodeUnknown, Detail: "missing child expression"}
	}
	return e.expr(expr)
}

func (e *explainer) term(t *planpb.TermExpr) *PredicateNode {
	return &PredicateNode{
		Node:     NodeTerm,
		Field:    e.column(t.GetColumnInfo()),
		Values:   e.values(t.GetValues(), t.GetTemplateVariableName()),
		InField:  t.GetIsInField(),
		Template: t.GetTemplateVariableName(),
	}
}

func (e *explainer) unaryRange(r *planpb.UnaryRangeExpr) *PredicateNode {
	n := &PredicateNode{
		Node:     NodeUnaryRange,
		Op:       r.GetOp().String(),
		Field:    e.column(r.GetColumnInfo()),
		Value:    e.value(r.GetValue(), r.GetTemplateVariableName()),
		Template: r.GetTemplateVariableName(),
	}
	if extras := r.GetExtraValues(); len(extras) > 0 {
		n.ExtraValues = e.values(extras, "")
	}
	return n
}

func (e *explainer) binaryRange(r *planpb.BinaryRangeExpr) *PredicateNode {
	return &PredicateNode{
		Node:  NodeBinaryRange,
		Field: e.column(r.GetColumnInfo()),
		Lower: &RangeBound{
			Value:     e.value(r.GetLowerValue(), r.GetLowerTemplateVariableName()),
			Inclusive: r.GetLowerInclusive(),
			Template:  r.GetLowerTemplateVariableName(),
		},
		Upper: &RangeBound{
			Value:     e.value(r.GetUpperValue(), r.GetUpperTemplateVariableName()),
			Inclusive: r.GetUpperInclusive(),
			Template:  r.GetUpperTemplateVariableName(),
		},
	}
}

func (e *explainer) arithCompare(a *planpb.BinaryArithOpEvalRangeExpr) *PredicateNode {
	return &PredicateNode{
		Node:            NodeArithCompare,
		Op:              a.GetOp().String(),
		Field:           e.column(a.GetColumnInfo()),
		ArithOp:         a.GetArithOp().String(),
		Operand:         e.value(a.GetRightOperand(), a.GetOperandTemplateVariableName()),
		Value:           e.value(a.GetValue(), a.GetValueTemplateVariableName()),
		Template:        a.GetValueTemplateVariableName(),
		OperandTemplate: a.GetOperandTemplateVariableName(),
	}
}

// column describes the field a predicate reads, preferring the type the plan
// itself carries over the schema's so the explanation matches what segcore
// will evaluate.
func (e *explainer) column(info *planpb.ColumnInfo) *FieldRef {
	if info == nil {
		return nil
	}
	ref := e.field(info.GetFieldId())
	if info.GetDataType() != schemapb.DataType_None {
		ref.Type = info.GetDataType().String()
	}
	if info.GetElementType() != schemapb.DataType_None {
		ref.ElementType = info.GetElementType().String()
	}
	if path := info.GetNestedPath(); len(path) > 0 {
		ref.NestedPath = slices.Clone(path)
	}
	ref.PrimaryKey = info.GetIsPrimaryKey()
	ref.AutoID = info.GetIsAutoID()
	ref.PartitionKey = info.GetIsPartitionKey()
	ref.ClusteringKey = info.GetIsClusteringKey()
	ref.Nullable = info.GetNullable()
	ref.ElementLevel = info.GetIsElementLevel()
	ref.CandidateIndexes = e.candidateIndexes(info.GetFieldId(), info.GetNestedPath())
	return &ref
}

// candidateIndexes copies the caller-supplied indexes for a field that serve
// a column reading nestedPath (see IndexCandidate.coversPath), params maps
// and paths included, so the explanation does not alias the caller's data.
func (e *explainer) candidateIndexes(id int64, nestedPath []string) []IndexCandidate {
	var out []IndexCandidate
	for _, index := range e.opts.FieldIndexes[id] {
		if !index.coversPath(nestedPath) {
			continue
		}
		index.Params = maps.Clone(index.Params)
		index.NestedPath = slices.Clone(index.NestedPath)
		out = append(out, index)
	}
	return out
}

func (e *explainer) field(id int64) FieldRef {
	ref := FieldRef{ID: id}
	if e.schema == nil {
		return ref
	}
	fs, err := e.schema.GetFieldFromID(id)
	if err != nil {
		return ref
	}
	ref.Name = fs.GetName()
	ref.Type = fs.GetDataType().String()
	if fs.GetElementType() != schemapb.DataType_None {
		ref.ElementType = fs.GetElementType().String()
	}
	return ref
}

func (e *explainer) fields(ids []int64) []FieldRef {
	out := make([]FieldRef, 0, len(ids))
	for _, id := range ids {
		out = append(out, e.field(id))
	}
	return out
}

func (e *explainer) value(v *planpb.GenericValue, template string) any {
	if v == nil {
		return nil
	}
	if e.opts.RedactTemplateValues && template != "" {
		return fmt.Sprintf("<template %s elided>", template)
	}
	return e.genericValue(v)
}

func (e *explainer) values(vs []*planpb.GenericValue, template string) []any {
	if e.opts.RedactTemplateValues && template != "" && len(vs) > 0 {
		return []any{fmt.Sprintf("<template %s elided, %d values>", template, len(vs))}
	}
	out := make([]any, 0, len(vs))
	for _, v := range vs {
		out = append(out, e.genericValue(v))
	}
	return out
}

func (e *explainer) genericValue(v *planpb.GenericValue) any {
	switch val := v.GetVal().(type) {
	case nil:
		return nil
	case *planpb.GenericValue_BoolVal:
		return val.BoolVal
	case *planpb.GenericValue_Int64Val:
		return val.Int64Val
	case *planpb.GenericValue_FloatVal:
		return Float64(val.FloatVal)
	case *planpb.GenericValue_StringVal:
		return val.StringVal
	case *planpb.GenericValue_ArrayVal:
		return e.values(val.ArrayVal.GetArray(), "")
	case *planpb.GenericValue_BytesVal:
		return &BlobSummary{Bytes: len(val.BytesVal)}
	default:
		e.unknown++
		return fmt.Sprintf("<unknown value %T>", val)
	}
}

func interval(iv *planpb.Interval) *IntervalExplanation {
	if iv == nil {
		return nil
	}
	return &IntervalExplanation{
		Years:   iv.GetYears(),
		Months:  iv.GetMonths(),
		Days:    iv.GetDays(),
		Hours:   iv.GetHours(),
		Minutes: iv.GetMinutes(),
		Seconds: iv.GetSeconds(),
	}
}

// decodeSearchParams turns the caller-written search_params JSON into a value
// that re-encodes as the same document. Numbers stay as written (json.Number)
// rather than passing through float64, which would round integers above 2^53.
// Anything that is not valid JSON is kept verbatim as a string.
func decodeSearchParams(raw string) any {
	if raw == "" {
		return nil
	}
	if !json.Valid([]byte(raw)) {
		return raw
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var parsed any
	if err := dec.Decode(&parsed); err != nil {
		return raw
	}
	return parsed
}

func keyValuePairs(pairs []*commonpb.KeyValuePair) map[string]string {
	if len(pairs) == 0 {
		return nil
	}
	out := make(map[string]string, len(pairs))
	for _, kv := range pairs {
		out[kv.GetKey()] = kv.GetValue()
	}
	return out
}

func fieldLabel(f FieldRef) string {
	label := f.Name
	if label == "" {
		label = "#" + strconv.FormatInt(f.ID, 10)
	}
	// Keys are quoted so one key containing "][" does not read like two. An
	// ARRAY field can only be subscripted by an integer (the parser rejects a
	// quoted key there), so its subscripts stay bare. A JSON path component is
	// stored as a string whether it was written [0] or ["0"], and segcore reads
	// both the same way, so JSON components are always quoted.
	isArray := f.Type == schemapb.DataType_Array.String()
	for _, key := range f.NestedPath {
		if _, err := strconv.ParseInt(key, 10, 64); !isArray || err != nil {
			key = strconv.Quote(key)
		}
		label += "[" + key + "]"
	}
	return label
}

func fieldLabels(fields []FieldRef) []string {
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		out = append(out, fieldLabel(f))
	}
	return out
}

// indexLabel renders a field's candidate indexes as " indexes=[name:type,...]",
// or nothing when there are none.
func indexLabel(f FieldRef) string {
	if len(f.CandidateIndexes) == 0 {
		return ""
	}
	names := make([]string, 0, len(f.CandidateIndexes))
	for _, idx := range f.CandidateIndexes {
		if idx.Type != "" {
			names = append(names, idx.Name+":"+idx.Type)
		} else {
			names = append(names, idx.Name)
		}
	}
	return " indexes=[" + strings.Join(names, ",") + "]"
}

// isoDuration spells an interval the way the expression language accepts it,
// as an ISO 8601 duration such as P1DT12H; a zero interval is PT0S.
func isoDuration(iv *IntervalExplanation) string {
	date := durationPart(iv.Years, "Y") + durationPart(iv.Months, "M") + durationPart(iv.Days, "D")
	clock := durationPart(iv.Hours, "H") + durationPart(iv.Minutes, "M") + durationPart(iv.Seconds, "S")
	switch {
	case clock != "":
		return "P" + date + "T" + clock
	case date != "":
		return "P" + date
	default:
		return "PT0S"
	}
}

func durationPart(n int64, unit string) string {
	if n == 0 {
		return ""
	}
	return strconv.FormatInt(n, 10) + unit
}

func writeNode(b *strings.Builder, n *PredicateNode, depth int) {
	b.WriteString(strings.Repeat("  ", depth))
	b.WriteString(describeNode(n))
	b.WriteByte('\n')
	for _, child := range []*PredicateNode{n.Element, n.Left, n.Right, n.Child} {
		if child != nil {
			writeNode(b, child, depth+1)
		}
	}
	for _, child := range n.Children {
		writeNode(b, child, depth+1)
	}
}

// textValue renders a predicate value for the text view. Strings are quoted
// so element boundaries survive (["a b", "c"] and ["a", "b c"] differ),
// floats keep the JSON spelling so 2.0 is not read as 2, and a value the plan
// left empty (an unfilled template bound) prints as null, as in JSON.
func textValue(v any) string {
	switch val := v.(type) {
	case nil:
		return "null"
	case string:
		return strconv.Quote(val)
	case Float64:
		return floatText(float64(val), 64)
	case []any:
		parts := make([]string, len(val))
		for i, item := range val {
			parts[i] = textValue(item)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	default:
		return fmt.Sprintf("%v", val)
	}
}

func describeNode(n *PredicateNode) string {
	parts := []string{n.Node}
	if n.Op != "" {
		parts = append(parts, n.Op)
	}
	if n.Function != "" {
		parts = append(parts, n.Function)
	}
	if n.Field != nil {
		parts = append(parts, fieldLabel(*n.Field)+indexLabel(*n.Field))
	}
	if n.RightField != nil {
		parts = append(parts, fieldLabel(*n.RightField)+indexLabel(*n.RightField))
	}
	if n.InField {
		parts = append(parts, "in_field")
	}
	if n.ArithOp != "" {
		parts = append(parts, "arith="+n.ArithOp)
	}
	if n.Interval != nil {
		parts = append(parts, "interval="+isoDuration(n.Interval))
	}
	if n.Operand != nil {
		parts = append(parts, "operand="+textValue(n.Operand))
	}
	if n.Value != nil {
		parts = append(parts, textValue(n.Value))
	}
	if n.Values != nil {
		// Present but empty prints as [], as in JSON: `f in []` matches nothing.
		parts = append(parts, textValue(n.Values))
	}
	if len(n.ExtraValues) > 0 {
		parts = append(parts, "extra="+textValue(n.ExtraValues))
	}
	if n.Lower != nil {
		parts = append(parts, fmt.Sprintf("lower=%s inclusive=%t", textValue(n.Lower.Value), n.Lower.Inclusive))
	}
	if n.Upper != nil {
		parts = append(parts, fmt.Sprintf("upper=%s inclusive=%t", textValue(n.Upper.Value), n.Upper.Inclusive))
	}
	if n.StructName != "" {
		parts = append(parts, "struct="+n.StructName)
	}
	if n.Count != nil {
		parts = append(parts, fmt.Sprintf("count=%d", *n.Count))
	}
	if n.SampleFactor != 0 {
		parts = append(parts, "sample_factor="+floatText(float64(n.SampleFactor), 32))
	}
	if n.WKT != "" {
		parts = append(parts, "wkt="+n.WKT)
	}
	if n.Distance != nil {
		parts = append(parts, "distance="+floatText(float64(*n.Distance), 64))
	}
	if n.Blob != nil {
		parts = append(parts, "blob="+n.Blob.String())
	}
	if n.Template != "" {
		parts = append(parts, "template="+n.Template)
	}
	if n.OperandTemplate != "" {
		parts = append(parts, "operand_template="+n.OperandTemplate)
	}
	if n.Lower != nil && n.Lower.Template != "" {
		parts = append(parts, "lower_template="+n.Lower.Template)
	}
	if n.Upper != nil && n.Upper.Template != "" {
		parts = append(parts, "upper_template="+n.Upper.Template)
	}
	if n.Detail != "" {
		parts = append(parts, n.Detail)
	}
	return strings.Join(parts, " ")
}
