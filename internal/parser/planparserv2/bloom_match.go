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

import (
	"encoding/binary"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	parser "github.com/milvus-io/milvus/internal/parser/planparserv2/generated"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// MBF1 envelope constants, kept in sync with the client builder
// (client/v3/sbbf) and the C++ prober (SplitBlockBloomFilterView) via the
// shared golden vectors. The server (this file) validates the envelope
// independently rather than importing the client SDK module: this package is
// compiled into the plan-parser c-shared library, which must not depend on the
// standalone client/v3 module.
const (
	mbf1Magic          = "MBF1"
	mbf1Version        = 1
	mbf1Algo           = 1 // parquet SBBF + XXH64
	mbf1HeaderSize     = 32
	mbf1BytesPerBlock  = 32
	mbf1MaxFilterBytes = 128 * 1024 * 1024
)

// bloom_match(field, {blob}) — approximate membership filter. The client builds
// the filter blob (client/sbbf, reproducible cross-language) and passes it as
// a bytes template parameter; the proxy validates the MBF1 envelope and embeds
// it into the plan without rebuilding. See
// docs/design-docs/design_docs/20260707-bloom-filter-expression.md.
const (
	// BloomMatchFunctionName is the CallExpr function name of the bloom filter
	// membership expression.
	BloomMatchFunctionName = "bloom_match"
)

// checkBloomMatchField validates that the probe column is a plain scalar field
// of an integer or string type, or a JSON field (optionally at a nested path,
// including dynamic fields). ARRAY and other types are rejected.
func checkBloomMatchField(columnInfo *planpb.ColumnInfo, argText string) error {
	if columnInfo == nil {
		return merr.WrapErrParameterInvalidMsg(
			"the first argument of bloom_match must be a scalar field name, got: %s", argText)
	}
	dataType := columnInfo.GetDataType()
	// JSON (including dynamic-field paths): the probe is STRICTLY TYPED —
	// only values stored as int64 can match int64 members, only strings can
	// match string members (raw UTF-8). A JSON double never matches, even an
	// integral 5.0 (deliberate divergence from exact `in`, which unifies
	// 5.0 == 5). Missing key / JSON null / bool / double / object / array
	// never match under either polarity. See PhyBloomFilterExpr's JSON path.
	if typeutil.IsJSONType(dataType) {
		return nil
	}
	if len(columnInfo.GetNestedPath()) != 0 {
		return merr.WrapErrParameterInvalidMsg(
			"bloom_match does not support nested paths on non-JSON fields, got: %s", argText)
	}
	// Only INT8/16/32/64 and VARCHAR are supported. Use an exact VARCHAR check
	// (not typeutil.IsStringType, which also accepts STRING/TEXT) so the proxy's
	// accepted type set matches the segcore executor exactly — otherwise a
	// STRING/TEXT field would build successfully here and only be rejected after
	// fan-out at the QueryNode.
	if !typeutil.IsIntegerType(dataType) && dataType != schemapb.DataType_VarChar {
		return merr.WrapErrParameterInvalidMsg(
			"bloom_match only supports INT8/INT16/INT32/INT64/VARCHAR fields and JSON paths, but field (%s) is of type %s",
			argText, dataType.String())
	}
	return nil
}

// validateBloomFilterBlob validates a client pre-built SBBF blob (raw bytes) and
// returns it ready to embed into the plan. The client builds the bit-identical
// MBF1/SBBF blob (client/sbbf, reproducible cross-language) and ships the
// compact ~32 MB blob as a raw bytes template value — no proxy-side build. The
// blob is opaque to the element type, so the caller is responsible for having
// hashed values matching the field type (int64 vs UTF-8); sbbf.Parse validates
// the MBF1 envelope (magic/version/algo/num_blocks/body length) and bounds the
// size to 128 MB, so a malformed or oversized blob is rejected here at the proxy
// rather than fanned out to QueryNodes.
func validateBloomFilterBlob(blob []byte) ([]byte, error) {
	// Per-blob gate. proxy.maxBloomFilterSize budgets the SBBF *body* (default
	// 32 MiB); the fixed 32-byte MBF1 header is allowed on top, hence the
	// `+ mbf1HeaderSize`. Budgeting the body rather than the whole blob matters
	// because the SBBF body is always a power of two: a full 32 MiB body is
	// 32 MiB + 32 B, so a whole-blob cap of exactly 32 MiB would reject it and
	// silently drop the usable ceiling to the next power-of-two-down (a 16 MiB
	// body, ~half the member capacity). The 128 MiB MBF1 num_blocks format cap
	// (checked in validateMBF1Envelope) remains the hard ceiling above this.
	//
	// The proxy separately budgets the assembled request's bloom-bearing plans
	// with proxy.maxBloomFilterPlanSize before proto.Marshal. This per-blob gate
	// remains necessary to reject one oversized filter at its input boundary.
	if maxSize := paramtable.Get().ProxyCfg.MaxBloomFilterSize.GetAsInt(); len(blob) > maxSize+mbf1HeaderSize {
		return nil, merr.WrapErrParameterInvalidMsg(
			"bloom_match filter blob body is %d bytes, exceeding proxy.maxBloomFilterSize (%d)",
			len(blob)-mbf1HeaderSize, maxSize)
	}
	if err := validateMBF1Envelope(blob); err != nil {
		return nil, merr.Wrap(err, "bloom_match filter blob is invalid")
	}
	return blob, nil
}

// validateMBF1Envelope structurally validates the MBF1 blob header
// (magic/version/algo/reserved/num_blocks/body-length) — the same checks the
// client builder and the C++ prober make, replicated here so this package does
// not import the client/v3 module (it is compiled into the plan-parser
// c-shared library). It does not probe the filter; segcore re-validates and
// probes on the data path.
func validateMBF1Envelope(blob []byte) error {
	if len(blob) < mbf1HeaderSize {
		return merr.WrapErrParameterInvalidMsg(
			"bloom filter blob too short: %d bytes, need at least %d", len(blob), mbf1HeaderSize)
	}
	if string(blob[0:4]) != mbf1Magic {
		return merr.WrapErrParameterInvalidMsg(
			"bloom filter blob has invalid magic %q, expected %q", blob[0:4], mbf1Magic)
	}
	if v := binary.LittleEndian.Uint16(blob[4:6]); v != mbf1Version {
		return merr.WrapErrParameterInvalidMsg(
			"unsupported bloom filter version %d, expected %d", v, mbf1Version)
	}
	if a := binary.LittleEndian.Uint16(blob[6:8]); a != mbf1Algo {
		return merr.WrapErrParameterInvalidMsg(
			"unsupported bloom filter algo %d, expected %d", a, mbf1Algo)
	}
	if r := binary.LittleEndian.Uint32(blob[28:32]); r != 0 {
		return merr.WrapErrParameterInvalidMsg(
			"bloom filter reserved field must be 0, got %d", r)
	}
	numBlocks := binary.LittleEndian.Uint32(blob[24:28])
	maxBlocks := uint32(mbf1MaxFilterBytes / mbf1BytesPerBlock)
	if numBlocks == 0 || numBlocks&(numBlocks-1) != 0 || numBlocks > maxBlocks {
		return merr.WrapErrParameterInvalidMsg(
			"bloom filter num_blocks %d is not a power of two in [1, %d]", numBlocks, maxBlocks)
	}
	if bodyLen := uint64(len(blob) - mbf1HeaderSize); bodyLen != uint64(numBlocks)*mbf1BytesPerBlock {
		return merr.WrapErrParameterInvalidMsg(
			"bloom filter body length %d does not match num_blocks %d (want %d bytes)",
			bodyLen, numBlocks, uint64(numBlocks)*mbf1BytesPerBlock)
	}
	return nil
}

// visitBloomMatch handles the bloom_match CallExpr:
//
//	bloom_match(field, {blob})
//
// The second argument must be a template placeholder carrying a client
// pre-built filter blob (raw bytes). A deferred bloom_match CallExpr node is
// emitted with IsTemplate set; FillBloomMatchExpressionValue validates the blob
// once the template value is resolved and rewrites the node into a
// BloomFilterExpr in place. There is no proxy-side build: the FPR is chosen by
// the client at build time and encoded in the blob header.
func (v *ParserVisitor) visitBloomMatch(ctx *parser.CallContext) interface{} {
	allArgs := ctx.AllExpr()
	if len(allArgs) != 2 {
		return merr.WrapErrParameterInvalidMsg(
			"bloom_match requires exactly 2 arguments: bloom_match(field, {blob}), got %d", len(allArgs))
	}

	field := allArgs[0].Accept(v)
	if err := getError(field); err != nil {
		return err
	}
	fieldExpr := getExpr(field)
	if fieldExpr == nil {
		return merr.WrapErrParameterInvalidMsg(
			"the first argument of bloom_match must be a scalar field name, got: %s", allArgs[0].GetText())
	}
	columnInfo := toColumnInfo(fieldExpr)
	if err := checkBloomMatchField(columnInfo, allArgs[0].GetText()); err != nil {
		return err
	}

	values := allArgs[1].Accept(v)
	if err := getError(values); err != nil {
		return err
	}
	valueExpr := getValueExpr(values)
	if valueExpr == nil || !isTemplateExpr(valueExpr) {
		return merr.WrapErrParameterInvalidMsg(
			"the second argument of bloom_match must be a {template} placeholder carrying a client pre-built filter blob, got: %s", allArgs[1].GetText())
	}

	// Deferred: the blob is validated and embedded by
	// FillBloomMatchExpressionValue once the template value is resolved.
	return &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_CallExpr{
				CallExpr: &planpb.CallExpr{
					FunctionName: BloomMatchFunctionName,
					FunctionParameters: []*planpb.Expr{
						fieldExpr.expr,
						{
							Expr:       &planpb.Expr_ValueExpr{ValueExpr: valueExpr},
							IsTemplate: true,
						},
					},
				},
			},
			IsTemplate: true,
		},
		dataType: schemapb.DataType_Bool,
	}
}

// FillBloomMatchExpressionValue resolves the template placeholder of a deferred
// bloom_match call, validates the client pre-built filter blob (raw bytes), and
// rewrites the node into a BloomFilterExpr in place.
func FillBloomMatchExpressionValue(expr *planpb.Expr, call *planpb.CallExpr, templateValues map[string]*planpb.GenericValue) error {
	params := call.GetFunctionParameters()
	if len(params) != 2 {
		return merr.WrapErrQueryPlanMsg("malformed bloom_match call: expected 2 parameters, got %d", len(params))
	}
	columnInfo := params[0].GetColumnExpr().GetInfo()
	templateName := params[1].GetValueExpr().GetTemplateVariableName()

	value, ok := templateValues[templateName]
	if !ok {
		return merr.WrapErrQueryPlanMsg("the value of expression template variable name {%s} is not found", templateName)
	}
	blobVal, ok := value.GetVal().(*planpb.GenericValue_BytesVal)
	if !ok {
		return merr.WrapErrQueryPlanMsg(
			"the value of bloom_match template variable {%s} must be a client pre-built filter blob (bytes)", templateName)
	}
	blob, err := validateBloomFilterBlob(blobVal.BytesVal)
	if err != nil {
		return err
	}
	expr.Expr = &planpb.Expr_BloomFilterExpr{
		BloomFilterExpr: &planpb.BloomFilterExpr{
			ColumnInfo: columnInfo,
			FilterBlob: blob,
		},
	}
	expr.IsTemplate = false
	return nil
}

// hasBloomFilterExpr reports whether the expression tree contains a bloom
// filter membership node — either a materialized BloomFilterExpr or a
// still-deferred bloom_match call.
func hasBloomFilterExpr(expr *planpb.Expr) bool {
	switch e := expr.GetExpr().(type) {
	case *planpb.Expr_BloomFilterExpr:
		return true
	case *planpb.Expr_CallExpr:
		if e.CallExpr.GetFunctionName() == BloomMatchFunctionName {
			return true
		}
		for _, param := range e.CallExpr.GetFunctionParameters() {
			if hasBloomFilterExpr(param) {
				return true
			}
		}
		return false
	case *planpb.Expr_UnaryExpr:
		return hasBloomFilterExpr(e.UnaryExpr.GetChild())
	case *planpb.Expr_BinaryExpr:
		return hasBloomFilterExpr(e.BinaryExpr.GetLeft()) || hasBloomFilterExpr(e.BinaryExpr.GetRight())
	case *planpb.Expr_RandomSampleExpr:
		return hasBloomFilterExpr(e.RandomSampleExpr.GetPredicate())
	case *planpb.Expr_ElementFilterExpr:
		return hasBloomFilterExpr(e.ElementFilterExpr.GetElementExpr()) ||
			hasBloomFilterExpr(e.ElementFilterExpr.GetPredicate())
	case *planpb.Expr_MatchExpr:
		// MATCH_*(struct_array, <predicate>) nests a predicate that may itself
		// contain bloom_match; recurse so the delete-safety guard is not bypassed.
		return hasBloomFilterExpr(e.MatchExpr.GetPredicate())
	default:
		return false
	}
}

// PlanContainsBloomFilter reports whether the plan's main predicate or any
// scorer filter contains a bloom_match expression. bloom_match is approximate
// (false positives) and is therefore rejected by the proxy delete path, where
// a false positive would delete rows outside the user's set. Proxy plan-size
// accounting also uses this function, so scorer filters must be included.
func PlanContainsBloomFilter(plan *planpb.PlanNode) bool {
	if plan == nil {
		return false
	}
	for _, scorer := range plan.GetScorers() {
		if hasBloomFilterExpr(scorer.GetFilter()) {
			return true
		}
	}
	switch realPlan := plan.GetNode().(type) {
	case *planpb.PlanNode_VectorAnns:
		return hasBloomFilterExpr(realPlan.VectorAnns.GetPredicates())
	case *planpb.PlanNode_Predicates:
		return hasBloomFilterExpr(realPlan.Predicates)
	case *planpb.PlanNode_Query:
		return hasBloomFilterExpr(realPlan.Query.GetPredicates())
	}
	return false
}

// collectBloomFilterExprs appends every BloomFilterExpr node in the tree.
// Mirrors the node set of hasBloomFilterExpr.
func collectBloomFilterExprs(expr *planpb.Expr, out *[]*planpb.BloomFilterExpr) {
	if expr == nil {
		return
	}
	switch e := expr.GetExpr().(type) {
	case *planpb.Expr_BloomFilterExpr:
		*out = append(*out, e.BloomFilterExpr)
	case *planpb.Expr_CallExpr:
		for _, param := range e.CallExpr.GetFunctionParameters() {
			collectBloomFilterExprs(param, out)
		}
	case *planpb.Expr_UnaryExpr:
		collectBloomFilterExprs(e.UnaryExpr.GetChild(), out)
	case *planpb.Expr_BinaryExpr:
		collectBloomFilterExprs(e.BinaryExpr.GetLeft(), out)
		collectBloomFilterExprs(e.BinaryExpr.GetRight(), out)
	case *planpb.Expr_RandomSampleExpr:
		collectBloomFilterExprs(e.RandomSampleExpr.GetPredicate(), out)
	case *planpb.Expr_ElementFilterExpr:
		collectBloomFilterExprs(e.ElementFilterExpr.GetElementExpr(), out)
		collectBloomFilterExprs(e.ElementFilterExpr.GetPredicate(), out)
	case *planpb.Expr_MatchExpr:
		collectBloomFilterExprs(e.MatchExpr.GetPredicate(), out)
	}
}

func planPredicates(plan *planpb.PlanNode) *planpb.Expr {
	switch realPlan := plan.GetNode().(type) {
	case *planpb.PlanNode_VectorAnns:
		return realPlan.VectorAnns.GetPredicates()
	case *planpb.PlanNode_Predicates:
		return realPlan.Predicates
	case *planpb.PlanNode_Query:
		return realPlan.Query.GetPredicates()
	}
	return nil
}

// bloomRedactedPlan wraps a plan so its String() elides bloom_match blobs. As a
// fmt.Stringer, mlog.Stringer defers the work: when the log level is disabled,
// String() is never called and there is zero cost.
type bloomRedactedPlan struct{ plan *planpb.PlanNode }

func (p bloomRedactedPlan) String() string {
	if p.plan == nil {
		return "<nil>"
	}
	var blooms []*planpb.BloomFilterExpr
	collectBloomFilterExprs(planPredicates(p.plan), &blooms)
	// Scorer filters carry their own predicate tree and can embed a bloom blob
	// too (function-score / rerank filters); redact those as well.
	for _, sc := range p.plan.GetScorers() {
		collectBloomFilterExprs(sc.GetFilter(), &blooms)
	}
	if len(blooms) == 0 {
		return p.plan.String()
	}
	// Swap each blob for a short {N bytes} marker (a byte-slice pointer
	// assignment — no copy, unlike proto.Clone which would duplicate the
	// up-to-tens-of-MiB body), stringify, then restore the originals via defer.
	// Safe because zap evaluates a Stringer field synchronously in this
	// goroutine at the log call, before the plan is marshaled downstream, so no
	// other reader observes the temporary state.
	saved := make([][]byte, len(blooms))
	for i, bf := range blooms {
		saved[i] = bf.FilterBlob
		bf.FilterBlob = []byte(fmt.Sprintf("<%d bytes elided>", len(saved[i])))
	}
	defer func() {
		for i, bf := range blooms {
			bf.FilterBlob = saved[i]
		}
	}()
	return p.plan.String()
}

// RedactPlanForLog returns a fmt.Stringer that renders the plan with every
// bloom_match filter blob replaced by a {size} marker, so a client's up-to-tens
// -of-MiB blob never lands verbatim in a proxy debug log. Cheap when logging is
// disabled (lazy) and when the plan has no bloom_match (no clone).
func RedactPlanForLog(plan *planpb.PlanNode) fmt.Stringer {
	return bloomRedactedPlan{plan: plan}
}
