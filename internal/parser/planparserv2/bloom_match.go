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
	"math"
	"math/bits"

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

	// Value domains recorded in the envelope's `domains` byte (offset 28),
	// mirroring sbbf.DomainInt64 / sbbf.DomainUTF8. The two hash domains share
	// one XXH64 output space, so the blob must declare which of them it was
	// built from; zero means an empty filter that records no domain.
	mbf1DomainInt64 = 1 << 0
	mbf1DomainUTF8  = 1 << 1
	mbf1DomainKnown = mbf1DomainInt64 | mbf1DomainUTF8

	// Accepted false-positive rate range, mirroring sbbf.MinFPR / sbbf.MaxFPR.
	// Used only to bound the fpr suggested in the over-size error.
	mbf1MinFPR = 0.0001
	mbf1MaxFPR = 0.05
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
	if !typeutil.IsIntegerType(dataType) && dataType != schemapb.DataType_VarChar && !typeutil.IsUUIDType(dataType) {
		return merr.WrapErrParameterInvalidMsg(
			"bloom_match only supports INT8/INT16/INT32/INT64/VARCHAR/UUID fields and JSON paths, but field (%s) is of type %s",
			argText, dataType.String())
	}
	return nil
}

// validateBloomFilterBlob validates a client pre-built SBBF blob (raw bytes) and
// returns it ready to embed into the plan. The client builds the bit-identical
// MBF1/SBBF blob (client/sbbf, reproducible cross-language) and ships the
// compact blob as a raw bytes template value — no proxy-side build. The
// blob declares the value domains it was built from, which
// checkBloomFilterValueDomain matches against the target field; this validation
// covers the envelope itself (magic/version/algo/domains/num_blocks/body length)
// and bounds the size to 128 MB, so a malformed, oversized or wrong-domain blob
// is rejected here at the proxy rather than fanned out to QueryNodes.
func validateBloomFilterBlob(blob []byte) ([]byte, error) {
	// Per-blob gate. proxy.maxMembershipFilterSize budgets the SBBF *body* (default
	// 64 MiB); the fixed 32-byte MBF1 header is allowed on top, hence the
	// `+ mbf1HeaderSize`. Budgeting the body rather than the whole blob matters
	// because the SBBF body is always a power of two: a full 64 MiB body is
	// 64 MiB + 32 B, so a whole-blob cap of exactly 64 MiB would reject it and
	// silently drop the usable ceiling to the next power-of-two-down (a 32 MiB
	// body, ~half the member capacity). The 128 MiB MBF1 num_blocks format cap
	// (checked in validateMBF1Envelope) remains the hard ceiling above this.
	//
	// The proxy separately budgets the assembled request's membership-filter-bearing plans
	// with proxy.maxMembershipFilterPlanSize before proto.Marshal. This per-blob gate
	// remains necessary to reject one oversized filter at its input boundary.
	if maxSize := paramtable.Get().ProxyCfg.MaxMembershipFilterSize.GetAsInt(); len(blob) > maxSize+mbf1HeaderSize {
		return nil, merr.WrapErrParameterInvalidMsg(
			"bloom_match filter blob body is %d bytes, exceeding proxy.maxMembershipFilterSize (%d)%s",
			len(blob)-mbf1HeaderSize, maxSize, oversizedBlobHint(blob, maxSize))
	}
	if err := validateMBF1Envelope(blob); err != nil {
		return nil, merr.Wrap(err, "bloom_match filter blob is invalid")
	}
	return blob, nil
}

// oversizedBlobHint turns "your filter is too big" into something the caller
// can act on. Without it the only remedy visible from the error is "send fewer
// members", when the usual fix is a higher fpr: SBBF bodies are powers of two,
// so a member count just past a boundary doubles the blob, and one step of fpr
// brings it back under the cap.
//
// It returns "" when no advice is possible. The blob is not yet validated here,
// so n_declared is read defensively and treated as a hint only — the caller's
// error is already decided and cannot be made wrong by a bogus value.
func oversizedBlobHint(blob []byte, maxSize int) string {
	if len(blob) < mbf1HeaderSize || maxSize < mbf1BytesPerBlock {
		return ""
	}
	n := binary.LittleEndian.Uint64(blob[8:16])
	if n == 0 || n > math.MaxInt64 {
		return ""
	}
	// Only powers of two are reachable body sizes, so the usable budget is the
	// largest power of two that fits under the cap, not the cap itself.
	usable := uint64(1) << (bits.Len64(uint64(maxSize)) - 1)

	// Invert the SBBF sizing formula m = -8n/ln(1-fpp^(1/8)) at m = usable*8
	// bits: fpp = (1 - e^(-n/usable))^8. Ceil to four decimals so the suggested
	// value is never a rounding step below what actually fits — a larger fpr
	// only ever yields a smaller filter.
	fpr := math.Pow(1-math.Exp(-float64(n)/float64(usable)), 8)
	fpr = math.Ceil(fpr*10000) / 10000
	if fpr < mbf1MinFPR {
		fpr = mbf1MinFPR
	}
	if fpr > mbf1MaxFPR {
		return fmt.Sprintf("; the declared %d members do not fit %d bytes even at the maximum fpr %g — "+
			"reduce the member count or raise proxy.maxMembershipFilterSize", n, usable, mbf1MaxFPR)
	}
	return fmt.Sprintf("; rebuild the filter with fpr >= %g for the declared %d members "+
		"(SBBF bodies are powers of two, so a smaller fpr jumps straight to the next size)", fpr, n)
}

// validateMBF1Envelope structurally validates the MBF1 blob header
// (magic/version/algo/domains/reserved/num_blocks/body-length) — the same checks the
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
	if d := blob[28]; d&^mbf1DomainKnown != 0 {
		return merr.WrapErrParameterInvalidMsg(
			"bloom filter declares unknown value domains 0x%02x, known bits 0x%02x", d, mbf1DomainKnown)
	}
	if r := blob[29] | blob[30] | blob[31]; r != 0 {
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

// checkBloomFilterValueDomain rejects a blob built from a value domain the
// target field can never probe. The two hash domains share one XXH64 output
// space, so the prober refuses to probe a domain the blob does not declare —
// which means a wrong-domain blob would otherwise execute happily and just
// return fewer rows, with no error anywhere. That silent recall loss is the
// failure this check converts into an input error at the request boundary.
//
// Two shapes are deliberately NOT rejected:
//   - JSON paths, whose value type is per row, not per field: a domain the blob
//     lacks simply never matches, which is the correct answer;
//   - a blob declaring no domain at all (an empty membership set), which is a
//     legal filter that matches nothing.
func checkBloomFilterValueDomain(columnInfo *planpb.ColumnInfo, blob []byte) error {
	dataType := columnInfo.GetDataType()
	if typeutil.IsJSONType(dataType) {
		return nil
	}
	domains := blob[28]
	if domains == 0 {
		return nil
	}
	want, wantName := mbf1DomainInt64, "int64"
	if dataType == schemapb.DataType_VarChar || typeutil.IsUUIDType(dataType) {
		want, wantName = mbf1DomainUTF8, "utf8"
	}
	if domains&byte(want) == 0 {
		return merr.WrapErrParameterInvalidMsg(
			"bloom_match filter blob was built from the %s value domain but field (%s) requires the %s domain; "+
				"rebuild the filter from values of the field's type",
			bloomDomainNames(domains), dataType.String(), wantName)
	}
	return nil
}

// bloomDomainNames renders a domains bitmask for error messages.
func bloomDomainNames(domains byte) string {
	switch domains {
	case mbf1DomainInt64:
		return "int64"
	case mbf1DomainUTF8:
		return "utf8"
	default:
		return fmt.Sprintf("0x%02x", domains)
	}
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
		return merr.WrapErrParameterInvalidMsg(
			"the second argument of bloom_match must be a {template} placeholder carrying a client pre-built filter blob")
	}
	valueExpr := getValueExpr(values)
	if valueExpr == nil || !isTemplateExpr(valueExpr) {
		return merr.WrapErrParameterInvalidMsg(
			"the second argument of bloom_match must be a {template} placeholder carrying a client pre-built filter blob")
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
	// Envelope is structurally valid here, so blob[28] is safe to read.
	if err := checkBloomFilterValueDomain(columnInfo, blob); err != nil {
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

// walkExpr visits every expression node until visit returns true. Keeping the
// recursion in one place prevents blob accounting, delete safety, element-level
// guards, and log redaction from drifting as new container nodes are added.
func walkExpr(expr *planpb.Expr, visit func(*planpb.Expr) bool) bool {
	if expr == nil {
		return false
	}
	if visit(expr) {
		return true
	}
	switch e := expr.GetExpr().(type) {
	case *planpb.Expr_CallExpr:
		for _, param := range e.CallExpr.GetFunctionParameters() {
			if walkExpr(param, visit) {
				return true
			}
		}
	case *planpb.Expr_UnaryExpr:
		return walkExpr(e.UnaryExpr.GetChild(), visit)
	case *planpb.Expr_BinaryExpr:
		return walkExpr(e.BinaryExpr.GetLeft(), visit) || walkExpr(e.BinaryExpr.GetRight(), visit)
	case *planpb.Expr_BinaryArithExpr:
		return walkExpr(e.BinaryArithExpr.GetLeft(), visit) || walkExpr(e.BinaryArithExpr.GetRight(), visit)
	case *planpb.Expr_RandomSampleExpr:
		return walkExpr(e.RandomSampleExpr.GetPredicate(), visit)
	case *planpb.Expr_ElementFilterExpr:
		return walkExpr(e.ElementFilterExpr.GetElementExpr(), visit) ||
			walkExpr(e.ElementFilterExpr.GetPredicate(), visit)
	case *planpb.Expr_MatchExpr:
		return walkExpr(e.MatchExpr.GetPredicate(), visit)
	}
	return false
}

// hasBloomFilterExpr reports whether the expression tree contains a bloom
// filter membership node — either a materialized BloomFilterExpr or a
// still-deferred bloom_match call.
func hasBloomFilterExpr(expr *planpb.Expr) bool {
	return walkExpr(expr, func(node *planpb.Expr) bool {
		switch e := node.GetExpr().(type) {
		case *planpb.Expr_BloomFilterExpr:
			return true
		case *planpb.Expr_CallExpr:
			return e.CallExpr.GetFunctionName() == BloomMatchFunctionName
		default:
			return false
		}
	})
}

// PlanContainsMembershipFilter reports whether a main plan predicate or scorer
// filter carries either supported client-built membership encoding.
func PlanContainsMembershipFilter(plan *planpb.PlanNode) bool {
	return PlanContainsBloomFilter(plan) || PlanContainsRoaringFilter(plan)
}

// planContainsFilter reports whether the plan's main predicate or any scorer
// filter satisfies has. Scorer filters carry their own predicate tree and can
// embed a membership blob too, so both plan-size accounting and delete safety
// must see them. Keeping the plan traversal here means a new PlanNode variant
// is picked up by every membership predicate at once.
//
// A nil plan needs no special case: the generated getters are nil-safe, so it
// yields no scorers and a nil predicate, and walkExpr reports false for nil.
func planContainsFilter(plan *planpb.PlanNode, has func(*planpb.Expr) bool) bool {
	for _, scorer := range plan.GetScorers() {
		if has(scorer.GetFilter()) {
			return true
		}
	}
	return has(planPredicates(plan))
}

// PlanContainsBloomFilter reports whether the plan's main predicate or a scorer
// filter contains a bloom_match expression. bloom_match is approximate (false
// positives) and is therefore also rejected by the proxy delete path, where a
// false positive would delete rows outside the user's set.
func PlanContainsBloomFilter(plan *planpb.PlanNode) bool {
	return planContainsFilter(plan, hasBloomFilterExpr)
}

// collectBloomFilterExprs appends every BloomFilterExpr node in the tree.
// Mirrors the node set of hasBloomFilterExpr.
func collectBloomFilterExprs(expr *planpb.Expr, out *[]*planpb.BloomFilterExpr) {
	walkExpr(expr, func(node *planpb.Expr) bool {
		if e, ok := node.GetExpr().(*planpb.Expr_BloomFilterExpr); ok {
			*out = append(*out, e.BloomFilterExpr)
		}
		return false
	})
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

// membershipRedactedPlan wraps a plan so its String() elides both approximate
// bloom_match blobs and exact roaring_match blobs. As a fmt.Stringer,
// mlog.Stringer defers the work: when the log level is disabled, String() is
// never called and there is zero cost.
type membershipRedactedPlan struct{ plan *planpb.PlanNode }

func (p membershipRedactedPlan) String() string {
	if p.plan == nil {
		return "<nil>"
	}

	var blooms []*planpb.BloomFilterExpr
	var roarings []*planpb.RoaringFilterExpr
	collectBloomFilterExprs(planPredicates(p.plan), &blooms)
	collectRoaringFilterExprs(planPredicates(p.plan), &roarings)
	for _, scorer := range p.plan.GetScorers() {
		collectBloomFilterExprs(scorer.GetFilter(), &blooms)
		collectRoaringFilterExprs(scorer.GetFilter(), &roarings)
	}
	if len(blooms) == 0 && len(roarings) == 0 {
		return p.plan.String()
	}
	// Swap each blob for one short marker (a byte-slice pointer assignment -- no
	// copy, unlike proto.Clone which would duplicate the up-to-tens-of-MiB body),
	// stringify, then restore the originals via defer.
	// Safe because zap evaluates a Stringer field synchronously in this
	// goroutine at the log call, and these call sites own the task-local plan, so
	// no other reader observes the temporary state.
	blobs := make([]*[]byte, 0, len(blooms)+len(roarings))
	for _, bf := range blooms {
		blobs = append(blobs, &bf.FilterBlob)
	}
	for _, rf := range roarings {
		blobs = append(blobs, &rf.BitmapBlob)
	}
	saved := make([][]byte, len(blobs))
	for i, blob := range blobs {
		saved[i] = *blob
		*blob = []byte("<blob>")
	}
	defer func() {
		for i, blob := range blobs {
			*blob = saved[i]
		}
	}()
	return p.plan.String()
}

// RedactPlanForLog returns a fmt.Stringer that renders the plan with every
// bloom_match or roaring_match blob replaced by <blob>, so a client's
// up-to-tens-of-MiB membership payload never lands verbatim in a proxy debug
// log. Cheap when logging is disabled (lazy) and when the plan has no
// membership blob (no clone).
func RedactPlanForLog(plan *planpb.PlanNode) fmt.Stringer {
	return membershipRedactedPlan{plan: plan}
}
