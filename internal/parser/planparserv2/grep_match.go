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
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	parser "github.com/milvus-io/milvus/internal/parser/planparserv2/generated"
	"github.com/milvus-io/milvus/internal/util/function/grep"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// GrepFunctionName is the filter predicate grep(field, "rg-style arguments").
// Like membership_match it is a soft keyword: the name stays an ordinary
// identifier everywhere except in this call shape.
const GrepFunctionName = "grep"

// GrepMatchesFunctionName is the output_fields item
// GREP_MATCHES(field, "rg-style arguments"): the matching and context lines
// of each returned row. The name is matched in any casing and is not reserved.
const GrepMatchesFunctionName = "grep_matches"

// visitGrepCall compiles grep(field, arguments) into a GrepExpr. The argument
// string is parsed here so syntax errors and unsupported options are reported
// by the proxy; regex compilation stays on the execution node.
func (v *ParserVisitor) visitGrepCall(ctx *parser.CallContext) interface{} {
	args := ctx.AllExpr()
	if len(args) != 2 {
		return merr.WrapErrParameterInvalidMsg(
			"%s requires exactly 2 arguments: a VarChar or Text field and an argument string", GrepFunctionName)
	}

	fieldText := args[0].GetText()
	if _, ok := args[0].(*parser.IdentifierContext); !ok {
		return merr.WrapErrParameterInvalidMsg(
			"the first argument of %s must be a VarChar or Text field name, got: %s", GrepFunctionName, fieldText)
	}
	// Text fields are rejected by ordinary filter operands; grep reads them
	// through its own segment reader.
	fieldExpr, err := v.translateIdentifierWithText(fieldText, true)
	if err != nil {
		return err
	}
	columnInfo := toColumnInfo(fieldExpr)
	if columnInfo == nil || len(columnInfo.GetNestedPath()) != 0 ||
		(columnInfo.GetDataType() != schemapb.DataType_VarChar && columnInfo.GetDataType() != schemapb.DataType_Text) {
		return merr.WrapErrParameterInvalidMsg(
			"the first argument of %s must be a schema VarChar or Text field, got: %s", GrepFunctionName, fieldText)
	}

	arguments, _, isTemplate, err := v.parseStringLiteralOrTemplate(args[1], "the second argument of grep")
	if err != nil {
		return err
	}
	if isTemplate {
		return merr.WrapErrParameterInvalidMsg(
			"the second argument of %s must be a string literal; template variables are not supported", GrepFunctionName)
	}

	spec, err := grep.Parse(arguments, paramtable.Get().FunctionCfg.GrepMaxArgumentBytes.GetAsInt64())
	if err != nil {
		return merr.Wrap(err, "parse grep arguments")
	}
	// Context and a positive matching-line cap shape the returned details; they
	// cannot change which rows match, so they have no meaning in a predicate.
	if spec.BeforeContext() != 0 || spec.AfterContext() != 0 {
		return merr.WrapErrParameterInvalidMsg(
			"context options (-A/-B/-C) are not supported in the %s predicate", GrepFunctionName)
	}
	maxCount, hasMaxCount := spec.MaxCount()
	if hasMaxCount && maxCount != 0 {
		return merr.WrapErrParameterInvalidMsg(
			"a positive -m/--max-count is not supported in the %s predicate", GrepFunctionName)
	}

	return &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_GrepExpr{GrepExpr: grepExprFromSpec(columnInfo, spec)},
		},
		dataType: schemapb.DataType_Bool,
	}
}

func grepExprFromSpec(columnInfo *planpb.ColumnInfo, spec *grep.Spec) *planpb.GrepExpr {
	patterns := make([]string, 0, spec.PatternCount())
	for i := 0; i < spec.PatternCount(); i++ {
		patterns = append(patterns, spec.Pattern(i))
	}
	maxCount, hasMaxCount := spec.MaxCount()
	return &planpb.GrepExpr{
		ColumnInfo:  columnInfo,
		Patterns:    patterns,
		PatternKind: uint32(spec.PatternKind()),
		CaseMode:    uint32(spec.CaseMode()),
		Boundary:    uint32(spec.Boundary()),
		SelectNone:  hasMaxCount && maxCount == 0,
	}
}

// NewGrepMatches builds the plan form of a GREP_MATCHES(field, arguments)
// output item. Unlike the predicate it accepts the options that only shape
// the returned lines: -m and -A/-B/-C.
func NewGrepMatches(field *schemapb.FieldSchema, arguments string) (*planpb.GrepMatches, error) {
	if field.GetDataType() != schemapb.DataType_VarChar && field.GetDataType() != schemapb.DataType_Text {
		return nil, merr.WrapErrParameterInvalidMsg(
			"the first argument of %s must be a schema VarChar or Text field, got: %s", GrepMatchesFunctionName, field.GetName())
	}
	spec, err := grep.Parse(arguments, paramtable.Get().FunctionCfg.GrepMaxArgumentBytes.GetAsInt64())
	if err != nil {
		return nil, merr.Wrap(err, "parse grep arguments")
	}
	columnInfo := &planpb.ColumnInfo{
		FieldId:        field.GetFieldID(),
		DataType:       field.GetDataType(),
		IsPartitionKey: field.GetIsPartitionKey(),
		Nullable:       field.GetNullable(),
	}
	maxCount, hasMaxCount := spec.MaxCount()
	return &planpb.GrepMatches{
		Spec:          grepExprFromSpec(columnInfo, spec),
		HasMaxCount:   hasMaxCount && maxCount != 0,
		MaxCount:      maxCount,
		BeforeContext: spec.BeforeContext(),
		AfterContext:  spec.AfterContext(),
	}, nil
}

// AndGrepPredicate returns `predicate AND grep(spec)`. A nil or always-true
// predicate yields the grep expression alone. element_filter and random_sample stay the
// top-level expression, as segcore requires: grep joins their document-level
// predicate.
func AndGrepPredicate(predicate *planpb.Expr, spec *planpb.GrepExpr) *planpb.Expr {
	grepExpr := &planpb.Expr{Expr: &planpb.Expr_GrepExpr{GrepExpr: spec}}
	and := func(left *planpb.Expr) *planpb.Expr {
		// An empty filter parses to AlwaysTrue; grep replaces it.
		if left == nil || left.GetAlwaysTrueExpr() != nil {
			return grepExpr
		}
		return &planpb.Expr{Expr: &planpb.Expr_BinaryExpr{BinaryExpr: &planpb.BinaryExpr{
			Op: planpb.BinaryExpr_LogicalAnd, Left: left, Right: grepExpr,
		}}}
	}
	switch root := predicate.GetExpr().(type) {
	case *planpb.Expr_ElementFilterExpr:
		root.ElementFilterExpr.Predicate = and(root.ElementFilterExpr.GetPredicate())
		return predicate
	case *planpb.Expr_RandomSampleExpr:
		root.RandomSampleExpr.Predicate = and(root.RandomSampleExpr.GetPredicate())
		return predicate
	}
	return and(predicate)
}
