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
	"fmt"
	"strconv"
	"strings"

	"github.com/antlr4-go/antlr/v4"

	planparserv2 "github.com/milvus-io/milvus/internal/parser/planparserv2/generated"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// SelectItemKind is the syntactic shape of one output_fields element.
type SelectItemKind int

const (
	// SelectItemStar is "*".
	SelectItemStar SelectItemKind = iota
	// SelectItemField is a field reference: a field name, "$meta", a JSON path,
	// or a struct sub-field form. The name is not resolved against a schema.
	SelectItemField
	// SelectItemAggregate is count/sum/avg/min/max over a field name, "*", or
	// an empty argument list.
	SelectItemAggregate
	// SelectItemVirtualColumn is one of the predefined "$" columns.
	SelectItemVirtualColumn
)

// Virtual column names accepted in output_fields.
const (
	VirtualColumnScore     = "$score"
	VirtualColumnDistance  = "$distance"
	VirtualColumnRank      = "$rank"
	VirtualColumnSegID     = "$seg_id"
	VirtualColumnSegOffset = "$seg_offset"
)

// VirtualColumnArgKind tells which argument form a virtual column carries.
type VirtualColumnArgKind int

const (
	// VirtualColumnNoArg is the bare form, such as "$score".
	VirtualColumnNoArg VirtualColumnArgKind = iota
	// VirtualColumnArgField is "$score(field_name)".
	VirtualColumnArgField
	// VirtualColumnArgIndex is "$score(0)": the position of a sub-request.
	VirtualColumnArgIndex
)

// SelectItem is the parsed form of one output_fields element. It carries
// syntax only; name resolution against the schema is the caller's job.
type SelectItem struct {
	Kind SelectItemKind
	// Source is the item text without the "AS alias" clause and without
	// surrounding whitespace. It is the default output name of the item.
	Source string
	// Alias is the name after AS, or empty.
	Alias string

	// AggregateOp is the lower-cased function name; AggregateArg is the field
	// name, "*", or empty. Both are set only for SelectItemAggregate.
	AggregateOp  string
	AggregateArg string

	// VirtualColumn is one of the VirtualColumn* names. The Arg fields are
	// set according to VirtualArgKind. Set only for SelectItemVirtualColumn.
	VirtualColumn   string
	VirtualArgKind  VirtualColumnArgKind
	VirtualArgField string
	VirtualArgIndex int64
}

// selectItemCacheKeyPrefix separates output_fields entries from filter
// expressions in the shared parse cache: the same text parses differently
// under the two entry rules. A filter cannot start with NUL and parse.
const selectItemCacheKeyPrefix = "\x00selectItem\x00"

const unsupportedSelectExprMsg = "expressions in output fields are not supported yet"

func isAggregateFunctionName(lowered string) bool {
	switch lowered {
	case "count", "sum", "avg", "min", "max":
		return true
	default:
		return false
	}
}

// SelectItemSyntaxError reports that an output_fields element does not match
// the selectItem rule. Callers word the message for their API; the proxy
// keeps the texts it returned for unparsable names before the rule existed.
type SelectItemSyntaxError struct {
	Item   string
	Detail string
}

func (e *SelectItemSyntaxError) Error() string {
	return fmt.Sprintf("invalid output field %q: %s", e.Item, e.Detail)
}

// ParseSelectItem parses one output_fields element with the selectItem rule.
func ParseSelectItem(item string) (SelectItem, error) {
	c := exprCache()
	key := selectItemCacheKeyPrefix + item
	if val, ok := c.get(key); ok {
		switch v := val.(type) {
		case SelectItem:
			return v, nil
		case error:
			return SelectItem{}, v
		}
	}
	parsed, err := parseSelectItemUncached(item)
	if err != nil {
		c.add(key, err, estimateErrorCacheBytes(key, err))
	} else {
		// A cached SelectItem holds a few substrings of the item text and no
		// parse tree, so the key and one copy of the text bound its size.
		c.add(key, parsed, int64(len(key)+len(item))+exprCacheEntryBaseBytes)
	}
	return parsed, err
}

func parseSelectItemUncached(item string) (SelectItem, error) {
	normalized := convertHanToASCII(item)
	listener := &errorListenerImpl{}
	lexer := getLexer(antlr.NewInputStream(normalized), listener)
	parser := getParser(lexer, listener)

	ast := parseTwoStage(parser, listener, parser.SelectItem)
	if err := listener.Error(); err != nil {
		return SelectItem{}, &SelectItemSyntaxError{Item: item, Detail: err.Error()}
	}
	if parser.GetCurrentToken().GetTokenType() != antlr.TokenEOF {
		return SelectItem{}, &SelectItemSyntaxError{Item: item, Detail: "unexpected input after the item"}
	}
	putLexer(lexer)
	putParser(parser)

	runes := []rune(normalized)
	sourceOf := func(ctx antlr.ParserRuleContext) string {
		return string(runes[ctx.GetStart().GetStart() : ctx.GetStop().GetStop()+1])
	}

	switch ctx := ast.(type) {
	case *planparserv2.SelectStarContext:
		return SelectItem{Kind: SelectItemStar, Source: "*"}, nil

	case *planparserv2.SelectAggregateContext:
		call := ctx.AggregateCall().(*planparserv2.AggregateCallContext)
		op := strings.ToLower(call.GetFunction().GetText())
		if !isAggregateFunctionName(op) {
			return SelectItem{}, merr.WrapErrParameterInvalidMsg("invalid output field %q: %s", item, unsupportedSelectExprMsg)
		}
		result := SelectItem{Kind: SelectItemAggregate, Source: sourceOf(call), AggregateOp: op}
		if call.GetArg() != nil {
			result.AggregateArg = call.GetArg().GetText()
		} else if call.GetStar() != nil {
			result.AggregateArg = "*"
		}
		if ctx.GetAlias() != nil {
			result.Alias = ctx.GetAlias().GetText()
		}
		return result, nil

	case *planparserv2.SelectExprContext:
		result := SelectItem{Source: sourceOf(ctx.Expr())}
		if ctx.GetAlias() != nil {
			result.Alias = ctx.GetAlias().GetText()
		}
		switch e := ctx.Expr().(type) {
		case *planparserv2.IdentifierContext, *planparserv2.JSONIdentifierContext,
			*planparserv2.StructFieldContext, *planparserv2.StructIndexFieldContext:
			result.Kind = SelectItemField
		case *planparserv2.VirtualColumnContext:
			result.Kind = SelectItemVirtualColumn
			if err := fillVirtualColumn(&result, e); err != nil {
				return SelectItem{}, merr.WrapErrParameterInvalidMsg("invalid output field %q: %s", item, err.Error())
			}
		default:
			return SelectItem{}, merr.WrapErrParameterInvalidMsg("invalid output field %q: %s", item, unsupportedSelectExprMsg)
		}
		return result, nil
	}
	return SelectItem{}, merr.WrapErrParameterInvalidMsg("invalid output field %q", item)
}

type selectItemSyntaxError string

func (e selectItemSyntaxError) Error() string { return string(e) }

func fillVirtualColumn(result *SelectItem, ctx *planparserv2.VirtualColumnContext) error {
	name := ctx.GetName().GetText()
	result.VirtualColumn = name
	switch {
	case ctx.GetArgField() != nil:
		result.VirtualArgKind = VirtualColumnArgField
		result.VirtualArgField = ctx.GetArgField().GetText()
	case ctx.GetArgIndex() != nil:
		text := ctx.GetArgIndex().GetText()
		// Decimal only: "010" and "0x1" are not sub-request positions.
		if len(text) > 1 && text[0] == '0' {
			return selectItemSyntaxError(name + " takes a field name or a decimal position, got " + text)
		}
		index, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return selectItemSyntaxError(name + " takes a field name or a decimal position, got " + text)
		}
		result.VirtualArgKind = VirtualColumnArgIndex
		result.VirtualArgIndex = index
	}
	switch name {
	case VirtualColumnSegID, VirtualColumnSegOffset:
		if result.VirtualArgKind != VirtualColumnNoArg {
			return selectItemSyntaxError(name + " does not take an argument")
		}
	case VirtualColumnRank:
		if result.VirtualArgKind == VirtualColumnNoArg {
			return selectItemSyntaxError(name + " requires a sub-request argument: a field name or a position")
		}
	}
	return nil
}
