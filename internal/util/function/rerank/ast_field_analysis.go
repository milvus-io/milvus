package rerank

import (
	"fmt"

	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/parser"
)

type fieldVisitor struct {
	fields map[string]bool
}

func (v *fieldVisitor) Visit(node *ast.Node) {
	// Look for MemberNode accessing "fields" map
	if member, ok := (*node).(*ast.MemberNode); ok {
		if ident, ok := member.Node.(*ast.IdentifierNode); ok {
			if ident.Value == "fields" {
				if propertyIdent, ok := member.Property.(*ast.IdentifierNode); ok {
					v.fields[propertyIdent.Value] = true
				} else if propertyStr, ok := member.Property.(*ast.StringNode); ok {
					v.fields[propertyStr.Value] = true
				}
			}
		}
	}
	// handle slice/map index access: fields[expr]
	if sliceNode, ok := (*node).(*ast.SliceNode); ok {
		if ident, ok := sliceNode.Node.(*ast.IdentifierNode); ok {
			if ident.Value == "fields" {
				if str, ok := sliceNode.To.(*ast.StringNode); ok {
					v.fields[str.Value] = true
				}
			}
		}
	}
}

// Extracts field names referenced in the expression using AST analysis
func analyzeRequiredFields(exprString string, allAvailableFields []string) ([]string, map[string]int, error) {
	tree, err := parser.Parse(exprString)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse expression for field analysis: %w", err)
	}
	visitor := &fieldVisitor{fields: make(map[string]bool)}
	ast.Walk(&tree.Node, visitor)
	requiredFields := []string{}
	fieldIndices := make(map[string]int)
	for i, fieldName := range allAvailableFields {
		if visitor.fields[fieldName] {
			requiredFields = append(requiredFields, fieldName)
			fieldIndices[fieldName] = i
		}
	}
	return requiredFields, fieldIndices, nil
}
