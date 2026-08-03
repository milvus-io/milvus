package rewriter

import (
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

type arrayContainsGroup struct {
	columnInfo  *planpb.ColumnInfo
	elements    []*planpb.GenericValue
	firstIndex  int
	sourceCount int
}

// combineArrayContains merges compatible ARRAY contains predicates on the same
// column. OR chains target ContainsAny, while AND chains target ContainsAll.
// JSON columns deliberately remain unchanged.
func combineArrayContains(parts []*planpb.Expr, targetOp planpb.JSONContainsExpr_JSONOp) []*planpb.Expr {
	if len(parts) < 2 || (targetOp != planpb.JSONContainsExpr_ContainsAny && targetOp != planpb.JSONContainsExpr_ContainsAll) {
		return parts
	}

	groups := make(map[string]*arrayContainsGroup)
	memberships := make([]*arrayContainsGroup, len(parts))
	for index, part := range parts {
		contains := part.GetJsonContainsExpr()
		if !canCombineArrayContains(contains, targetOp) {
			continue
		}

		key := columnKey(contains.GetColumnInfo())
		group := groups[key]
		if group == nil {
			group = &arrayContainsGroup{
				columnInfo: contains.GetColumnInfo(),
				firstIndex: index,
			}
			groups[key] = group
		}
		group.elements = append(group.elements, contains.GetElements()...)
		group.sourceCount++
		memberships[index] = group
	}

	out := make([]*planpb.Expr, 0, len(parts))
	for index, part := range parts {
		group := memberships[index]
		if group == nil || group.sourceCount < 2 {
			out = append(out, part)
			continue
		}
		if index != group.firstIndex {
			continue
		}

		out = append(out, &planpb.Expr{
			Expr: &planpb.Expr_JsonContainsExpr{
				JsonContainsExpr: &planpb.JSONContainsExpr{
					ColumnInfo:       group.columnInfo,
					Elements:         group.elements,
					Op:               targetOp,
					ElementsSameType: arrayContainsElementsSameType(group.elements),
				},
			},
		})
	}
	return out
}

func canCombineArrayContains(expr *planpb.JSONContainsExpr, targetOp planpb.JSONContainsExpr_JSONOp) bool {
	if expr == nil || expr.GetColumnInfo() == nil || expr.GetColumnInfo().GetDataType() != schemapb.DataType_Array {
		return false
	}

	sourceOp := expr.GetOp()
	if sourceOp != planpb.JSONContainsExpr_Contains && sourceOp != targetOp {
		return false
	}
	if sourceOp == planpb.JSONContainsExpr_Contains && len(expr.GetElements()) != 1 {
		return false
	}

	for _, element := range expr.GetElements() {
		if _, ok := arrayContainsValueType(element); !ok {
			return false
		}
	}
	return true
}

func arrayContainsElementsSameType(elements []*planpb.GenericValue) bool {
	if len(elements) == 0 {
		return true
	}

	elementType, ok := arrayContainsValueType(elements[0])
	if !ok {
		return false
	}
	for _, element := range elements[1:] {
		currentType, ok := arrayContainsValueType(element)
		if !ok || currentType != elementType {
			return false
		}
	}
	return true
}

func arrayContainsValueType(value *planpb.GenericValue) (schemapb.DataType, bool) {
	if value == nil {
		return schemapb.DataType_None, false
	}

	switch typedValue := value.GetVal().(type) {
	case *planpb.GenericValue_BoolVal:
		return schemapb.DataType_Bool, typedValue != nil
	case *planpb.GenericValue_Int64Val:
		return schemapb.DataType_Int64, typedValue != nil
	case *planpb.GenericValue_FloatVal:
		return schemapb.DataType_Double, typedValue != nil && !math.IsNaN(typedValue.FloatVal)
	case *planpb.GenericValue_StringVal:
		return schemapb.DataType_VarChar, typedValue != nil
	default:
		return schemapb.DataType_None, false
	}
}
