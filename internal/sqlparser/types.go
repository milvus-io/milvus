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

package sqlparser

import pg_query "github.com/pganalyze/pg_query_go/v6"

// QueryType indicates what kind of Milvus operation the SQL maps to.
type QueryType int

const (
	QueryTypeQuery  QueryType = iota // Scalar query / aggregation -> QueryPlanNode
	QueryTypeSearch                  // Vector search -> VectorANNS
	QueryTypeHybrid                  // Vector search + scalar filter
)

// SqlComponents is the structured intermediate representation (IR)
// bridging the PG parser AST to Milvus plan generation.
type SqlComponents struct {
	Collection   string           // FROM clause -> collection name
	SelectItems  []SelectItem     // SELECT list
	Where        *WhereClause     // WHERE clause (converted to Milvus expr)
	GroupBy      []GroupByItem    // GROUP BY keys
	OrderBy      []OrderItem      // ORDER BY list
	Limit        int64            // LIMIT value, -1 means no limit
	VectorSearch *VectorSearchDef // Vector search definition (nil if none)
	QueryType    QueryType        // Determined from SELECT/ORDER BY analysis
}

// SelectItemType distinguishes different kinds of SELECT list elements.
type SelectItemType int

const (
	SelectColumn     SelectItemType = iota // Plain column reference
	SelectAgg                              // Aggregate function call (count, sum, min, max, avg)
	SelectComputed                         // Computed expression (extract, to_timestamp, etc.)
	SelectVectorDist                       // Vector distance expression
	SelectStar                             // SELECT * (may be unsupported)
)

// SelectItem represents a single item in the SELECT list.
type SelectItem struct {
	Type     SelectItemType
	Column   *ColumnRef      // Non-nil when Type == SelectColumn
	AggFunc  *AggFuncCall    // Non-nil when Type == SelectAgg
	Computed *FunctionCall   // Non-nil when Type == SelectComputed
	Distance *VectorDistExpr // Non-nil when Type == SelectVectorDist
	Alias    string          // AS alias (empty if none)
	RawExpr  *pg_query.Node  // Original AST expression (for fallback/debugging)
}

// ColumnRef represents a column reference, supporting PG JSON operators.
//
// Example: data->'commit'->>'collection'
//
//	FieldName  = "data"
//	NestedPath = ["commit", "collection"]
//	IsText     = true (last op was ->>)
type ColumnRef struct {
	FieldName  string   // Top-level field name, e.g. "data"
	NestedPath []string // JSON nested path from -> / ->> operators
	IsText     bool     // true if last operator was ->> (text extraction)
	CastType   string   // Optional type cast from ::type syntax, e.g. "text", "bigint"
}

// MilvusExpr converts this ColumnRef to a Milvus JSON field expression.
// e.g. data->'commit'->>'collection' -> data["commit"]["collection"]
func (c *ColumnRef) MilvusExpr() string {
	if len(c.NestedPath) == 0 {
		return c.FieldName
	}
	expr := c.FieldName
	for _, p := range c.NestedPath {
		expr += `["` + p + `"]`
	}
	return expr
}

// AggFuncCall represents an aggregate function call.
type AggFuncCall struct {
	FuncName string     // count, sum, min, max, avg
	Arg      *ColumnRef // Argument column; nil for count(*)
	Distinct bool       // true for count(DISTINCT ...)
}

// FunctionCall represents a scalar function call (e.g. extract, to_timestamp).
type FunctionCall struct {
	FuncName string        // e.g. "extract", "to_timestamp"
	Args     []interface{} // Can be ColumnRef, FunctionCall, literal values, string
}

// GroupByItem represents a GROUP BY key, which can be a column ref or an alias.
type GroupByItem struct {
	Column *ColumnRef // Direct column reference (nil if using alias)
	Alias  string     // Alias reference from SELECT list
}

// OrderItem represents an ORDER BY item.
type OrderItem struct {
	Ref        string // Column name, alias, or aggregate expression
	Desc       bool   // true = DESC, false = ASC (default)
	IsDistance bool   // true when ordering by vector distance
}

// WhereClause holds the WHERE condition, both as the original AST
// and the converted Milvus expression string.
type WhereClause struct {
	MilvusExpr string         // Converted Milvus filter expression (e.g. data["kind"] == "commit")
	RawExpr    *pg_query.Node // Original PG AST expression (for complex conversions)
}

// VectorDistOp represents the vector distance operator type.
type VectorDistOp int

const (
	VectorDistL2     VectorDistOp = iota // <->  L2 distance
	VectorDistCosine                     // <=>  cosine distance
	VectorDistIP                         // <#>  inner product (negative)
)

// MetricType returns the Milvus metric type string for this operator.
func (op VectorDistOp) MetricType() string {
	switch op {
	case VectorDistL2:
		return "L2"
	case VectorDistCosine:
		return "COSINE"
	case VectorDistIP:
		return "IP"
	default:
		return "L2"
	}
}

// VectorDistExpr represents a vector distance expression in the SELECT list.
//
// Example: embedding <-> '[1,2,3]'::vector AS distance
type VectorDistExpr struct {
	Column   *ColumnRef   // Vector field reference
	Operator VectorDistOp // Distance operator
	Vector   []float32    // Query vector literal
	ParamRef string       // Parameter placeholder, e.g. "$1" (alternative to Vector)
}

// VectorSearchDef holds the complete vector search definition
// extracted from the SQL query.
type VectorSearchDef struct {
	FieldName  string    // Vector field name
	FieldID    int64     // Resolved field ID (populated during schema resolution)
	MetricType string    // Inferred from operator: L2, COSINE, IP
	Vector     []float32 // Query vector literal (nil when using ParamRef)
	ParamRef   string    // Parameter placeholder, e.g. "$1" (alternative to Vector)
	TopK       int64     // From LIMIT clause
	DistAlias  string    // Alias for the distance column (for ORDER BY reference)
}
