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

package orderby

import (
	"fmt"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

// OrderByField defines a single sort key for ORDER BY operations.
// This is the shared type used across proxy, delegator, and querynode levels.
type OrderByField struct {
	FieldID    int64
	FieldName  string
	Ascending  bool // true = ASC (default), false = DESC
	NullsFirst bool // true = NULLS FIRST, false = NULLS LAST
	DataType   schemapb.DataType
}

// String returns a human-readable representation
func (f *OrderByField) String() string {
	dir := "ASC"
	if !f.Ascending {
		dir = "DESC"
	}
	nulls := "NULLS LAST"
	if f.NullsFirst {
		nulls = "NULLS FIRST"
	}
	return fmt.Sprintf("%s %s %s", f.FieldName, dir, nulls)
}

// orderByFieldBuilder is used internally to track which options were set
type orderByFieldBuilder struct {
	field                   *OrderByField
	nullsFirstSetExplicitly bool
}

// OrderByFieldOption is a functional option for creating OrderByField
type OrderByFieldOption func(*orderByFieldBuilder)

// WithAscending sets the ascending flag
func WithAscending(ascending bool) OrderByFieldOption {
	return func(b *orderByFieldBuilder) {
		b.field.Ascending = ascending
	}
}

// WithNullsFirst sets the nulls_first flag explicitly
func WithNullsFirst(nullsFirst bool) OrderByFieldOption {
	return func(b *orderByFieldBuilder) {
		b.field.NullsFirst = nullsFirst
		b.nullsFirstSetExplicitly = true
	}
}

// NewOrderByField creates a new OrderByField with the given options.
// Default: Ascending=true, NullsFirst follows PostgreSQL convention (ASC->NULLS LAST, DESC->NULLS FIRST)
func NewOrderByField(fieldID int64, fieldName string, dataType schemapb.DataType, opts ...OrderByFieldOption) *OrderByField {
	builder := &orderByFieldBuilder{
		field: &OrderByField{
			FieldID:   fieldID,
			FieldName: fieldName,
			DataType:  dataType,
			Ascending: true, // default ASC
		},
		nullsFirstSetExplicitly: false,
	}

	// Apply options
	for _, opt := range opts {
		opt(builder)
	}

	// Apply PostgreSQL convention for null handling if not explicitly set
	// Default: ASC -> NULLS LAST (false), DESC -> NULLS FIRST (true)
	if !builder.nullsFirstSetExplicitly {
		builder.field.NullsFirst = !builder.field.Ascending
	}

	return builder.field
}

// ParseOrderByFields parses ORDER BY specification from string slice.
// Format: ["field1:asc", "field2:desc:nulls_first"] or ["field1", "field2:desc"]
// Default direction is ASC if not specified.
// Default null handling follows PostgreSQL: ASC -> NULLS LAST, DESC -> NULLS FIRST
func ParseOrderByFields(orderByStrs []string, schema *schemapb.CollectionSchema) ([]*OrderByField, error) {
	if len(orderByStrs) == 0 {
		return nil, nil
	}

	// Build field name to schema map
	fieldNameToSchema := make(map[string]*schemapb.FieldSchema)
	for _, field := range schema.Fields {
		fieldNameToSchema[field.Name] = field
	}

	result := make([]*OrderByField, 0, len(orderByStrs))

	for _, orderByStr := range orderByStrs {
		orderByStr = strings.TrimSpace(orderByStr)
		if orderByStr == "" {
			continue
		}

		// Parse "field:direction:nulls_option" format
		parts := strings.Split(orderByStr, ":")
		fieldName := strings.TrimSpace(parts[0])

		ascending := true
		if len(parts) > 1 {
			dir := strings.ToLower(strings.TrimSpace(parts[1]))
			switch dir {
			case "desc", "descending":
				ascending = false
			case "asc", "ascending", "":
				ascending = true
			default:
				return nil, fmt.Errorf("invalid order direction '%s' for field '%s', must be 'asc' or 'desc'", dir, fieldName)
			}
		}

		// Default null handling follows PostgreSQL convention
		nullsFirst := !ascending // ASC -> NULLS LAST (false), DESC -> NULLS FIRST (true)

		// Parse explicit null ordering if provided
		if len(parts) > 2 {
			nullOpt := strings.ToLower(strings.TrimSpace(parts[2]))
			switch nullOpt {
			case "nulls_first":
				nullsFirst = true
			case "nulls_last":
				nullsFirst = false
			default:
				return nil, fmt.Errorf("invalid null ordering '%s', must be 'nulls_first' or 'nulls_last'", nullOpt)
			}
		}

		// Validate field exists
		fieldSchema, found := fieldNameToSchema[fieldName]
		if !found {
			return nil, fmt.Errorf("order_by field '%s' does not exist in collection schema", fieldName)
		}

		// Validate field type is sortable
		if !IsSortableType(fieldSchema.DataType) {
			return nil, fmt.Errorf("order_by field '%s' has type %s which is not sortable",
				fieldName, fieldSchema.DataType.String())
		}

		result = append(result, &OrderByField{
			FieldID:    fieldSchema.FieldID,
			FieldName:  fieldName,
			Ascending:  ascending,
			NullsFirst: nullsFirst,
			DataType:   fieldSchema.DataType,
		})
	}

	return result, nil
}

// IsSortableType checks if a data type can be used in ORDER BY
func IsSortableType(dt schemapb.DataType) bool {
	switch dt {
	case schemapb.DataType_Bool,
		schemapb.DataType_Int8,
		schemapb.DataType_Int16,
		schemapb.DataType_Int32,
		schemapb.DataType_Int64,
		schemapb.DataType_Float,
		schemapb.DataType_Double,
		schemapb.DataType_String,
		schemapb.DataType_VarChar:
		return true
	default:
		return false
	}
}
