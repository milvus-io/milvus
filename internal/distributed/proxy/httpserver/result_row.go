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

package httpserver

import (
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	responseFormatLegacy     = "legacy"
	responseFormatNamespaced = "namespaced"
)

func useNamespacedResponse(value string) (bool, error) {
	switch value {
	case "", responseFormatLegacy:
		return false, nil
	case responseFormatNamespaced:
		return true, nil
	default:
		return false, merr.WrapErrParameterInvalidMsg("responseFormat must be legacy or namespaced")
	}
}

// resultRow selects map destinations once per row. Legacy destinations alias the
// same map: field decoding and write order are shared without copying the row.
type resultRow struct {
	fields        map[string]interface{}
	dynamicFields map[string]interface{}
	meta          map[string]interface{}
	output        map[string]interface{}
}

func newResultRow(namespaced bool) resultRow {
	if namespaced {
		return newNamespacedResultRow()
	}
	row := map[string]interface{}{}
	return resultRow{fields: row, dynamicFields: row, meta: row, output: row}
}

type dynamicFieldOutput uint8

const (
	dynamicFieldSelect dynamicFieldOutput = iota
	dynamicFieldFlatten
	dynamicFieldNamespace
)

func newNamespacedResultRow() resultRow {
	row := resultRow{
		fields: map[string]interface{}{}, dynamicFields: map[string]interface{}{}, meta: map[string]interface{}{},
	}
	row.output = map[string]interface{}{
		"fields": row.fields, "dynamicFields": row.dynamicFields, "meta": row.meta,
	}
	return row
}

// queryResultMetadata carries values from the RPC response.
type queryResultMetadata struct {
	ids           *schemapb.IDs
	scores        []float32
	searchOffsets *schemapb.LongArray
	queryOffsets  []*milvuspb.ElementIndices
}

// A bare static name and an explicit dynamic selector can resolve to the same
// output name. Select dynamic keys from the original request before writing rows.
func namespacedDynamicFields(requested, resolved []string, fields []*schemapb.FieldData, schema *schemapb.CollectionSchema) (keys []string, allDynamicFields bool, err error) {
	var dynamic *schemapb.FieldData
	for _, field := range fields {
		if field.GetIsDynamic() {
			dynamic = field
			break
		}
	}
	if dynamic == nil {
		return nil, false, nil
	}
	if len(requested) == 0 {
		requested = resolved
	}
	selectors := make([]string, len(requested))
	for i, selector := range requested {
		selectors[i] = strings.TrimSpace(selector)
		switch selectors[i] {
		case "*", "$meta", dynamic.GetFieldName():
			return nil, true, nil
		}
	}
	var helper *typeutil.SchemaHelper
	for _, selector := range genDynamicFields(selectors, fields) {
		open, bracket := strings.IndexByte(selector, '('), strings.IndexByte(selector, '[')
		if open >= 0 && (bracket < 0 || open < bracket) {
			continue
		}
		if helper == nil {
			helper, err = typeutil.CreateSchemaHelper(schema)
			if err != nil {
				return nil, false, merr.Wrap(err, "invalid schema for dynamic result")
			}
		}
		err = planparserv2.ParseIdentifier(helper, selector, func(expr *planpb.Expr) error {
			col := expr.GetColumnExpr().GetInfo()
			field, lookupErr := helper.GetFieldFromID(col.GetFieldId())
			if lookupErr == nil && field.GetIsDynamic() && len(col.GetNestedPath()) == 1 {
				keys = append(keys, col.GetNestedPath()[0])
			}
			return nil
		})
		if err != nil {
			return nil, false, err
		}
	}
	return keys, false, nil
}
