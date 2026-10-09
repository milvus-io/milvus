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

package requestbudget

import (
	"context"

	"github.com/bytedance/sonic"
	"github.com/bytedance/sonic/ast"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// MaxJSONUnitBytes is the draft capacity limit for a single REST data row.
// It is independent of the request's elapsed-time budget.
const MaxJSONUnitBytes = 4 << 20

// Keep normal batches smaller than one accepted row. This gives cancellation
// checkpoints without paying one Sonic call per small row.
const decodeBatchBytes = 1 << 20

// DecodeDataRows isolates a bulk REST data array and decodes it in bounded
// Sonic batches. It returns a small-data placeholder for the existing Gin
// binding/validation path; callers retain the unmodified body for
// schema-sensitive conversion after binding.
func DecodeDataRows(ctx context.Context, body []byte, maxUnitBytes int) ([]byte, []map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if maxUnitBytes <= 0 {
		return nil, nil, merr.WrapErrServiceInternalMsg("max JSON unit bytes must be positive")
	}
	parser := ast.NewParser(string(body))
	root, parseErr := parser.Parse()
	if parseErr != 0 {
		return nil, nil, parser.ExportError(parseErr)
	}
	if root.TypeSafe() != ast.V_OBJECT {
		return nil, nil, merr.WrapErrParameterInvalidMsg("REST request body must be an object")
	}
	metadata := make([]byte, 0, 256)
	metadata = append(metadata, '{')
	var rows []map[string]any
	var scanErr error
	dataFound := false
	fieldCount := 0
	err := root.ForEach(func(path ast.Sequence, value *ast.Node) bool {
		if scanErr = ctx.Err(); scanErr != nil {
			return false
		}
		if path.Key == nil {
			scanErr = merr.WrapErrServiceInternalMsg("REST JSON object field is missing its key")
			return false
		}
		if fieldCount != 0 {
			metadata = append(metadata, ',')
		}
		fieldCount++
		key, err := json.Marshal(*path.Key)
		if err != nil {
			scanErr = err
			return false
		}
		metadata = append(metadata, key...)
		metadata = append(metadata, ':')
		if *path.Key == "data" {
			if dataFound {
				scanErr = merr.WrapErrParameterInvalidMsg("REST request body has duplicate data fields")
				return false
			}
			dataFound = true
			if value.TypeSafe() != ast.V_ARRAY {
				scanErr = merr.WrapErrParameterInvalidMsg("data must be an array")
				return false
			}
			rows, scanErr = decodeArrayRows(ctx, value, maxUnitBytes)
			if scanErr != nil {
				return false
			}
			if len(rows) == 0 {
				metadata = append(metadata, "[]"...)
			} else {
				metadata = append(metadata, "[{}]"...)
			}
			return true
		}
		raw, err := value.Raw()
		if err != nil {
			scanErr = err
			return false
		}
		if len(metadata)+len(raw)+1 > maxUnitBytes {
			scanErr = merr.WrapErrParameterInvalidMsg("REST JSON metadata exceeds the %d byte limit", maxUnitBytes)
			return false
		}
		metadata = append(metadata, raw...)
		return true
	})
	if err != nil {
		return nil, nil, err
	}
	if scanErr != nil {
		return nil, nil, scanErr
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	// The AST parser is lazy and keeps a private parser copy per object. Validate
	// the complete envelope as well, including content after the root object.
	if !sonic.Valid(body) {
		return nil, nil, merr.WrapErrParameterInvalidMsg("REST request body is not valid JSON")
	}
	if !dataFound {
		if len(body) > maxUnitBytes {
			return nil, nil, merr.WrapErrParameterInvalidMsg("REST JSON metadata exceeds the %d byte limit", maxUnitBytes)
		}
		return body, nil, nil
	}
	metadata = append(metadata, '}')
	if len(metadata) > maxUnitBytes {
		return nil, nil, merr.WrapErrParameterInvalidMsg("REST JSON metadata exceeds the %d byte limit", maxUnitBytes)
	}
	return metadata, rows, nil
}

func decodeArrayRows(ctx context.Context, data *ast.Node, maxUnitBytes int) ([]map[string]any, error) {
	var rows []map[string]any
	chunk := make([]byte, 0, decodeBatchBytes)
	chunk = append(chunk, '[')
	flush := func() error {
		if len(chunk) == 1 {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk = append(chunk, ']')
		var batch []map[string]any
		if err := json.Unmarshal(chunk, &batch); err != nil {
			return err
		}
		rows = append(rows, batch...)
		chunk = chunk[:1]
		return ctx.Err()
	}
	var scanErr error
	err := data.ForEach(func(_ ast.Sequence, value *ast.Node) bool {
		if scanErr = ctx.Err(); scanErr != nil {
			return false
		}
		raw, err := value.Raw()
		if err != nil {
			scanErr = err
			return false
		}
		if len(raw) > maxUnitBytes {
			scanErr = merr.WrapErrParameterInvalidMsg("one JSON data row exceeds the %d byte limit", maxUnitBytes)
			return false
		}
		if len(chunk) > 1 && len(chunk)+len(raw)+2 > decodeBatchBytes {
			if scanErr = flush(); scanErr != nil {
				return false
			}
		}
		if len(chunk) > 1 {
			chunk = append(chunk, ',')
		}
		chunk = append(chunk, raw...)
		return true
	})
	if err != nil {
		return nil, err
	}
	if scanErr != nil {
		return nil, scanErr
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return rows, nil
}
