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
	"context"

	"github.com/bytedance/sonic/ast"
	"github.com/tidwall/gjson"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// rawDataRows provides gjson-compatible row views without gjson's full-body
// scan or Array conversion. The active bulk binder has already validated the
// complete JSON envelope; this pass preserves each row's original spelling
// for schema-sensitive conversion and checks cancellation at row boundaries.
func rawDataRows(ctx context.Context, body []byte) ([]gjson.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	parser := ast.NewParser(string(body))
	root, parseErr := parser.Parse()
	if parseErr != 0 {
		return nil, parser.ExportError(parseErr)
	}
	if root.TypeSafe() != ast.V_OBJECT {
		return nil, merr.WrapErrParameterInvalidMsg("REST request body must be an object")
	}
	data := root.Get("data")
	if !data.Exists() {
		return nil, nil
	}
	if data.TypeSafe() != ast.V_ARRAY {
		return nil, merr.WrapErrParameterInvalidMsg("data must be an array")
	}
	var rows []gjson.Result
	var scanErr error
	err := data.ForEach(func(_ ast.Sequence, row *ast.Node) bool {
		if scanErr = ctx.Err(); scanErr != nil {
			return false
		}
		raw, err := row.Raw()
		if err != nil {
			scanErr = err
			return false
		}
		rows = append(rows, gjson.Parse(raw))
		return true
	})
	if err != nil {
		return nil, err
	}
	if scanErr != nil {
		return nil, scanErr
	}
	return rows, ctx.Err()
}
