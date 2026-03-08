// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package typeutil

import (
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func ParseAndVerifyNestedPath(identifier string, schema *schemapb.CollectionSchema, fieldID int64) (string, error) {
	helper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		return "", err
	}

	var identifierExpr *planpb.Expr
	err = planparserv2.ParseIdentifier(helper, identifier, func(expr *planpb.Expr) error {
		identifierExpr = expr
		return nil
	})
	if err != nil {
		return "", err
	}
	if identifierExpr.GetColumnExpr().GetInfo().GetFieldId() != fieldID {
		return "", errors.New("fieldID not match with field name")
	}

	nestedPath := identifierExpr.GetColumnExpr().GetInfo().GetNestedPath()
	// escape the nested path to avoid the path being interpreted as a JSON Pointer
	nestedPath = lo.Map(nestedPath, func(path string, _ int) string {
		s := strings.ReplaceAll(path, "~", "~0")
		s = strings.ReplaceAll(s, "/", "~1")
		return s
	})
	if len(nestedPath) == 0 {
		// if nested path is empty, it means the json path is the field name.
		// Dont return "/" here, it not a valid json path for simdjson.
		return "", nil
	}
	//nolint:gocritic // JSON pointer format requires leading slash and forward slashes
	return "/" + strings.Join(nestedPath, "/"), nil
}
