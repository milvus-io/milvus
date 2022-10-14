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

package autoindex

import (
	"fmt"
	"strconv"
	"strings"

	"go/ast"
	"go/parser"

	//"encoding/json"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

const (
	VariablePrefix = "__"
)

type identVisitor struct {
	input string
	valid bool
}

func (v *identVisitor) Visit(n ast.Node) ast.Visitor {
	if n == nil {
		return nil
	}
	switch d := n.(type) {
	case *ast.Ident:
		if strings.HasPrefix(d.Name, VariablePrefix) {
			if v.valid {
				if v.input != d.Name {
					v.valid = false
					return nil
				}
			} else {
				v.input = d.Name
				v.valid = true
			}
		}
	}
	return v
}

func parseIdentFromExpr(expression string) (string, error) {
	expr, err := parser.ParseExpr(expression)
	if err != nil {
		return "", fmt.Errorf("parse input from expression failed: %v", err)
	}
	var x identVisitor
	ast.Walk(&x, expr)
	if !x.valid {
		return "", fmt.Errorf("parse input from expression failed: number of input variable should be 1")
	}
	return x.input, nil
}

func parseAssignment(stmt string) (input string, output string, expr string, err error) {
	defer func() {
		if err != nil {
			input = ""
			output = ""
			expr = ""
		}
	}()
	exprs := strings.Split(stmt, "=")
	if len(exprs) != 2 {
		err = fmt.Errorf("parse assignment stmt failed, wrong format")
		return
	}
	output, err = parseIdentFromExpr(exprs[0])
	if err != nil {
		err = fmt.Errorf("parse assignment stmt failed, wrong lvalue format:%w", err)
		return
	}
	expr = exprs[1]
	input, err = parseIdentFromExpr(expr)
	if err != nil {
		err = fmt.Errorf("parse assignment stmt failed, wrong rvalue format:%w", err)
		return
	}
	return
}

func getInt64FromParams(params []*commonpb.KeyValuePair, key string) (int64, error) {
	valueStr, err := funcutil.GetAttrByKeyFromRepeatedKV(key, params)
	if err != nil {
		return 0, fmt.Errorf("%s not found in search_params", key)
	}
	value, err := strconv.ParseInt(valueStr, 0, 64)
	if err != nil {
		return 0, fmt.Errorf("%s [%s] is invalid", key, valueStr)
	}
	return value, nil
}
