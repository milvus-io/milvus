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

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// SQLParamTemplateName maps PostgreSQL positional params ($1, $2, ...) to
// planparserv2 expression template names.
func SQLParamTemplateName(number int) string {
	return fmt.Sprintf("sql_param_%d", number)
}

// BuildSQLParamTemplateValues normalizes SQL request params for planparserv2.
// Positional keys "$1" and "1" are normalized to sql_param_1.
func BuildSQLParamTemplateValues(params map[string]*schemapb.TemplateValue) (map[string]*schemapb.TemplateValue, error) {
	if len(params) == 0 {
		return nil, nil
	}

	values := make(map[string]*schemapb.TemplateValue, len(params))
	for key, value := range params {
		if value == nil {
			return nil, fmt.Errorf("SQL param %q is nil", key)
		}
		values[NormalizeSQLParamName(key)] = value
	}
	return values, nil
}

// NormalizeSQLParamTemplateValues normalizes positional SQL params without
// returning an error. It is intended for REST params immediately converted from
// JSON values by the HTTP layer.
func NormalizeSQLParamTemplateValues(params map[string]*schemapb.TemplateValue) map[string]*schemapb.TemplateValue {
	values, err := BuildSQLParamTemplateValues(params)
	if err != nil {
		return params
	}
	return values
}

func NormalizeSQLParamName(key string) string {
	key = strings.TrimSpace(key)
	if number, ok := parseSQLParamNumber(strings.TrimPrefix(key, "$")); ok {
		return SQLParamTemplateName(number)
	}
	return key
}

func parseSQLParamNumber(raw string) (int, bool) {
	if raw == "" {
		return 0, false
	}
	number, err := strconv.Atoi(raw)
	if err != nil || number <= 0 {
		return 0, false
	}
	return number, true
}
