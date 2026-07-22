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

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// Parse parses a SQL string and returns the full parse result.
func Parse(sql string) (*pg_query.ParseResult, error) {
	result, err := pg_query.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("sql parse error: %w", err)
	}
	if len(result.Stmts) == 0 {
		return nil, fmt.Errorf("empty SQL statement")
	}
	return result, nil
}

// ParseOne parses a single SQL statement and returns the RawStmt AST node.
func ParseOne(sql string) (*pg_query.RawStmt, error) {
	result, err := Parse(sql)
	if err != nil {
		return nil, err
	}
	if len(result.Stmts) != 1 {
		return nil, fmt.Errorf("expected 1 statement, got %d", len(result.Stmts))
	}
	return result.Stmts[0], nil
}
