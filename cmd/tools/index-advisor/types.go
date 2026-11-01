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

package main

// This file holds the types the rule engine works on: column identifies what
// a predicate reads, fieldContext is the input of every rule, advice is what
// the rules build, and candidate is one index the advice proposes.

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Recommendation actions.
const (
	actionValidate       = "validate_candidates" // unindexed: validate the candidates
	actionReviewExisting = "review_existing"     // indexed: check the existing index, compare candidates
	actionNoAction       = "no_action"
)

// Candidate roles.
const (
	roleBaseline    = "baseline"    // AUTOINDEX
	roleAlternative = "alternative" // a specific type with evidence
)

// column identifies what a predicate reads: a field, or a JSON path inside a
// JSON field (dynamic fields are paths inside the hidden $meta field).
type column struct {
	field        string
	jsonPath     []string
	dataType     schemapb.DataType // type of the field itself; JSON for paths
	elementType  schemapb.DataType // element type of an ARRAY field
	isPrimaryKey bool
	isDynamic    bool
}

// String renders the column as the report shows it, in a form Milvus's parser
// accepts: price, meta["brand"], $meta["color"] for a dynamic key, or tags[0]
// for an element of an ARRAY field.
func (c column) String() string {
	var b strings.Builder
	b.WriteString(c.field)
	for _, p := range c.jsonPath {
		if typeutil.IsArrayType(c.dataType) {
			b.WriteString("[" + p + "]")
		} else {
			b.WriteString("[" + strconv.Quote(p) + "]")
		}
	}
	return b.String()
}

// typeName is the column's type as the report shows it.
func (c column) typeName() string {
	switch {
	case len(c.jsonPath) > 0:
		return "JSON path"
	case typeutil.IsArrayType(c.dataType):
		return fmt.Sprintf("Array<%s>", c.elementType)
	default:
		return c.dataType.String()
	}
}

// fieldContext is everything the rules know about one column: its type, how
// the analyzed requests filter on it, its statistics, and its existing indexes.
type fieldContext struct {
	db, collection string
	col            column
	kind           columnKind
	// castType is the json_cast_type an index on a JSON path needs, inferred
	// from the literals compared with the path; "" for other columns, or when
	// the literals do not determine it.
	castType string
	workload *fieldWorkload
	stats    fieldStatistics
	existing []indexSnapshot // from the snapshot; not evidence that any request used them
}

// fieldStatistics are the --field-stats numbers for a column; nil is unknown.
type fieldStatistics struct {
	rowCount      *int64
	distinctCount *int64
	nullFraction  *float64
}

// advice is what the rule chain concludes for one column.
type advice struct {
	action     string
	candidates []candidate
	notes      []string
}

func (a *advice) add(c candidate) { a.candidates = append(a.candidates, c) }
func (a *advice) note(n string)   { a.notes = append(a.notes, n) }

// candidate is one index to validate, as reported.
type candidate struct {
	IndexType string `json:"index_type"`
	Role      string `json:"role"`
	// FieldName is the field_name to create the index on; for a path in the
	// dynamic field it is the top-level key, as DescribeIndex reports it.
	FieldName  string            `json:"field_name"`
	Params     map[string]string `json:"params,omitempty"`
	Serves     []string          `json:"serves"`     // observed op classes it is meant to serve
	Predicates int               `json:"predicates"` // requests with at least one of those
	Evidence   []string          `json:"evidence"`
	Source     string            `json:"source"`

	// resolvesTo is, for AUTOINDEX, the type Milvus builds by default. It is
	// stated in Evidence and used to compare with existing indexes.
	resolvesTo string
}

func (c *candidate) setParam(key, value string) {
	if c.Params == nil {
		c.Params = map[string]string{}
	}
	c.Params[key] = value
}
