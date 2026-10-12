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

package planparserv2

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// The REST id-based get and delete endpoints build their filter as
// `pk in {__pk_ids}` and pass the ids as a template value, rather than
// formatting them into the expression text where a quote in an id was parsed as
// syntax. This is the parser side of that contract.
func TestPrimaryKeyIDsAsTemplateValue(t *testing.T) {
	helper := newTestSchemaHelper(t)

	t.Run("int64 ids", func(t *testing.T) {
		mv := map[string]*schemapb.TemplateValue{"__pk_ids": {
			Val: &schemapb.TemplateValue_ArrayVal{ArrayVal: &schemapb.TemplateArrayValue{
				Data: &schemapb.TemplateArrayValue_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2, 9007199254740993}}},
			}},
		}}
		_, err := CreateRetrievePlan(helper, "Int64Field in {__pk_ids}", mv)
		require.NoError(t, err)
	})

	t.Run("varchar ids with quotes and backslashes", func(t *testing.T) {
		mv := map[string]*schemapb.TemplateValue{"__pk_ids": {
			Val: &schemapb.TemplateValue_ArrayVal{ArrayVal: &schemapb.TemplateArrayValue{
				Data: &schemapb.TemplateArrayValue_StringData{StringData: &schemapb.StringArray{
					Data: []string{`alice", "bob`, `say "hi"`, `back\slash`},
				}},
			}},
		}}
		plan, err := CreateRetrievePlan(helper, "VarCharField in {__pk_ids}", mv)
		require.NoError(t, err)
		require.NotNil(t, plan)
		// the crafted id must survive as ONE term, not become expression syntax
		values := plan.GetQuery().GetPredicates().GetTermExpr().GetValues()
		require.Len(t, values, 3, "three ids in, three terms out")
		got := make([]string, 0, len(values))
		for _, v := range values {
			got = append(got, v.GetStringVal())
		}
		assert.ElementsMatch(t, []string{`alice", "bob`, `say "hi"`, `back\slash`}, got)
	})
}
