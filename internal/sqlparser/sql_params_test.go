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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestBuildSQLParamTemplateValues(t *testing.T) {
	values, err := BuildSQLParamTemplateValues(map[string]*schemapb.TemplateValue{
		"$1": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 18}},
		"2":  {Val: &schemapb.TemplateValue_StringVal{StringVal: "18"}},
		"$3": {Val: &schemapb.TemplateValue_BoolVal{BoolVal: true}},
	})
	require.NoError(t, err)

	assert.Equal(t, int64(18), values["sql_param_1"].GetInt64Val())
	assert.Equal(t, "18", values["sql_param_2"].GetStringVal())
	assert.True(t, values["sql_param_3"].GetBoolVal())
}

func TestBuildSQLParamTemplateValuesRejectsNilValue(t *testing.T) {
	_, err := BuildSQLParamTemplateValues(map[string]*schemapb.TemplateValue{"$1": nil})
	assert.Error(t, err)
}
