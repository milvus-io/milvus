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

package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/sqlparser"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

func TestBuildColumnNames(t *testing.T) {
	tests := []struct {
		name   string
		comp   *sqlparser.SqlComponents
		expect []string
	}{
		{
			name: "column with alias",
			comp: &sqlparser.SqlComponents{
				SelectItems: []sqlparser.SelectItem{
					{
						Type:   sqlparser.SelectColumn,
						Column: &sqlparser.ColumnRef{FieldName: "data", NestedPath: []string{"kind"}},
						Alias:  "event_type",
					},
				},
			},
			expect: []string{"event_type"},
		},
		{
			name: "column without alias",
			comp: &sqlparser.SqlComponents{
				SelectItems: []sqlparser.SelectItem{
					{
						Type:   sqlparser.SelectColumn,
						Column: &sqlparser.ColumnRef{FieldName: "id"},
					},
				},
			},
			expect: []string{"id"},
		},
		{
			name: "count star",
			comp: &sqlparser.SqlComponents{
				SelectItems: []sqlparser.SelectItem{
					{
						Type:    sqlparser.SelectAgg,
						AggFunc: &sqlparser.AggFuncCall{FuncName: "count"},
					},
				},
			},
			expect: []string{"count(*)"},
		},
		{
			name: "count star with alias",
			comp: &sqlparser.SqlComponents{
				SelectItems: []sqlparser.SelectItem{
					{
						Type:    sqlparser.SelectAgg,
						AggFunc: &sqlparser.AggFuncCall{FuncName: "count"},
						Alias:   "total",
					},
				},
			},
			expect: []string{"total"},
		},
		{
			name: "sum with json field",
			comp: &sqlparser.SqlComponents{
				SelectItems: []sqlparser.SelectItem{
					{
						Type: sqlparser.SelectAgg,
						AggFunc: &sqlparser.AggFuncCall{
							FuncName: "sum",
							Arg:      &sqlparser.ColumnRef{FieldName: "data", NestedPath: []string{"amount"}},
						},
					},
				},
			},
			expect: []string{`sum(data["amount"])`},
		},
		{
			name: "mixed select items",
			comp: &sqlparser.SqlComponents{
				SelectItems: []sqlparser.SelectItem{
					{
						Type:   sqlparser.SelectColumn,
						Column: &sqlparser.ColumnRef{FieldName: "data", NestedPath: []string{"kind"}},
						Alias:  "kind",
					},
					{
						Type:    sqlparser.SelectAgg,
						AggFunc: &sqlparser.AggFuncCall{FuncName: "count"},
						Alias:   "cnt",
					},
					{
						Type: sqlparser.SelectAgg,
						AggFunc: &sqlparser.AggFuncCall{
							FuncName: "sum",
							Arg:      &sqlparser.ColumnRef{FieldName: "data", NestedPath: []string{"val"}},
						},
					},
				},
			},
			expect: []string{"kind", "cnt", `sum(data["val"])`},
		},
		{
			name:   "empty select",
			comp:   &sqlparser.SqlComponents{},
			expect: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildColumnNames(tt.comp)
			assert.Equal(t, tt.expect, result)
		})
	}
}

func TestFormatAggForOutput(t *testing.T) {
	tests := []struct {
		name   string
		agg    *sqlparser.AggFuncCall
		expect string
	}{
		{
			name:   "count_star",
			agg:    &sqlparser.AggFuncCall{FuncName: "count"},
			expect: "count(*)",
		},
		{
			name: "count_field",
			agg: &sqlparser.AggFuncCall{
				FuncName: "count",
				Arg:      &sqlparser.ColumnRef{FieldName: "id"},
			},
			expect: "count(id)",
		},
		{
			name: "sum_plain",
			agg: &sqlparser.AggFuncCall{
				FuncName: "sum",
				Arg:      &sqlparser.ColumnRef{FieldName: "price"},
			},
			expect: "sum(price)",
		},
		{
			name: "sum_json",
			agg: &sqlparser.AggFuncCall{
				FuncName: "sum",
				Arg:      &sqlparser.ColumnRef{FieldName: "data", NestedPath: []string{"amount"}},
			},
			expect: `sum(data["amount"])`,
		},
		{
			name: "avg_json_nested",
			agg: &sqlparser.AggFuncCall{
				FuncName: "avg",
				Arg:      &sqlparser.ColumnRef{FieldName: "data", NestedPath: []string{"stats", "score"}},
			},
			expect: `avg(data["stats"]["score"])`,
		},
		{
			name: "min_field",
			agg: &sqlparser.AggFuncCall{
				FuncName: "min",
				Arg:      &sqlparser.ColumnRef{FieldName: "ts"},
			},
			expect: "min(ts)",
		},
		{
			name: "max_field",
			agg: &sqlparser.AggFuncCall{
				FuncName: "max",
				Arg:      &sqlparser.ColumnRef{FieldName: "ts"},
			},
			expect: "max(ts)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatAggForOutput(tt.agg)
			assert.Equal(t, tt.expect, result)
		})
	}
}

func TestSqlQueryTask_OnEnqueue(t *testing.T) {
	task := &sqlQueryTask{
		RetrieveRequest: &internalpb.RetrieveRequest{
			Base: &commonpb.MsgBase{},
		},
	}
	err := task.OnEnqueue()
	assert.NoError(t, err)
	assert.Equal(t, commonpb.MsgType_Retrieve, task.Base.MsgType)
	assert.NotZero(t, task.Base.SourceID)
}

func TestSqlQueryTask_OnEnqueue_NilBase(t *testing.T) {
	task := &sqlQueryTask{
		RetrieveRequest: &internalpb.RetrieveRequest{},
	}
	err := task.OnEnqueue()
	assert.NoError(t, err)
	assert.NotNil(t, task.Base)
	assert.Equal(t, commonpb.MsgType_Retrieve, task.Base.MsgType)
}

func TestSqlQueryTask_TaskInterface(t *testing.T) {
	task := &sqlQueryTask{
		RetrieveRequest: &internalpb.RetrieveRequest{
			Base: &commonpb.MsgBase{
				MsgID:     123,
				MsgType:   commonpb.MsgType_Retrieve,
				Timestamp: 456,
			},
		},
	}

	assert.Equal(t, int64(123), task.ID())
	assert.Equal(t, commonpb.MsgType_Retrieve, task.Type())
	assert.Equal(t, Timestamp(456), task.BeginTs())
	assert.Equal(t, Timestamp(456), task.EndTs())
	assert.Equal(t, "SqlQueryTask", task.Name())

	task.SetID(789)
	assert.Equal(t, int64(789), task.ID())

	task.SetTs(1000)
	assert.Equal(t, Timestamp(1000), task.BeginTs())
}
