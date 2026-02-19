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

package delegator

import (
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

const benchNumQueryPKs = 2000

// buildPlanForProto creates a PlanNode with the specified number of PK values
// for proto serialization benchmarks.
func buildPlanForProto(numPKs int, dataType schemapb.DataType) *planpb.PlanNode {
	values := make([]*planpb.GenericValue, numPKs)
	for i := 0; i < numPKs; i++ {
		switch dataType {
		case schemapb.DataType_Int64:
			values[i] = &planpb.GenericValue{
				Val: &planpb.GenericValue_Int64Val{Int64Val: int64(i)},
			}
		case schemapb.DataType_VarChar:
			values[i] = &planpb.GenericValue{
				Val: &planpb.GenericValue_StringVal{StringVal: fmt.Sprintf("pk_%d", i)},
			}
		}
	}

	return &planpb.PlanNode{
		Node: &planpb.PlanNode_Query{
			Query: &planpb.QueryPlanNode{
				Predicates: &planpb.Expr{
					Expr: &planpb.Expr_TermExpr{
						TermExpr: &planpb.TermExpr{
							ColumnInfo: &planpb.ColumnInfo{
								FieldId:      100,
								DataType:     dataType,
								IsPrimaryKey: true,
							},
							Values: values,
						},
					},
				},
			},
		},
	}
}

// BenchmarkProtoMarshalPlan2000PKs benchmarks proto.Marshal on a PlanNode with 2000 PKs.
func BenchmarkProtoMarshalPlan2000PKs(b *testing.B) {
	b.Run("Int64", func(b *testing.B) {
		plan := buildPlanForProto(benchNumQueryPKs, schemapb.DataType_Int64)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := proto.Marshal(plan)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("VarChar", func(b *testing.B) {
		plan := buildPlanForProto(benchNumQueryPKs, schemapb.DataType_VarChar)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := proto.Marshal(plan)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkProtoUnmarshalPlan2000PKs benchmarks proto.Unmarshal on a PlanNode with 2000 PKs.
func BenchmarkProtoUnmarshalPlan2000PKs(b *testing.B) {
	b.Run("Int64", func(b *testing.B) {
		plan := buildPlanForProto(benchNumQueryPKs, schemapb.DataType_Int64)
		data, err := proto.Marshal(plan)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var out planpb.PlanNode
			if err := proto.Unmarshal(data, &out); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("VarChar", func(b *testing.B) {
		plan := buildPlanForProto(benchNumQueryPKs, schemapb.DataType_VarChar)
		data, err := proto.Marshal(plan)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var out planpb.PlanNode
			if err := proto.Unmarshal(data, &out); err != nil {
				b.Fatal(err)
			}
		}
	})
}
