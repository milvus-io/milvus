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

package testcases

import (
	"fmt"
	"maps"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// Generate concrete tagged struct fields for each vector representation, without six duplicate row types.
func rlsSchemaStructInput(r rlsSchemaRow) any {
	names := slices.Sorted(maps.Keys(r))
	fields := make([]reflect.StructField, len(names))
	for i, name := range names {
		fields[i] = reflect.StructField{Name: fmt.Sprintf("Field%d", i), Type: reflect.TypeOf(r[name]), Tag: reflect.StructTag(fmt.Sprintf(`milvus:"name:%s"`, name))}
	}
	value := reflect.New(reflect.StructOf(fields))
	for i, name := range names {
		value.Elem().Field(i).Set(reflect.ValueOf(r[name]))
	}
	return value.Interface()
}

// existing/new mixed upsert batches exercise all vectors and both PK types through all serializers.
func TestRLSSchemaUpsertCreatesRows(t *testing.T) {
	for _, vector := range []string{"float32", "binary", "float16", "bfloat16", "int8", "sparse"} {
		for _, varchar := range []bool{false, true} {
			for _, mode := range []string{"column", "row_map", "row_struct"} {
				t.Run(fmt.Sprintf("%s/varchar_%v/%s", vector, varchar, mode), func(t *testing.T) {
					t.Parallel()
					ctx := hp.CreateContext(t, 6*time.Minute)
					mc := hp.CreateDefaultMilvusClient(ctx, t).Client
					tc := rlsSchemaCase{vector: vector, varchar: varchar}
					name := prepareRLSSchema(ctx, t, mc, tc)
					all := map[any]rlsSchemaRow{}
					for i, owner := range []string{"alice", "bob", "alice", "bob"} {
						r := rlsSchemaData(t, tc, i+1, 0, owner)
						_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name, rlsSchemaWriteColumns(t, tc, r)...).WithRLSPrincipal(owner))
						require.NoError(t, err)
						all[r["id"]] = r
					}
					for _, stage := range []string{"full_mixed", "partial_create", "partial_mixed", "rejected_mixed"} {
						t.Run(stage, func(t *testing.T) {
							ids := []int{1, 5}
							partial := false
							switch stage {
							case "partial_create":
								ids = []int{7}
								partial = true
							case "partial_mixed":
								ids = []int{3, 9}
								partial = true
							case "rejected_mixed":
								ids = []int{1, 2}
								partial = true
							}
							rows := make([]rlsSchemaRow, len(ids))
							values := make([]any, len(ids))
							for i, id := range ids {
								rows[i] = rlsSchemaData(t, tc, id, 2, "alice")
								if mode == "row_struct" {
									values[i] = rlsSchemaStructInput(rows[i])
								} else {
									values[i] = rlsSchemaRowInput(rows[i])
								}
							}
							var option client.UpsertOption
							if mode == "column" {
								option = client.NewColumnBasedInsertOption(name, rlsSchemaWriteColumns(t, tc, rows...)...).WithPartialUpdate(partial).WithRLSPrincipal("alice")
							} else {
								option = client.NewRowBasedInsertOption(name, values...).WithPartialUpdate(partial).WithRLSPrincipal("alice")
							}
							result, err := mc.Upsert(ctx, option)
							if stage == "rejected_mixed" {
								require.ErrorContains(t, err, "RLS")
							} else {
								require.NoError(t, err)
								require.EqualValues(t, len(rows), result.UpsertCount)
								for _, r := range rows {
									all[r["id"]] = r
								}
							}
							checkRLSSchemaReads(ctx, t, mc, name, tc, all)
						})
					}
					rlsFlush(ctx, t, mc, name)
					t.Run("flushed", func(t *testing.T) { checkRLSSchemaReads(ctx, t, mc, name, tc, all) })
				})
			}
		}
	}
}
