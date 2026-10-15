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

package clustering

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestDenseClusteringKeyAdmission(t *testing.T) {
	paramtable.Init()
	cfg := &paramtable.Get().CommonCfg
	for _, item := range []*paramtable.ParamItem{&cfg.EnableVectorClusteringKey, &cfg.UseVectorAsClusteringKey, &cfg.UsePartitionKeyAsClusteringKey} {
		old := item.SwapTempValue("true")
		t.Cleanup(func() { item.SwapTempValue(old) })
	}
	for _, dtype := range []schemapb.DataType{schemapb.DataType_FloatVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector} {
		t.Run(dtype.String(), func(t *testing.T) {
			vector := &schemapb.FieldSchema{FieldID: 100, DataType: dtype}
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{vector}}
			require.Same(t, vector, GetClusteringKeyField(schema))
			second := &schemapb.FieldSchema{FieldID: 101, DataType: schemapb.DataType_Float16Vector}
			schema.Fields = append(schema.Fields, second)
			require.Nil(t, GetClusteringKeyField(schema), "multiple eligible vectors require explicit selection")
			vector.IsClusteringKey = true
			require.Same(t, vector, GetClusteringKeyField(schema))
			cfg.EnableVectorClusteringKey.SwapTempValue("false")
			require.Nil(t, GetClusteringKeyField(schema))
			cfg.EnableVectorClusteringKey.SwapTempValue("true")
			vector.IsClusteringKey = false
			partition := &schemapb.FieldSchema{FieldID: 102, DataType: schemapb.DataType_Int64, IsPartitionKey: true}
			schema.Fields = []*schemapb.FieldSchema{vector, partition}
			require.Same(t, partition, GetClusteringKeyField(schema), "partition-key priority is preserved")
			vector.IsClusteringKey = true
			require.Same(t, vector, GetClusteringKeyField(schema))
			vector.IsClusteringKey = false
			schema.Fields = []*schemapb.FieldSchema{vector}
			cfg.UseVectorAsClusteringKey.SwapTempValue("false")
			require.Nil(t, GetClusteringKeyField(schema))
			cfg.UseVectorAsClusteringKey.SwapTempValue("true")
		})
	}
	for _, dtype := range []schemapb.DataType{schemapb.DataType_BinaryVector, schemapb.DataType_SparseFloatVector, schemapb.DataType_Int8Vector, schemapb.DataType_ArrayOfVector} {
		field := &schemapb.FieldSchema{DataType: dtype}
		schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}}
		require.Nil(t, GetClusteringKeyField(schema))
		field.IsClusteringKey = true
		require.Nil(t, GetClusteringKeyField(schema))
	}
}
