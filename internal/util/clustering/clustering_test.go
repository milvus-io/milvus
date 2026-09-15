// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package clustering

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestGetClusteringKeyFieldDenseFloatVector(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	require.NoError(t, params.Save(params.CommonCfg.EnableVectorClusteringKey.Key, "true"))
	require.NoError(t, params.Save(params.CommonCfg.UseVectorAsClusteringKey.Key, "true"))
	defer params.Reset(params.CommonCfg.EnableVectorClusteringKey.Key)
	defer params.Reset(params.CommonCfg.UseVectorAsClusteringKey.Key)

	for _, dataType := range []schemapb.DataType{
		schemapb.DataType_FloatVector,
		schemapb.DataType_Float16Vector,
		schemapb.DataType_BFloat16Vector,
	} {
		t.Run(dataType.String(), func(t *testing.T) {
			field := &schemapb.FieldSchema{FieldID: 100, DataType: dataType}
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}}
			assert.Same(t, field, GetClusteringKeyField(schema))
		})
	}
}
