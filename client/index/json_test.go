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

package index

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/common"
)

func TestJsonPathIndex(t *testing.T) {
	// for unit test logic only, supported types are determined in milvus server.
	indexTypes := []IndexType{Inverted, BITMAP, Sorted}
	jsonCastTypes := []string{"double", "int", "varchar"}
	jsonPath := fmt.Sprintf("fieldName['%d']", rand.Intn(100))
	indexName := fmt.Sprintf("idx_%d", rand.Intn(100))

	indexType := indexTypes[rand.Intn(len(indexTypes))]
	castType := jsonCastTypes[rand.Intn(len(jsonCastTypes))]

	idx := NewJsonPathIndex(indexType, castType, jsonPath).WithIndexName(indexName)
	assert.Equal(t, indexType, idx.IndexType())
	assert.Equal(t, indexName, idx.Name())

	params := idx.Params()
	assert.Equal(t, castType, params[common.JSONCastTypeKey])
	assert.Equal(t, jsonPath, params[common.JSONPathKey])
}
