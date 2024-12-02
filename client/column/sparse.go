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

package column

import (
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
)

var _ (Column) = (*ColumnSparseFloatVector)(nil)

type ColumnSparseFloatVector struct {
	*vectorBase[entity.SparseEmbedding]
}

func NewColumnSparseVectors(name string, values []entity.SparseEmbedding) *ColumnSparseFloatVector {
	return &ColumnSparseFloatVector{
		// sparse embedding need not specify dimension
		vectorBase: newVectorBase(name, 0, values, entity.FieldTypeSparseVector),
	}
}

func (c *ColumnSparseFloatVector) FieldData() *schemapb.FieldData {
	fd := c.vectorBase.FieldData()
	max := lo.MaxBy(c.values, func(a, b entity.SparseEmbedding) bool {
		return a.Dim() > b.Dim()
	})
	vectors := fd.GetVectors()
	vectors.Dim = int64(max.Dim())
	return fd
}

func (c *ColumnSparseFloatVector) Slice(start, end int) Column {
	return &ColumnSparseFloatVector{
		vectorBase: c.vectorBase.slice(start, end),
	}
}
