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

package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestRTREEChecker(t *testing.T) {
	c := newRTREEChecker()

	t.Run("valid data type", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			DataType: schemapb.DataType_Geometry,
		}
		err := c.CheckValidDataType(IndexRTREE, field)
		assert.NoError(t, err)
	})

	t.Run("invalid data type", func(t *testing.T) {
		field := &schemapb.FieldSchema{
			DataType: schemapb.DataType_VarChar,
		}
		err := c.CheckValidDataType(IndexRTREE, field)
		assert.Error(t, err)
	})

	t.Run("non-geometry data type", func(t *testing.T) {
		params := make(map[string]string)
		err := c.CheckTrain(schemapb.DataType_VarChar, schemapb.DataType_None, params)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "RTREE index can only be built on geometry field")
	})
}
