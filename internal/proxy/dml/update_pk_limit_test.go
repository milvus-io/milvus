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

package dml

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestPKUpdateSubmittedPKLimit(t *testing.T) {
	paramtable.Init()
	require.NoError(t, paramtable.Get().Save("proxy.pkUpdate.maxPkCount", "4"))
	t.Cleanup(func() { paramtable.Get().Reset("proxy.pkUpdate.maxPkCount") })
	for _, typ := range []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_VarChar} {
		for _, count := range []int{0, 3, 4, 5} {
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
				FieldID: 100, Name: "pk", DataType: typ, IsPrimaryKey: true,
				TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "32"}},
			}}}
			// All keys are equal: deduplication must not bypass the submitted cap.
			ids := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: make([]int64, count)}}}
			if typ == schemapb.DataType_VarChar {
				ids.IdField = &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: make([]string, count)}}
			}
			err := validateUpdatePKs(ids, schema)
			switch count {
			case 0:
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
			case 5:
				require.ErrorIs(t, err, merr.ErrParameterTooLarge)
			default:
				require.NoError(t, err)
			}
		}
	}
}
