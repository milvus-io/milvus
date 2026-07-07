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

package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestInsertNodeVerifyPayloadFields(t *testing.T) {
	paramtable.Init()
	iNode := &insertNode{channel: "by-dev-rootcoord-dml_0_100v0"}

	eraSchema := &schemapb.CollectionSchema{
		Version: 104,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100},
			{FieldID: 101},
			{FieldID: 158}, // dropped from the current schema since v104
		},
	}
	eraFields := typeutil.NewSet[int64](100, 101, 158)

	newMsg := func(fieldIDs ...int64) *InsertMsg {
		fieldsData := make([]*schemapb.FieldData, 0, len(fieldIDs))
		for _, id := range fieldIDs {
			fieldsData = append(fieldsData, &schemapb.FieldData{FieldId: id})
		}
		return &InsertMsg{
			BaseMsg: msgstream.BaseMsg{EndTimestamp: 1000},
			InsertRequest: &msgpb.InsertRequest{
				SegmentID:  1,
				FieldsData: fieldsData,
			},
		}
	}

	// payload within its era schema, including a since-dropped field: valid.
	assert.NoError(t, iNode.verifyPayloadFields(newMsg(100, 101, 158), eraSchema, eraFields))

	// payload carries a field absent from its own era schema: corrupted.
	err := iNode.verifyPayloadFields(newMsg(100, 159), eraSchema, eraFields)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "corrupted")
}
