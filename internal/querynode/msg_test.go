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

package querynode

import (
	"testing"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/stretchr/testify/assert"
)

func TestMsg_canMerge(t *testing.T) {
	a, err1 := genSimpleRetrieveMsg()
	assert.NoError(t, err1)

	b, err2 := genSimpleSearchMsg()
	assert.NoError(t, err2)
	ok := canMerge(a, b)
	assert.Equal(t, false, ok)

	msg1, err1 := genSimpleSearchMsg()
	assert.NoError(t, err1)
	msg2, err2 := genSimpleSearchMsg()
	assert.NoError(t, err2)

	ok = canMerge(msg1, msg2)
	assert.Equal(t, true, ok)

	msg1.DbID = 123
	ok = canMerge(msg1, msg2)
	assert.Equal(t, false, ok)

	msg1.DbID = msg2.DbID
	msg1.CollectionID = 1133223

	ok = canMerge(msg1, msg2)
	assert.Equal(t, false, ok)
	msg1.CollectionID = msg2.CollectionID

	msg1.DslType = commonpb.DslType_BoolExprV1
	ok = canMerge(msg1, msg2)
	assert.Equal(t, false, ok)
	msg1.DslType = msg2.DslType

	msg1.MetricType = "abc"
	ok = canMerge(msg1, msg2)
	assert.Equal(t, false, ok)
	msg1.MetricType = msg2.MetricType

	msg1.PartitionIDs = []UniqueID{4, 5, 6}
	ok = canMerge(msg1, msg2)
	assert.Equal(t, false, ok)
	msg1.PartitionIDs = msg2.PartitionIDs

	msg1.SerializedExprPlan = []byte{byte('a'), byte('b'), byte('c')}
	ok = canMerge(msg1, msg2)
	assert.Equal(t, false, ok)
	msg1.SerializedExprPlan = msg2.SerializedExprPlan

	msg1.TravelTimestamp = 1
	ok = canMerge(msg1, msg2)
	assert.Equal(t, false, ok)

	msg1.TravelTimestamp = msg2.TravelTimestamp
	ok = canMerge(msg1, msg2)
	assert.Equal(t, true, ok)

	msg1.NQ = 1000
	msg1.TopK = 0
	msg1.OrigNQs = nil
	maxTopK := int64(0)
	for i := 0; i < 100; i++ {
		topK := int64(i % 10)
		if maxTopK < topK {
			maxTopK = topK
		}
		msg1.OrigNQs = append(msg1.OrigNQs, 1)
		msg1.OrigTopKs = append(msg1.OrigTopKs, int64(i%10))
	}
	msg1.TopK = maxTopK

	msg2.NQ = 10
	msg2.TopK = 20

	ok = canMerge(msg1, msg2)
	assert.Equal(t, true, ok)

}

func TestMsg_MergeRequest(t *testing.T) {

	msg1, err1 := genSimpleSearchMsg()
	assert.NoError(t, err1)
	msg2, err2 := genSimpleSearchMsg()
	assert.NoError(t, err2)

	msg1.NQ = 100
	msg1.OrigNQs = nil
	maxTopK := int64(0)
	for i := 0; i < 100; i++ {
		topK := int64(i % 10)
		if maxTopK < topK {
			maxTopK = topK
		}
		msg1.OrigNQs = append(msg1.OrigNQs, 1)
		msg1.OrigTopKs = append(msg1.OrigTopKs, int64(i%10))
	}
	oldNq := msg1.NQ
	msg1.TopK = maxTopK

	msg2.NQ = 10
	msg2.TopK = 20
	ok := canMerge(msg1, msg2)
	assert.Equal(t, true, ok)

	mergeMsg := mergeSearchMsg(msg1, msg2)
	mergeSearchMsg, _ := mergeMsg.(*searchMsg)
	assert.Equal(t, mergeSearchMsg.NQ, msg2.NQ+oldNq)
	assert.Equal(t, mergeSearchMsg.TopK, msg2.TopK)
	assert.Equal(t, mergeSearchMsg.MetricType, msg2.MetricType)

}
