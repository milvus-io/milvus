// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package querynode

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/msgstream"
)

func TestVChannelStage_VChannelStage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	inputChan := make(chan queryMsg, queryBufferSize)
	unsolvedChan := make(chan queryMsg, queryBufferSize)
	resChan := make(chan queryResult, queryBufferSize)

	s := genSimpleStreaming(ctx)

	vStage := newVChannelStage(ctx,
		defaultCollectionID,
		defaultVChannel,
		inputChan,
		unsolvedChan,
		resChan,
		s)
	go vStage.start()

	// construct searchMsg
	searchReq := genSimpleSearchRequest()
	plan, reqs := genSimplePlanAndRequests()
	msg := &searchMsg{
		SearchMsg: &msgstream.SearchMsg{
			BaseMsg: msgstream.BaseMsg{
				HashValues: []uint32{0},
			},
			SearchRequest: *searchReq,
		},
		plan: plan,
		reqs: reqs,
	}

	// 1. output of resultResultHandlerStage
	go func() {
		inputChan <- msg
	}()
	res := <-resChan
	sr, ok := res.(*searchResult)
	assert.True(t, ok)
	assert.NoError(t, sr.err)
	assert.Equal(t, 1, len(sr.matchedSegments))
	assert.Equal(t, 1, len(sr.sealedSegmentSearched))
	assert.Equal(t, defaultSegmentID, sr.sealedSegmentSearched[0])

	// 2. output of unsolvedStage
	msg.GuaranteeTimestamp = Timestamp(1000)
	go func() {
		inputChan <- msg
	}()
	res2 := <-unsolvedChan
	sr2, ok := res2.(*searchMsg)
	assert.True(t, ok)
	assert.Equal(t, defaultCollectionID, sr2.CollectionID)
	assert.Equal(t, 1, len(sr2.PartitionIDs))
	assert.Equal(t, defaultPartitionID, sr2.PartitionIDs[0])
}
