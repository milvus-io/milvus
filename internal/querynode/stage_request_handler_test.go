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
)

func TestRequestHandlerStage_RequestHandlerStage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	his := genSimpleHistorical(ctx)
	s := genSimpleStreaming(ctx)

	inputChan := make(chan queryMsg, queryBufferSize)
	hisOutput := make(chan queryMsg, queryBufferSize)
	streamingOutput := make(map[Channel]chan queryMsg)

	resultStream := genQueryMsgStream(ctx)
	reqStage := newRequestHandlerStage(ctx,
		defaultCollectionID,
		inputChan,
		hisOutput,
		streamingOutput,
		s,
		his,
		resultStream)
	go reqStage.start()

	// construct searchMsg
	sm2 := genSimpleSearchMsg()

	go func() {
		inputChan <- sm2
	}()

	res := <-hisOutput
	sm, ok := res.(*searchMsg)
	assert.True(t, ok)
	assert.Equal(t, defaultCollectionID, sm.CollectionID)
	assert.Equal(t, 1, len(sm.PartitionIDs))
	assert.Equal(t, defaultPartitionID, sm.PartitionIDs[0])
	assert.NotNil(t, sm.plan)
	assert.NotNil(t, sm.reqs)
	sm.plan.delete()
	for _, req := range sm.reqs {
		req.delete()
	}
}
