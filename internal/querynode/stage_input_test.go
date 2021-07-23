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
	"github.com/milvus-io/milvus/internal/proto/commonpb"
)

func TestInputStage_InputStage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lbOutput := make(chan *msgstream.LoadBalanceSegmentsMsg, queryBufferSize)
	queryOutput := make(chan queryMsg, queryBufferSize)

	stream := genQueryMsgStream(ctx)
	stream.AsConsumer([]string{defaultQueryChannel}, defaultSubName)
	stream.Start()
	defer stream.Close()

	iStage := newInputStage(ctx, cancel, defaultCollectionID, stream, lbOutput, queryOutput)
	go iStage.start()
	produceSimpleSearchMsg(ctx)

	msg := <-queryOutput
	assert.Equal(t, commonpb.MsgType_Search, msg.Type())
	sm, ok := msg.(*msgstream.SearchMsg)
	assert.True(t, ok)
	assert.Equal(t, defaultCollectionID, sm.CollectionID)
	assert.Equal(t, 1, len(sm.PartitionIDs))
	assert.Equal(t, defaultPartitionID, sm.PartitionIDs[0])
}
