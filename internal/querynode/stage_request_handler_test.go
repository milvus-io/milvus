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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
)

func genRequestStage(ctx context.Context, t *testing.T) *requestHandlerStage {
	his, err := genSimpleHistorical(ctx)
	assert.NoError(t, err)
	str, err := genSimpleStreaming(ctx)
	assert.NoError(t, err)

	inputChan := make(chan queryMsg, queryBufferSize)
	hisOutput := make(chan queryMsg, queryBufferSize)
	var streamingOutput sync.Map

	resultStream, err := genQueryMsgStream(ctx)
	assert.NoError(t, err)
	reqStage := newRequestHandlerStage(ctx,
		defaultCollectionID,
		inputChan,
		hisOutput,
		&streamingOutput,
		str,
		his,
		resultStream)
	return reqStage
}

func TestRequestHandlerStage_TestSearch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reqStage := genRequestStage(ctx, t)
	go reqStage.start()

	// construct searchMsg
	sm2, err := genSimpleSearchMsg()
	assert.NoError(t, err)

	go func() {
		reqStage.input <- sm2
	}()

	res := <-reqStage.historicalOutput
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

func TestRequestHandlerStage_TestRetrieve(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reqStage := genRequestStage(ctx, t)
	go reqStage.start()

	// construct retrieveMsg
	sm2, err := genSimpleRetrieveMsg()
	assert.NoError(t, err)

	go func() {
		reqStage.input <- sm2
	}()

	res := <-reqStage.historicalOutput
	sm, ok := res.(*retrieveMsg)
	assert.True(t, ok)
	assert.Equal(t, defaultCollectionID, sm.CollectionID)
	assert.Equal(t, 1, len(sm.PartitionIDs))
	assert.Equal(t, defaultPartitionID, sm.PartitionIDs[0])
	assert.NotNil(t, sm.plan)
	sm.plan.delete()
}

func TestRequestHandlerStage_Invalid(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("test invalid collectionID", func(t *testing.T) {
		reqStage := genRequestStage(ctx, t)
		go reqStage.start()
		// construct retrieveMsg
		msg, err := genSimpleRetrieveMsg()
		assert.NoError(t, err)
		msg.CollectionID = UniqueID(1000)
		go func() {
			reqStage.input <- msg
		}()
		time.Sleep(20 * time.Millisecond)
	})

	t.Run("test no collection", func(t *testing.T) {
		reqStage := genRequestStage(ctx, t)
		go reqStage.start()
		// construct retrieveMsg
		msg, err := genSimpleSearchMsg()
		assert.NoError(t, err)

		err = reqStage.historical.replica.removeCollection(defaultCollectionID)
		assert.NoError(t, err)

		go func() {
			reqStage.input <- msg
		}()
		time.Sleep(20 * time.Millisecond)
	})

	t.Run("test invalid message type", func(t *testing.T) {
		reqStage := genRequestStage(ctx, t)
		go reqStage.start()
		// construct retrieveMsg
		msg, err := genSimpleRetrieveMsg()
		assert.NoError(t, err)
		msg.Base.MsgType = commonpb.MsgType_CreateCollection
		go func() {
			reqStage.input <- msg
		}()
		time.Sleep(20 * time.Millisecond)
	})

	t.Run("test collection release time", func(t *testing.T) {
		reqStage := genRequestStage(ctx, t)
		go reqStage.start()
		// construct retrieveMsg
		msg, err := genSimpleRetrieveMsg()
		assert.NoError(t, err)

		msg.GuaranteeTimestamp = Timestamp(0)

		col, err := reqStage.historical.replica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)
		col.setReleaseTime(Timestamp(0))

		go func() {
			reqStage.input <- msg
		}()
		time.Sleep(20 * time.Millisecond)
	})

	t.Run("test invalid retrieve plan", func(t *testing.T) {
		reqStage := genRequestStage(ctx, t)
		go reqStage.start()
		// construct retrieveMsg
		msg, err := genSimpleRetrieveMsg()
		assert.NoError(t, err)

		err = reqStage.streaming.replica.removeCollection(defaultCollectionID)
		assert.NoError(t, err)

		go func() {
			reqStage.input <- msg
		}()
		time.Sleep(20 * time.Millisecond)
	})

	t.Run("test invalid search plan", func(t *testing.T) {
		reqStage := genRequestStage(ctx, t)
		go reqStage.start()
		// construct retrieveMsg
		msg, err := genSimpleSearchMsg()
		assert.NoError(t, err)

		err = reqStage.streaming.replica.removeCollection(defaultCollectionID)
		assert.NoError(t, err)

		go func() {
			reqStage.input <- msg
		}()
		time.Sleep(20 * time.Millisecond)
	})
}
