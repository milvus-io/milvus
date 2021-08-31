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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
)

func genVChannelStage(ctx context.Context, t *testing.T) *vChannelStage {
	inputChan := make(chan queryMsg, queryBufferSize)
	resChan := make(chan queryResult, queryBufferSize)

	s, err := genSimpleStreaming(ctx)
	assert.NoError(t, err)

	vStage := newVChannelStage(ctx,
		defaultCollectionID,
		defaultVChannel,
		inputChan,
		resChan,
		s)
	return vStage
}

func TestVChannelStage_Search(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vStage := genVChannelStage(ctx, t)
	go vStage.start()

	// construct searchMsg
	searchReq, err := genSimpleSearchRequest()
	assert.NoError(t, err)
	plan, reqs, err := genSimpleSearchPlanAndRequests()
	assert.NoError(t, err)
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

	go func() {
		vStage.input <- msg
	}()
	res := <-vStage.queryOutput
	sr, ok := res.(*searchResult)
	assert.True(t, ok)
	assert.NoError(t, sr.err)
	assert.Equal(t, 0, len(sr.sealedSegmentSearched))
}

func TestVChannelStage_Retrieve(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vStage := genVChannelStage(ctx, t)
	go vStage.start()

	// construct retrieveMsg
	retrieveReq, err := genSimpleRetrieveRequest()
	assert.NoError(t, err)
	plan, err := genSimpleRetrievePlan()
	assert.NoError(t, err)
	msg := &retrieveMsg{
		RetrieveMsg: &msgstream.RetrieveMsg{
			BaseMsg: msgstream.BaseMsg{
				HashValues: []uint32{0},
			},
			RetrieveRequest: *retrieveReq,
		},
		plan: plan,
	}

	go func() {
		vStage.input <- msg
	}()
	res := <-vStage.queryOutput
	sr, ok := res.(*retrieveResult)
	assert.True(t, ok)
	assert.NoError(t, sr.err)
	assert.Equal(t, 0, len(sr.segmentRetrieved))
}

func TestVChannelStage_DoUnsolvedMsg(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("test serviceable search", func(t *testing.T) {
		// construct searchMsg
		searchReq, err := genSimpleSearchRequest()
		assert.NoError(t, err)
		plan, reqs, err := genSimpleSearchPlanAndRequests()
		assert.NoError(t, err)
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

		vStage := genVChannelStage(ctx, t)
		vStage.unsolvedMsg = append(vStage.unsolvedMsg, msg)

		vStage.doUnsolvedQueryMsg(Timestamp(1000))

		res := <-vStage.queryOutput
		sr, ok := res.(*searchResult)
		assert.True(t, ok)
		assert.NoError(t, sr.err)
		assert.Equal(t, 0, len(sr.sealedSegmentSearched))
	})

	t.Run("test unsolved search", func(t *testing.T) {
		// construct searchMsg
		searchReq, err := genSimpleSearchRequest()
		assert.NoError(t, err)
		plan, reqs, err := genSimpleSearchPlanAndRequests()
		assert.NoError(t, err)
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
		msg.GuaranteeTimestamp = Timestamp(1000)

		vStage := genVChannelStage(ctx, t)
		vStage.unsolvedMsg = append(vStage.unsolvedMsg, msg)

		vStage.doUnsolvedQueryMsg(0)
	})

	t.Run("test no collection", func(t *testing.T) {
		// construct searchMsg
		searchReq, err := genSimpleSearchRequest()
		assert.NoError(t, err)
		plan, reqs, err := genSimpleSearchPlanAndRequests()
		assert.NoError(t, err)
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

		vStage := genVChannelStage(ctx, t)
		err = vStage.streaming.replica.removeCollection(defaultCollectionID)
		assert.NoError(t, err)

		vStage.unsolvedMsg = append(vStage.unsolvedMsg, msg)
		vStage.doUnsolvedQueryMsg(0)
	})

	t.Run("test collection release time", func(t *testing.T) {
		// construct searchMsg
		searchReq, err := genSimpleSearchRequest()
		assert.NoError(t, err)
		plan, reqs, err := genSimpleSearchPlanAndRequests()
		assert.NoError(t, err)
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

		vStage := genVChannelStage(ctx, t)
		col, err := vStage.streaming.replica.getCollectionByID(defaultCollectionID)
		assert.NoError(t, err)
		col.setReleaseTime(Timestamp(0))

		vStage.unsolvedMsg = append(vStage.unsolvedMsg, msg)
		vStage.doUnsolvedQueryMsg(0)
	})

	t.Run("test execute failed", func(t *testing.T) {
		// construct searchMsg
		searchReq, err := genSimpleSearchRequest()
		assert.NoError(t, err)
		plan, reqs, err := genSimpleSearchPlanAndRequests()
		assert.NoError(t, err)

		searchReq.Base.MsgType = commonpb.MsgType_CreateCollection
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

		vStage := genVChannelStage(ctx, t)

		vStage.unsolvedMsg = append(vStage.unsolvedMsg, msg)
		vStage.doUnsolvedQueryMsg(0)
	})
}

func TestVChannelStage_QueryError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("test search", func(t *testing.T) {
		vStage := genVChannelStage(ctx, t)

		// construct searchMsg
		searchReq, err := genSimpleSearchRequest()
		assert.NoError(t, err)
		plan, reqs, err := genSimpleSearchPlanAndRequests()
		assert.NoError(t, err)
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

		vStage.queryError(msg, errors.New("test error"))

		res := <-vStage.queryOutput
		sr, ok := res.(*searchResult)
		assert.True(t, ok)
		assert.Error(t, sr.err)
	})

	t.Run("test retrieve", func(t *testing.T) {
		vStage := genVChannelStage(ctx, t)

		// construct retrieveMsg
		retrieveReq, err := genSimpleRetrieveRequest()
		assert.NoError(t, err)
		plan, err := genSimpleRetrievePlan()
		assert.NoError(t, err)
		msg := &retrieveMsg{
			RetrieveMsg: &msgstream.RetrieveMsg{
				BaseMsg: msgstream.BaseMsg{
					HashValues: []uint32{0},
				},
				RetrieveRequest: *retrieveReq,
			},
			plan: plan,
		}

		vStage.queryError(msg, errors.New("test error"))

		res := <-vStage.queryOutput
		sr, ok := res.(*retrieveResult)
		assert.True(t, ok)
		assert.Error(t, sr.err)
	})
}
