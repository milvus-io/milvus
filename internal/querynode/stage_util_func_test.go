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
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

func TestStageUtilFunc_PublishQueryResult(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := genQueryMsgStream(ctx)
	assert.NoError(t, err)
	defer stream.Close()

	queryResChannel := genQueryResultChannel()

	stream.AsProducer([]string{queryResChannel})
	stream.Start()

	resMsg := &msgstream.SearchResultMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{0},
		},
		SearchResults: internalpb.SearchResults{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_SearchResult,
			},
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		},
	}
	publishQueryResult(resMsg, stream)

	resStream, err := initConsumer(ctx, queryResChannel)
	assert.NoError(t, err)
	defer resStream.Close()

	res, err := consumeSimpleSearchResult(resStream)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, res.Status.ErrorCode)
	assert.Equal(t, commonpb.MsgType_SearchResult, res.SearchResults.Base.MsgType)
}

func TestStageUtilFunc_PublishFailedQueryResult(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := genQueryMsgStream(ctx)
	assert.NoError(t, err)
	defer stream.Close()

	queryResultChannel := genQueryResultChannel()

	stream.AsProducer([]string{queryResultChannel})
	stream.Start()

	sm, err := genSimpleSearchMsg()
	assert.NoError(t, err)
	errStr := "[query node unittest] example search failed error message"
	publishFailedQueryResult(sm, errStr, stream)
}
