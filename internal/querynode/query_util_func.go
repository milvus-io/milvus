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
	"fmt"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/trace"
)

func publishQueryResult(msg msgstream.TsMsg, stream msgstream.MsgStream) {
	span, ctx := trace.StartSpanFromContext(msg.TraceCtx())
	defer span.Finish()
	msg.SetTraceCtx(ctx)
	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, msg)
	err := stream.Produce(&msgPack)
	if err != nil {
		log.Error(err.Error())
	}
}

func publishFailedQueryResult(msg msgstream.TsMsg, errMsg string, stream msgstream.MsgStream) {
	msgType := msg.Type()
	span, ctx := trace.StartSpanFromContext(msg.TraceCtx())
	defer span.Finish()
	msg.SetTraceCtx(ctx)
	msgPack := msgstream.MsgPack{}

	resultChannelInt := 0
	baseMsg := msgstream.BaseMsg{
		HashValues: []uint32{uint32(resultChannelInt)},
	}
	baseResult := &commonpb.MsgBase{
		MsgID:     msg.ID(),
		Timestamp: msg.BeginTs(),
		SourceID:  msg.SourceID(),
	}

	switch msgType {
	case commonpb.MsgType_Retrieve:
		retrieveMsg := msg.(*msgstream.RetrieveMsg)
		baseResult.MsgType = commonpb.MsgType_RetrieveResult
		retrieveResultMsg := &msgstream.RetrieveResultMsg{
			BaseMsg: baseMsg,
			RetrieveResults: internalpb.RetrieveResults{
				Base:            baseResult,
				Status:          &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: errMsg},
				ResultChannelID: retrieveMsg.ResultChannelID,
				Ids:             nil,
				FieldsData:      nil,
			},
		}
		msgPack.Msgs = append(msgPack.Msgs, retrieveResultMsg)
	case commonpb.MsgType_Search:
		searchMsg := msg.(*msgstream.SearchMsg)
		baseResult.MsgType = commonpb.MsgType_SearchResult
		searchResultMsg := &msgstream.SearchResultMsg{
			BaseMsg: baseMsg,
			SearchResults: internalpb.SearchResults{
				Base:            baseResult,
				Status:          &commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError, Reason: errMsg},
				ResultChannelID: searchMsg.ResultChannelID,
			},
		}
		msgPack.Msgs = append(msgPack.Msgs, searchResultMsg)
	default:
		log.Error(fmt.Errorf("publish invalid msgType %d", msgType).Error())
		return
	}

	err := stream.Produce(&msgPack)
	if err != nil {
		log.Error(err.Error())
	}
}
