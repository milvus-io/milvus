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

package dql

import (
	"context"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/function/highlight"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// HighlightTaskName is the task name for the post-search highlight task.
const HighlightTaskName = "Highlight"

// git highlight after search
type HighlightTask struct {
	baseTask
	Condition
	*querypb.GetHighlightRequest
	ctx               context.Context
	collectionName    string
	collectionID      typeutil.UniqueID
	dbName            string
	readSchemaVersion *int32

	result *querypb.GetHighlightResponse
}

func (t *HighlightTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *HighlightTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *HighlightTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *HighlightTask) Name() string {
	return HighlightTaskName
}

func (t *HighlightTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *HighlightTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *HighlightTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *HighlightTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *HighlightTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_Undefined
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *HighlightTask) PreExecute(ctx context.Context) error {
	return nil
}

func (t *HighlightTask) Execute(ctx context.Context) error {
	// Pin metadata to the search collection ID; a same-name recreation must not
	// redirect highlighting to a different collection's analyzer.
	info, err := t.GetMetaCache().GetCollectionInfo(ctx, t.dbName, "", t.collectionID)
	if err != nil {
		return err
	}
	if len(info.VChannels) == 0 {
		return merr.WrapErrServiceNotReadyMsg("collection has no analyzer channel")
	}
	result := &querypb.GetHighlightResponse{Status: merr.Success()}
	for _, task := range t.GetTasks() {
		texts := make([][]byte, len(task.GetTexts()))
		for i, text := range task.GetTexts() {
			texts[i] = []byte(text)
		}
		response, err := streaming.WAL().AnalyzerClient().RunAnalyzer(ctx, &streamingpb.StreamingNodeRunAnalyzerRequest{
			Placeholder: texts, WithDetail: true,
			Source: &streamingpb.StreamingNodeRunAnalyzerRequest_FieldAnalyzer{FieldAnalyzer: &streamingpb.StreamingFieldAnalyzer{
				CollectionId: t.collectionID, Vchannel: info.VChannels[0], FieldId: task.GetFieldId(), AnalyzerNames: task.GetAnalyzerNames(), ReadSchemaVersion: t.readSchemaVersion,
			}},
		})
		if err != nil {
			if status.IsCanceled(err) {
				return err
			}
			return merr.WrapErrServiceUnavailableErr(err, "highlight analyzer transport")
		}
		if err := merr.Error(response.GetStatus()); err != nil {
			return err
		}
		tokens := make([][]*milvuspb.AnalyzerToken, len(response.GetResults()))
		for i, analyzed := range response.GetResults() {
			tokens[i] = analyzed.GetTokens()
		}
		fragments, err := highlight.BuildFragments(task, t.GetTopks(), tokens)
		if err != nil {
			return err
		}
		result.Results = append(result.Results, fragments...)
	}
	t.result = result
	return nil
}

func (t *HighlightTask) PostExecute(ctx context.Context) error {
	return nil
}

// isIgnoreGrowing is used to check if the request should ignore growing
func isIgnoreGrowing(params []*commonpb.KeyValuePair) (bool, error) {
	for _, kv := range params {
		if kv.GetKey() == IgnoreGrowingKey {
			ignoreGrowing, err := strconv.ParseBool(kv.GetValue())
			if err != nil {
				return false, merr.WrapErrParameterInvalidMsg("parse ignore growing field failed")
			}
			return ignoreGrowing, nil
		}
	}
	return false, nil
}
