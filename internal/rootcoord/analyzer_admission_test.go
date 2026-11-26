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

package rootcoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestAnalyzerAdmissionDoesNotRecheckExistingTLS(t *testing.T) {
	paramtable.Init()
	old := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{
		{Key: "enable_analyzer", Value: "true"},
		{Key: "analyzer_params", Value: `{"tokenizer":{"type":"grpc","endpoint":"https://host","tls":{"ca_cert":"/legacy/ca.pem"}}}`},
	}}}}
	core := &Core{} // unchanged schema must never call the validation RPC
	require.NoError(t, core.validateChangedAnalyzers(context.Background(), proto.Clone(old).(*schemapb.CollectionSchema), old))
	next := proto.Clone(old).(*schemapb.CollectionSchema)
	next.Fields[0].TypeParams[1].Value = `{"tokenizer":{"type":"grpc","endpoint":"https://host","tls":{"ca_cert":"new.pem"}}}`
	coordinator := mocks.NewMixCoord(t)
	coordinator.EXPECT().ValidateAnalyzer(mock.Anything, mock.MatchedBy(func(req *querypb.ValidateAnalyzerRequest) bool {
		require.Len(t, req.AnalyzerInfos, 1)
		require.Contains(t, req.AnalyzerInfos[0].Params, "/new.pem")
		return true
	})).Return(&querypb.ValidateAnalyzerResponse{Status: merr.Status(merr.ErrParameterInvalid)}, nil).Once()
	core.mixCoord = coordinator
	require.Error(t, core.validateChangedAnalyzers(context.Background(), next, old))
}

func TestAnalyzerAdmissionPreservesIgnoredLegacyMultiParams(t *testing.T) {
	paramtable.Init()
	for _, multi := range []string{"invalid JSON", `{"by_field":"text","analyzers":{"default":{"tokenizer":"standard"}}}`} {
		t.Run(multi, func(t *testing.T) {
			previous := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
				Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{
					{Key: "enable_analyzer", Value: "true"},
					{Key: "analyzer_params", Value: `{"tokenizer":{"type":"grpc","endpoint":"https://host","tls":{"ca_cert":"/legacy/ca.pem"}}}`},
					{Key: "multi_analyzer_params", Value: multi},
				},
			}}}
			core := &Core{}
			unchanged := proto.Clone(previous).(*schemapb.CollectionSchema)
			unchanged.Fields[0].Description = "updated description"
			unchanged.Fields = append(unchanged.Fields, &schemapb.FieldSchema{Name: "other", DataType: schemapb.DataType_Int64})
			require.NoError(t, core.validateChangedAnalyzers(context.Background(), unchanged, previous))

			repaired := proto.Clone(previous).(*schemapb.CollectionSchema)
			repaired.Fields[0].TypeParams = repaired.Fields[0].TypeParams[:2]
			require.NoError(t, core.validateChangedAnalyzers(context.Background(), repaired, previous))
			require.Error(t, core.validateChangedAnalyzers(context.Background(), proto.Clone(previous).(*schemapb.CollectionSchema), nil))

			activated := proto.Clone(previous).(*schemapb.CollectionSchema)
			activated.Fields[0].TypeParams = append(activated.Fields[0].TypeParams, &commonpb.KeyValuePair{Key: "enable_match", Value: "true"})
			require.Error(t, core.validateChangedAnalyzers(context.Background(), activated, previous))

			coordinator := mocks.NewMixCoord(t)
			coordinator.EXPECT().ValidateAnalyzer(mock.Anything, mock.MatchedBy(func(req *querypb.ValidateAnalyzerRequest) bool {
				require.Len(t, req.AnalyzerInfos, 1)
				require.Contains(t, req.AnalyzerInfos[0].Params, "/new.pem")
				return true
			})).Return(&querypb.ValidateAnalyzerResponse{Status: merr.Status(merr.ErrParameterInvalid)}, nil).Once()
			core.mixCoord = coordinator
			repaired.Fields[0].TypeParams[1].Value = `{"tokenizer":{"type":"grpc","endpoint":"https://host","tls":{"ca_cert":"new.pem"}}}`
			require.Error(t, core.validateChangedAnalyzers(context.Background(), repaired, previous))
		})
	}
}

func TestAnalyzerAdmissionValidatesOnlyChangedNamedAnalyzer(t *testing.T) {
	paramtable.Init()
	previous := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{{Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{
			{Key: "enable_analyzer", Value: "true"},
			{Key: "multi_analyzer_params", Value: `{"by_field":"text","analyzers":{"default":{"tokenizer":"standard"},"custom":{"tokenizer":"whitespace"}}}`},
		}}},
		Functions: []*schemapb.FunctionSchema{{Type: schemapb.FunctionType_BM25, InputFieldNames: []string{"text"}}},
	}
	next := proto.Clone(previous).(*schemapb.CollectionSchema)
	next.Fields[0].TypeParams[1].Value = `{"by_field":"text","analyzers":{"default":{"tokenizer":"standard"},"custom":{"tokenizer":"jieba"}}}`
	coordinator := mocks.NewMixCoord(t)
	coordinator.EXPECT().ValidateAnalyzer(mock.Anything, mock.MatchedBy(func(req *querypb.ValidateAnalyzerRequest) bool {
		require.Len(t, req.AnalyzerInfos, 1)
		require.Equal(t, "custom", req.AnalyzerInfos[0].Name)
		return true
	})).Return(&querypb.ValidateAnalyzerResponse{Status: merr.Success()}, nil).Once()
	core := &Core{mixCoord: coordinator}
	require.NoError(t, core.validateChangedAnalyzers(context.Background(), next, previous))
}

func TestAnalyzerAdmissionChecksNewConsumers(t *testing.T) {
	paramtable.Init()
	for _, consumer := range []string{"match", "bm25"} {
		t.Run(consumer, func(t *testing.T) {
			previous := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
				Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{
					{Key: "enable_analyzer", Value: "true"},
					{Key: "analyzer_params", Value: `{"tokenizer":{"type":"grpc","endpoint":"https://host","tls":{"ca_cert":"legacy.pem"}}}`},
				},
			}}}
			next := proto.Clone(previous).(*schemapb.CollectionSchema)
			if consumer == "match" {
				next.Fields[0].TypeParams = append(next.Fields[0].TypeParams, &commonpb.KeyValuePair{Key: "enable_match", Value: "true"})
			} else {
				next.Functions = []*schemapb.FunctionSchema{{Type: schemapb.FunctionType_BM25, InputFieldNames: []string{"text"}}}
			}
			coordinator := mocks.NewMixCoord(t)
			coordinator.EXPECT().ValidateAnalyzer(mock.Anything, mock.MatchedBy(func(req *querypb.ValidateAnalyzerRequest) bool {
				require.Len(t, req.AnalyzerInfos, 1)
				require.Contains(t, req.AnalyzerInfos[0].Params, "/legacy.pem")
				return true
			})).Return(&querypb.ValidateAnalyzerResponse{Status: merr.Status(merr.ErrParameterInvalid)}, nil).Once()
			core := &Core{mixCoord: coordinator}
			require.Error(t, core.validateChangedAnalyzers(context.Background(), next, previous))
		})
	}
}
