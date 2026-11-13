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

package ddl

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func newTextNgramTask(extraParams []*commonpb.KeyValuePair) *CreateIndexTask {
	return &CreateIndexTask{
		req: &milvuspb.CreateIndexRequest{
			Base: &commonpb.MsgBase{}, CollectionName: "docs", FieldName: "body",
			IndexName: "body_ngram", ExtraParams: extraParams,
		},
		collectionID: 1,
		fieldSchema: &schemapb.FieldSchema{
			FieldID: 101, Name: "body", DataType: schemapb.DataType_Text, Nullable: true,
		},
	}
}

func TestTextNgramIndexAdmission(t *testing.T) {
	for _, enabled := range []string{"false", "true"} {
		t.Run("scalarAutoIndex="+enabled, func(t *testing.T) {
			cfg := &paramtable.Get().AutoIndexConfig
			previous := cfg.ScalarAutoIndexEnable.SwapTempValue(enabled)
			t.Cleanup(func() { cfg.ScalarAutoIndexEnable.SwapTempValue(previous) })
			for _, tc := range []struct {
				name, indexType, params string
				omitType                bool
				option                  *commonpb.KeyValuePair
				wantErr                 error
			}{
				{name: "explicit", indexType: "NGRAM", params: `{"min_gram":2,"max_gram":3}`},
				{name: "mmap", indexType: "NGRAM", params: `{"min_gram":2,"max_gram":3}`, option: &commonpb.KeyValuePair{Key: common.MmapEnabledKey, Value: "true"}},
				{name: "warmup", indexType: "NGRAM", params: `{"min_gram":2,"max_gram":3}`, option: &commonpb.KeyValuePair{Key: common.WarmupKey, Value: "disable"}},
				{name: "omitted type with grams", omitType: true, params: `{"min_gram":2,"max_gram":3}`, wantErr: merr.ErrParameterInvalid},
				{name: "empty type", wantErr: merr.ErrParameterInvalid},
				{name: "auto", indexType: AutoIndexName, wantErr: merr.ErrParameterInvalid},
				{name: "inverted", indexType: "INVERTED", wantErr: merr.ErrParameterInvalid},
				{name: "hybrid", indexType: "HYBRID", wantErr: merr.ErrParameterInvalid},
				{name: "fmindex", indexType: "FMINDEX", wantErr: merr.ErrParameterInvalid},
				{name: "missing grams", indexType: "NGRAM", wantErr: merr.ErrParameterMissing},
				{name: "missing max", indexType: "NGRAM", params: `{"min_gram":2}`, wantErr: merr.ErrParameterMissing},
				{name: "invalid grams", indexType: "NGRAM", params: `{"min_gram":5,"max_gram":3}`, wantErr: merr.ErrParameterInvalid},
				{name: "invalid mmap", indexType: "NGRAM", params: `{"min_gram":2,"max_gram":3}`, option: &commonpb.KeyValuePair{Key: common.MmapEnabledKey, Value: "invalid"}, wantErr: merr.ErrParameterInvalid},
				{name: "invalid warmup", indexType: "NGRAM", params: `{"min_gram":2,"max_gram":3}`, option: &commonpb.KeyValuePair{Key: common.WarmupKey, Value: "invalid"}, wantErr: merr.ErrParameterInvalid},
			} {
				t.Run(tc.name, func(t *testing.T) {
					var extra []*commonpb.KeyValuePair
					if !tc.omitType {
						extra = append(extra, &commonpb.KeyValuePair{Key: common.IndexTypeKey, Value: tc.indexType})
					}
					if tc.params != "" {
						extra = append(extra, &commonpb.KeyValuePair{Key: common.ParamsKey, Value: tc.params})
					}
					if tc.option != nil {
						extra = append(extra, tc.option)
					}
					task := newTextNgramTask(extra)
					err := task.parseIndexParams(context.Background())
					if tc.wantErr != nil {
						require.ErrorIs(t, err, tc.wantErr)
						return
					}
					require.NoError(t, err)
					require.False(t, task.isAutoIndex)
					require.Equal(t, "NGRAM", funcutil.KeyValuePair2Map(task.newIndexParams)[common.IndexTypeKey])
					require.Equal(t, schemapb.DataType_Text, task.fieldSchema.GetDataType())
				})
			}
		})
	}
}

func TestTextNgramIndexRequestAndDescription(t *testing.T) {
	for _, enabled := range []string{"false", "true"} {
		for _, policy := range []string{"HYBRID", "INVERTED"} {
			for _, wrapped := range []bool{false, true} {
				t.Run(enabled+"/"+policy+"/wrapped="+strconv.FormatBool(wrapped), func(t *testing.T) {
					cfg := &paramtable.Get().AutoIndexConfig
					oldEnable := cfg.ScalarAutoIndexEnable.SwapTempValue(enabled)
					oldPolicy := cfg.ScalarAutoIndexParams.SwapTempValue(`{"varchar":"` + policy + `"}`)
					t.Cleanup(func() {
						cfg.ScalarAutoIndexEnable.SwapTempValue(oldEnable)
						cfg.ScalarAutoIndexParams.SwapTempValue(oldPolicy)
					})
					extra := []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "NGRAM"}}
					if wrapped {
						extra = append(extra, &commonpb.KeyValuePair{Key: common.ParamsKey, Value: `{"min_gram":2,"max_gram":4}`})
					} else {
						extra = append(extra, &commonpb.KeyValuePair{Key: indexparamcheck.MinGramKey, Value: "2"}, &commonpb.KeyValuePair{Key: indexparamcheck.MaxGramKey, Value: "4"})
					}
					ctx := context.Background()
					task := newTextNgramTask(extra)
					require.NoError(t, task.parseIndexParams(ctx))
					var sent *indexpb.CreateIndexRequest
					coord := mocks.NewMockMixCoordClient(t)
					coord.On("CreateIndex", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
						sent = args.Get(1).(*indexpb.CreateIndexRequest)
					}).Return(merr.Success(), nil).Once()
					task.mixCoord = coord
					require.NoError(t, task.Execute(ctx))
					require.NotNil(t, sent)
					require.False(t, sent.GetIsAutoIndex())
					require.Equal(t, int64(101), sent.GetFieldID())
					require.Equal(t, map[string]string{common.IndexTypeKey: "NGRAM", indexparamcheck.MinGramKey: "2", indexparamcheck.MaxGramKey: "4"}, funcutil.KeyValuePair2Map(sent.GetIndexParams()))
					require.Equal(t, extra, sent.GetUserIndexParams())

					// Describe uses the persisted definition, even after the
					// automatic policy changes since the CreateIndex request.
					cfg.ScalarAutoIndexEnable.SwapTempValue("true")
					cfg.ScalarAutoIndexParams.SwapTempValue(`{"varchar":"BITMAP"}`)
					coord.On("DescribeIndex", mock.Anything, mock.Anything).Return(&indexpb.DescribeIndexResponse{
						Status: merr.Success(),
						IndexInfos: []*indexpb.IndexInfo{{
							FieldID: sent.GetFieldID(), IndexName: sent.GetIndexName(),
							IsAutoIndex: sent.GetIsAutoIndex(), IndexParams: sent.GetIndexParams(), UserIndexParams: sent.GetUserIndexParams(),
						}},
					}, nil).Once()
					cache := NewMockCache(t)
					cache.On("GetCollectionSchema", mock.Anything, "", "docs").Return(mustNewSchemaInfo(&schemapb.CollectionSchema{
						Name: "docs", Fields: []*schemapb.FieldSchema{
							{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}, task.fieldSchema,
						},
					}), nil).Once()
					describe := &DescribeIndexTask{
						baseTask: baseTask{MetaCache: cache}, mixCoord: coord, collectionID: 1,
						DescribeIndexRequest: &milvuspb.DescribeIndexRequest{CollectionName: "docs", IndexName: "body_ngram"},
					}
					require.NoError(t, describe.Execute(ctx))
					require.Len(t, describe.result.GetIndexDescriptions(), 1)
					params, err := indexparamcheck.ExpandIndexParams(describe.result.GetIndexDescriptions()[0].GetParams())
					require.NoError(t, err)
					require.Equal(t, funcutil.KeyValuePair2Map(sent.GetIndexParams()), params)
				})
			}
		}
	}
}

func TestVarcharNgramAutoIndexPolicyUnchanged(t *testing.T) {
	for _, enabled := range []string{"false", "true"} {
		t.Run(enabled, func(t *testing.T) {
			cfg := &paramtable.Get().AutoIndexConfig
			oldEnable := cfg.ScalarAutoIndexEnable.SwapTempValue(enabled)
			oldPolicy := cfg.ScalarAutoIndexParams.SwapTempValue(`{"varchar":"INVERTED"}`)
			t.Cleanup(func() {
				cfg.ScalarAutoIndexEnable.SwapTempValue(oldEnable)
				cfg.ScalarAutoIndexParams.SwapTempValue(oldPolicy)
			})
			for _, kind := range []string{"NGRAM", AutoIndexName, "omitted"} {
				t.Run(kind, func(t *testing.T) {
					extra := []*commonpb.KeyValuePair{{Key: common.ParamsKey, Value: `{"min_gram":2,"max_gram":3}`}}
					if kind != "omitted" {
						extra = append(extra, &commonpb.KeyValuePair{Key: common.IndexTypeKey, Value: kind})
					}
					task := newTextNgramTask(extra)
					task.fieldSchema.DataType = schemapb.DataType_VarChar
					require.NoError(t, task.parseIndexParams(context.Background()))
					automatic := enabled == "true" || kind != "NGRAM"
					require.Equal(t, automatic, task.isAutoIndex)
					want := "NGRAM"
					if automatic {
						want = "INVERTED"
					}
					require.Equal(t, want, funcutil.KeyValuePair2Map(task.newIndexParams)[common.IndexTypeKey])
				})
			}
		})
	}
}
