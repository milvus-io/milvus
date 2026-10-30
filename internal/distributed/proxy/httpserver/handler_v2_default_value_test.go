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

package httpserver

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestIntegerDefaultsCreateAndAddField(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaAndLimitsEnabled.Key)

	for _, route := range []string{"create", "add"} {
		for _, test := range []struct {
			literal string
			want    int64
			invalid bool
		}{
			{"9007199254740993", 9007199254740993, false},
			{"9223372036854775807", 9223372036854775807, false},
			{"-9223372036854775808", -9223372036854775808, false},
			{"1.9", 0, true},
			{"9223372036854775808", 0, true},
			{"-9223372036854775809", 0, true},
			{"1e9999999999999999999999999", 0, true},
			{"1e-9999999999999999999999999", 0, true},
			{"0e9999999999999999999999999", 0, false},
		} {
			t.Run(route+"/"+test.literal, func(t *testing.T) {
				mp := mocks.NewMockProxy(t)
				fieldJSON := fmt.Sprintf(`{"fieldName":"number","dataType":"Int64","nullable":true,"defaultValue":%s}`, test.literal)
				path := versionalV2(CollectionFieldCategory, AddAction)
				body := `{"collectionName":"book","schema":` + fieldJSON + `}`
				if route == "create" {
					path = versionalV2(CollectionCategory, CreateAction)
					body = `{"collectionName":"book","schema":{"fields":[` +
						`{"fieldName":"id","dataType":"Int64","isPrimary":true},` +
						`{"fieldName":"vector","dataType":"FloatVector","elementTypeParams":{"dim":4}},` + fieldJSON + `]}}`
					if !test.invalid {
						mp.EXPECT().CreateCollection(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, req *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
							var schema schemapb.CollectionSchema
							require.NoError(t, proto.Unmarshal(req.GetSchema(), &schema))
							require.Len(t, schema.GetFields(), 3)
							assert.Equal(t, test.want, schema.GetFields()[2].GetDefaultValue().GetLongData())
							return merr.Success(), nil
						}).Once()
					}
				} else if !test.invalid {
					mp.EXPECT().AlterCollectionSchema(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, req *milvuspb.AlterCollectionSchemaRequest) (*milvuspb.AlterCollectionSchemaResponse, error) {
						fields := req.GetAction().GetAddRequest().GetFieldInfos()
						require.Len(t, fields, 1)
						assert.Equal(t, test.want, fields[0].GetFieldSchema().GetDefaultValue().GetLongData())
						return &milvuspb.AlterCollectionSchemaResponse{AlterStatus: merr.Success()}, nil
					}).Once()
				}
				engine := initHTTPServerV2(mp, false)
				req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
				req.Header.Set("Content-Type", "application/json")
				w := httptest.NewRecorder()
				engine.ServeHTTP(w, req)
				assert.Equal(t, http.StatusOK, w.Code)
				var response ReturnErrMsg
				require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
				if test.invalid {
					assert.Equal(t, merr.Code(merr.ErrParameterInvalid), response.Code)
					assert.Contains(t, response.Message, "exact integer")
				} else {
					assert.Zero(t, response.Code)
				}
			})
		}
	}
}
