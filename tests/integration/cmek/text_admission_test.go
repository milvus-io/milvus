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

package cmek

import (
	"context"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (s *RawDataV3Suite) TestTextAdmission() {
	for _, test := range []struct {
		name       string
		dbName     string
		fieldType  schemapb.DataType
		properties []*commonpb.KeyValuePair
		reject     bool
	}{
		{name: "encrypted_text", dbName: s.dbName, fieldType: schemapb.DataType_Text, reject: true},
		{name: "encrypted_text_cannot_override_ez", dbName: s.dbName, fieldType: schemapb.DataType_Text,
			properties: []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "0"}}, reject: true},
		{name: "encrypted_non_text", dbName: s.dbName, fieldType: schemapb.DataType_Int64},
		{name: "plain_text", dbName: "default", fieldType: schemapb.DataType_Text},
		{name: "plain_text_ignores_request_ez", dbName: "default", fieldType: schemapb.DataType_Text,
			properties: []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: strconv.FormatInt(s.ezID, 10)}}},
	} {
		s.Run(test.name, func() {
			ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), time.Minute)
			defer cancel()
			collection := "cmek_admission_" + funcutil.GenRandomStr()
			schema := &schemapb.CollectionSchema{
				Name: collection,
				Fields: []*schemapb.FieldSchema{
					{Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
					{Name: "payload", DataType: test.fieldType},
					{Name: "vector", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}}},
				},
			}
			encoded, err := proto.Marshal(schema)
			s.Require().NoError(err)
			status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
				DbName: test.dbName, CollectionName: collection, Schema: encoded, ShardsNum: 1, Properties: test.properties,
			})
			s.Require().NoError(err)
			if merr.Ok(status) {
				s.T().Cleanup(func() {
					dropped, err := s.Cluster.MilvusClient.DropCollection(context.Background(), &milvuspb.DropCollectionRequest{
						DbName: test.dbName, CollectionName: collection,
					})
					s.NoError(merr.CheckRPCCall(dropped, err))
				})
			}
			exists, err := s.Cluster.MilvusClient.HasCollection(ctx, &milvuspb.HasCollectionRequest{
				DbName: test.dbName, CollectionName: collection,
			})
			s.Require().NoError(merr.CheckRPCCall(exists, err))
			if test.reject {
				s.Equal(merr.Code(merr.ErrParameterInvalid), status.GetCode())
				s.Contains(status.GetReason(), "TEXT fields are not supported with CMEK")
				s.Equal("true", status.GetExtraInfo()["is_input_error"])
				s.False(status.GetRetriable())
				s.False(exists.GetValue(), "rejected collection must not exist")
				return
			}
			s.Require().NoError(merr.Error(status))
			s.Require().True(exists.GetValue())
			description, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
				DbName: test.dbName, CollectionName: collection,
			})
			s.Require().NoError(merr.CheckRPCCall(description, err))
			expectedEZ := ""
			if test.dbName == s.dbName {
				expectedEZ = strconv.FormatInt(s.ezID, 10)
			}
			s.Equal(expectedEZ, propertyValue(description.GetProperties(), common.EncryptionEzIDKey))
		})
	}
}
