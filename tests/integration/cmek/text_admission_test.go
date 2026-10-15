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
		{
			name: "encrypted_text_cannot_override_ez", dbName: s.dbName, fieldType: schemapb.DataType_Text,
			properties: []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "0"}}, reject: true,
		},
		{name: "encrypted_non_text", dbName: s.dbName, fieldType: schemapb.DataType_Int64},
		{name: "plain_text", dbName: "default", fieldType: schemapb.DataType_Text},
		{
			name: "plain_text_ignores_request_ez", dbName: "default", fieldType: schemapb.DataType_Text,
			properties: []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: strconv.FormatInt(s.ezID, 10)}},
		},
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

func (s *RawDataV3Suite) TestAddTextAdmission() {
	for _, operation := range []string{"AddCollectionField", "AlterCollectionSchema", "AlterCollectionSchemaWithBackfill"} {
		for _, test := range []struct {
			name      string
			dbName    string
			fieldType schemapb.DataType
			alias     bool
			reject    bool
		}{
			{name: "encrypted_text", dbName: s.dbName, fieldType: schemapb.DataType_Text, reject: true},
			{name: "encrypted_text_alias", dbName: s.dbName, fieldType: schemapb.DataType_Text, alias: true, reject: true},
			{name: "encrypted_non_text", dbName: s.dbName, fieldType: schemapb.DataType_Int64},
			{name: "plain_text", dbName: "default", fieldType: schemapb.DataType_Text},
		} {
			s.Run(operation+"/"+test.name, func() {
				ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), time.Minute)
				defer cancel()
				collection := "cmek_add_admission_" + funcutil.GenRandomStr()
				schema := &schemapb.CollectionSchema{
					Name: collection,
					Fields: []*schemapb.FieldSchema{
						{Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
						{Name: "vector", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}}},
					},
				}
				encoded, err := proto.Marshal(schema)
				s.Require().NoError(err)
				status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
					DbName: test.dbName, CollectionName: collection, Schema: encoded, ShardsNum: 1,
				})
				s.Require().NoError(merr.CheckRPCCall(status, err))
				s.T().Cleanup(func() {
					cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
					defer cleanupCancel()
					dropped, err := s.Cluster.MilvusClient.DropCollection(cleanupCtx, &milvuspb.DropCollectionRequest{
						DbName: test.dbName, CollectionName: collection,
					})
					s.NoError(merr.CheckRPCCall(dropped, err))
				})
				target := collection
				if test.alias {
					target = collection + "_alias"
					status, err = s.Cluster.MilvusClient.CreateAlias(ctx, &milvuspb.CreateAliasRequest{
						DbName: test.dbName, CollectionName: collection, Alias: target,
					})
					s.Require().NoError(merr.CheckRPCCall(status, err))
					s.T().Cleanup(func() {
						cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
						defer cleanupCancel()
						dropped, err := s.Cluster.MilvusClient.DropAlias(cleanupCtx, &milvuspb.DropAliasRequest{
							DbName: test.dbName, Alias: target,
						})
						s.NoError(merr.CheckRPCCall(dropped, err))
					})
				}
				describe := &milvuspb.DescribeCollectionRequest{DbName: test.dbName, CollectionName: collection}
				before, err := s.Cluster.MilvusClient.DescribeCollection(ctx, describe)
				s.Require().NoError(merr.CheckRPCCall(before, err))
				expectedEZ := ""
				if test.dbName == s.dbName {
					expectedEZ = strconv.FormatInt(s.ezID, 10)
				}
				s.Require().Equal(expectedEZ, propertyValue(before.GetProperties(), common.EncryptionEzIDKey))

				field := &schemapb.FieldSchema{Name: "added_payload", DataType: test.fieldType, Nullable: true}
				if operation == "AddCollectionField" {
					encoded, err = proto.Marshal(field)
					s.Require().NoError(err)
					status, err = s.Cluster.MilvusClient.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
						DbName: test.dbName, CollectionName: target, Schema: encoded,
					})
				} else {
					var response *milvuspb.AlterCollectionSchemaResponse
					response, err = s.Cluster.MilvusClient.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
						DbName: test.dbName, CollectionName: target,
						Action: &milvuspb.AlterCollectionSchemaRequest_Action{
							Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
								AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
									FieldInfos:         []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{{FieldSchema: field}},
									DoPhysicalBackfill: operation == "AlterCollectionSchemaWithBackfill",
								},
							},
						},
					})
					status = response.GetAlterStatus()
				}
				s.Require().NoError(err)
				after, err := s.Cluster.MilvusClient.DescribeCollection(ctx, describe)
				s.Require().NoError(merr.CheckRPCCall(after, err))
				s.Equal(expectedEZ, propertyValue(after.GetProperties(), common.EncryptionEzIDKey))
				if test.reject {
					s.Equal(merr.Code(merr.ErrParameterInvalid), status.GetCode())
					s.Contains(status.GetReason(), "TEXT fields are not supported with CMEK")
					s.Equal("true", status.GetExtraInfo()["is_input_error"])
					s.False(status.GetRetriable())
					s.Equal(before.GetSchema().GetVersion(), after.GetSchema().GetVersion())
					s.True(proto.Equal(before.GetSchema(), after.GetSchema()), "rejected addition must leave the complete schema unchanged")
					s.Equal(before.GetProperties(), after.GetProperties())
					return
				}
				s.Require().NoError(merr.Error(status))
				s.Equal(before.GetSchema().GetVersion()+1, after.GetSchema().GetVersion())
				s.Require().Len(after.GetSchema().GetFields(), len(before.GetSchema().GetFields())+1)
				added := after.GetSchema().GetFields()[len(after.GetSchema().GetFields())-1]
				s.Equal(field.GetName(), added.GetName())
				s.Equal(field.GetDataType(), added.GetDataType())
				s.True(added.GetNullable())
				s.Positive(added.GetFieldID())
			})
		}
	}
}
