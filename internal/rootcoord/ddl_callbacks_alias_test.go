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

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestDDLCallbacksAliasDDL(t *testing.T) {
	core := initStreamingSystemAndCore(t)

	// create database and collection to test alias ddl.
	status, err := core.CreateDatabase(context.Background(), &milvuspb.CreateDatabaseRequest{
		DbName: "test",
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	testSchema := &schemapb.CollectionSchema{
		Name:        "test_collection",
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				Name:     "field1",
				DataType: schemapb.DataType_Int64,
			},
		},
	}
	schemaBytes, _ := proto.Marshal(testSchema)
	status, err = core.CreateCollection(context.Background(), &milvuspb.CreateCollectionRequest{
		DbName:         "test",
		CollectionName: "test_collection",
		Schema:         schemaBytes,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	testSchema.Name = "test_collection2"
	schemaBytes, _ = proto.Marshal(testSchema)
	status, err = core.CreateCollection(context.Background(), &milvuspb.CreateCollectionRequest{
		DbName:         "test",
		CollectionName: "test_collection2",
		Schema:         schemaBytes,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	// create an alias with a not-exist database.
	status, err = core.CreateAlias(context.Background(), &milvuspb.CreateAliasRequest{
		DbName:         "test2",
		CollectionName: "test_collection",
		Alias:          "test_alias",
	})
	require.Error(t, merr.CheckRPCCall(status, err))

	// create an alias with a not-exist collection.
	status, err = core.CreateAlias(context.Background(), &milvuspb.CreateAliasRequest{
		DbName:         "test",
		CollectionName: "test_collection3",
		Alias:          "test_alias",
	})
	require.Error(t, merr.CheckRPCCall(status, err))

	// create an alias
	status, err = core.CreateAlias(context.Background(), &milvuspb.CreateAliasRequest{
		DbName:         "test",
		CollectionName: "test_collection",
		Alias:          "test_alias",
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	coll, err := core.meta.GetCollectionByName(context.Background(), "test", "test_alias", typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.NotZero(t, coll.CollectionID)
	require.Equal(t, "test_collection", coll.Name)

	// create an alias already created on current collection should be ok.
	status, err = core.CreateAlias(context.Background(), &milvuspb.CreateAliasRequest{
		DbName:         "test",
		CollectionName: "test_collection",
		Alias:          "test_alias",
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	// create an alias already created on another collection should be error.
	status, err = core.CreateAlias(context.Background(), &milvuspb.CreateAliasRequest{
		DbName:         "test",
		CollectionName: "test_collection2",
		Alias:          "test_alias",
	})
	require.Error(t, merr.CheckRPCCall(status, err))

	// test alter alias already created on current collection should be ok.
	status, err = core.AlterAlias(context.Background(), &milvuspb.AlterAliasRequest{
		DbName:         "test",
		CollectionName: "test_collection",
		Alias:          "test_alias",
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	// test alter alias to another collection should be ok.
	status, err = core.AlterAlias(context.Background(), &milvuspb.AlterAliasRequest{
		DbName:         "test",
		CollectionName: "test_collection2",
		Alias:          "test_alias",
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	// alter alias to a not-exist database should be error.
	status, err = core.AlterAlias(context.Background(), &milvuspb.AlterAliasRequest{
		DbName:         "test2",
		CollectionName: "test_collection2",
		Alias:          "test_alias",
	})
	require.Error(t, merr.CheckRPCCall(status, err))

	// alter alias to a not-exist collection should be error.
	status, err = core.AlterAlias(context.Background(), &milvuspb.AlterAliasRequest{
		DbName:         "test",
		CollectionName: "test_collection3",
		Alias:          "test_alias",
	})
	require.Error(t, merr.CheckRPCCall(status, err))

	// alter alias to a not exist alias should be error.
	status, err = core.AlterAlias(context.Background(), &milvuspb.AlterAliasRequest{
		DbName:         "test",
		CollectionName: "test_collection2",
		Alias:          "test_alias2",
	})
	require.Error(t, merr.CheckRPCCall(status, err))

	// drop a not exist alias should be ok.
	status, err = core.DropAlias(context.Background(), &milvuspb.DropAliasRequest{
		DbName: "test",
		Alias:  "test_alias2",
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	// drop a alias exist should be ok.
	status, err = core.DropAlias(context.Background(), &milvuspb.DropAliasRequest{
		DbName: "test",
		Alias:  "test_alias",
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	// drop a alias already dropped should be ok.
	status, err = core.DropAlias(context.Background(), &milvuspb.DropAliasRequest{
		DbName: "test",
		Alias:  "test_alias",
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
}
