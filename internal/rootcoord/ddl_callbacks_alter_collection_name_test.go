// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestDDLCallbacksAlterCollectionName(t *testing.T) {
	core := initStreamingSystemAndCore(t)

	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollection" + funcutil.RandomString(10)
	newDbName := "testDatabaseNew" + funcutil.RandomString(10)
	newCollectionName := "testCollectionNew" + funcutil.RandomString(10)

	resp, err := core.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)

	resp, err = core.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{
		OldName: collectionName,
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)

	resp, err = core.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{
		DbName:    dbName,
		OldName:   collectionName,
		NewDBName: dbName,
		NewName:   collectionName,
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrParameterInvalid)

	resp, err = core.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{
		DbName:    dbName,
		OldName:   collectionName,
		NewDBName: newDbName,
		NewName:   newCollectionName,
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrDatabaseNotFound)

	resp, err = core.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{
		DbName: util.DefaultDBName,
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	resp, err = core.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{
		DbName:  util.DefaultDBName,
		OldName: collectionName,
		NewName: newCollectionName,
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrCollectionNotFound)

	// rename a collection success in one database.
	createCollectionForTest(t, ctx, core, dbName, collectionName)
	resp, err = core.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{
		DbName:  dbName,
		OldName: collectionName,
		NewName: newCollectionName,
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
	require.ErrorIs(t, err, merr.ErrCollectionNotFound)
	require.Nil(t, coll)
	coll, err = core.meta.GetCollectionByName(ctx, dbName, newCollectionName, typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.NotNil(t, coll)
	require.Equal(t, coll.Name, newCollectionName)

	// rename a collection success cross database.
	resp, err = core.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{
		DbName: newDbName,
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	resp, err = core.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{
		DbName:    dbName,
		OldName:   newCollectionName,
		NewDBName: newDbName,
		NewName:   newCollectionName,
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	coll, err = core.meta.GetCollectionByName(ctx, dbName, newCollectionName, typeutil.MaxTimestamp)
	require.ErrorIs(t, err, merr.ErrCollectionNotFound)
	require.Nil(t, coll)
	coll, err = core.meta.GetCollectionByName(ctx, newDbName, newCollectionName, typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.NotNil(t, coll)
	require.Equal(t, coll.Name, newCollectionName)
	require.Equal(t, coll.DBName, newDbName)

	// rename a collection and database at same time.
	resp, err = core.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{
		DbName:    newDbName,
		OldName:   newCollectionName,
		NewDBName: dbName,
		NewName:   collectionName,
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	coll, err = core.meta.GetCollectionByName(ctx, newDbName, newCollectionName, typeutil.MaxTimestamp)
	require.ErrorIs(t, err, merr.ErrCollectionNotFound)
	require.Nil(t, coll)
	coll, err = core.meta.GetCollectionByName(ctx, newDbName, collectionName, typeutil.MaxTimestamp)
	require.ErrorIs(t, err, merr.ErrCollectionNotFound)
	require.Nil(t, coll)
	coll, err = core.meta.GetCollectionByName(ctx, dbName, newCollectionName, typeutil.MaxTimestamp)
	require.ErrorIs(t, err, merr.ErrCollectionNotFound)
	require.Nil(t, coll)
	coll, err = core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp)
	require.NoError(t, err)
	require.NotNil(t, coll)
	require.Equal(t, coll.Name, collectionName)
	require.Equal(t, coll.DBName, dbName)
	require.Equal(t, coll.SchemaVersion, int32(0)) // SchemaVersion should not be changed with rename.

	resp, err = core.CreateAlias(ctx, &milvuspb.CreateAliasRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Alias:          newCollectionName,
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))

	// rename a collection has aliases cross database should fail.
	resp, err = core.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{
		DbName:    dbName,
		OldName:   collectionName,
		NewDBName: newDbName,
		NewName:   collectionName,
	})
	require.Error(t, merr.CheckRPCCall(resp, err))

	// rename a collection to a duplicated name should fail.
	resp, err = core.RenameCollection(ctx, &milvuspb.RenameCollectionRequest{
		DbName:  dbName,
		OldName: collectionName,
		NewName: newCollectionName,
	})
	require.Error(t, merr.CheckRPCCall(resp, err))
}
