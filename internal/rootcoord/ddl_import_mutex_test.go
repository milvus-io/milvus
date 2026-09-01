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

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/mocks"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func newCoreWithImports(resp *internalpb.ListImportsResponse, err error) *Core {
	mixc := &mocks.MixCoord{}
	mixc.EXPECT().ListImports(mock.Anything, mock.Anything).Return(resp, err).Maybe()
	return &Core{mixCoord: mixc}
}

func TestCheckNoInFlightImportJob(t *testing.T) {
	ctx := context.Background()

	t.Run("no jobs", func(t *testing.T) {
		c := newCoreWithImports(&internalpb.ListImportsResponse{Status: merr.Success()}, nil)
		require.NoError(t, c.checkNoInFlightImportJob(ctx, "coll", 1))
	})

	t.Run("terminal jobs only", func(t *testing.T) {
		c := newCoreWithImports(&internalpb.ListImportsResponse{
			Status: merr.Success(),
			JobIDs: []string{"1", "2"},
			States: []internalpb.ImportJobState{internalpb.ImportJobState_Completed, internalpb.ImportJobState_Failed},
		}, nil)
		require.NoError(t, c.checkNoInFlightImportJob(ctx, "coll", 1))
	})

	t.Run("in-flight job blocks", func(t *testing.T) {
		c := newCoreWithImports(&internalpb.ListImportsResponse{
			Status: merr.Success(),
			JobIDs: []string{"1", "2"},
			States: []internalpb.ImportJobState{internalpb.ImportJobState_Completed, internalpb.ImportJobState_Importing},
		}, nil)
		err := c.checkNoInFlightImportJob(ctx, "coll", 1)
		require.ErrorIs(t, err, merr.ErrCollectionDDLImportConflict)
		require.False(t, merr.Status(err).GetRetriable())
	})

	t.Run("rpc error propagates", func(t *testing.T) {
		c := newCoreWithImports(&internalpb.ListImportsResponse{
			Status: merr.Status(merr.WrapErrServiceNotReady("datacoord", 1, "test")),
		}, nil)
		err := c.checkNoInFlightImportJob(ctx, "coll", 1)
		require.Error(t, err)
		require.NotErrorIs(t, err, merr.ErrCollectionDDLImportConflict)
	})
}

// fakeInFlightLister is a MixCoord that also provides the in-process
// state-only import lookup, mirroring mixCoordImpl.
type fakeInFlightLister struct {
	*mocks.MixCoord
	jobID int64
	state internalpb.ImportJobState
	found bool
	err   error
}

func (f *fakeInFlightLister) FirstInFlightImportJob(ctx context.Context, collectionID int64) (int64, internalpb.ImportJobState, bool, error) {
	return f.jobID, f.state, f.found, f.err
}

// TestCheckNoInFlightImportJobListerPath covers the state-only lookup path the
// in-process coordinator provides (no ListImports progress materialization).
func TestCheckNoInFlightImportJobListerPath(t *testing.T) {
	ctx := context.Background()

	c := &Core{mixCoord: &fakeInFlightLister{
		MixCoord: &mocks.MixCoord{},
		jobID:    7, state: internalpb.ImportJobState_Importing, found: true,
	}}
	err := c.checkNoInFlightImportJob(ctx, "coll", 1)
	require.ErrorIs(t, err, merr.ErrCollectionDDLImportConflict)
	require.False(t, merr.Status(err).GetRetriable())

	c = &Core{mixCoord: &fakeInFlightLister{MixCoord: &mocks.MixCoord{}}}
	require.NoError(t, c.checkNoInFlightImportJob(ctx, "coll", 1))

	c = &Core{mixCoord: &fakeInFlightLister{
		MixCoord: &mocks.MixCoord{},
		err:      merr.WrapErrServiceNotReady("datacoord", 1, "test"),
	}}
	err = c.checkNoInFlightImportJob(ctx, "coll", 1)
	require.Error(t, err)
	require.NotErrorIs(t, err, merr.ErrCollectionDDLImportConflict)
}

// TestCheckLockedCollectionName covers the post-lock re-resolution that closes
// the alias-repoint / rename window between name resolution and lock.
func TestCheckLockedCollectionName(t *testing.T) {
	ctx := context.Background()

	newCoreWithResolvedName := func(name string) *Core {
		meta := mockrootcoord.NewIMetaTable(t)
		meta.EXPECT().GetCollectionByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(&model.Collection{Name: name}, nil).Maybe()
		return &Core{meta: meta}
	}

	require.NoError(t, newCoreWithResolvedName("collA").checkLockedCollectionName(ctx, "db", "aliasX", "collA"))
	err := newCoreWithResolvedName("collB").checkLockedCollectionName(ctx, "db", "aliasX", "collA")
	require.ErrorIs(t, err, merr.ErrCollectionDDLImportConflict)
	require.False(t, merr.Status(err).GetRetriable())
}

// TestAddFieldRejectedDuringImport drives the real AddCollectionField path:
// with an in-flight import job on the collection the DDL must be rejected
// before any broadcast, leaving the schema version untouched.
func TestAddFieldRejectedDuringImport(t *testing.T) {
	core := initStreamingSystemAndCore(t)
	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testCollection" + funcutil.RandomString(10)
	createCollectionForTest(t, ctx, core, dbName, collectionName)

	importsMocker := mockey.Mock((*mocks.MixCoord).ListImports).Return(&internalpb.ListImportsResponse{
		Status: merr.Success(),
		JobIDs: []string{"100"},
		States: []internalpb.ImportJobState{internalpb.ImportJobState_Importing},
	}, nil).Build()
	t.Cleanup(func() { importsMocker.UnPatch() })

	schemaBytes, err := proto.Marshal(&schemapb.FieldSchema{
		Name:     "newField",
		DataType: schemapb.DataType_Int64,
		Nullable: true,
	})
	require.NoError(t, err)
	resp, err := core.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         schemaBytes,
	})
	require.ErrorIs(t, merr.CheckRPCCall(resp, err), merr.ErrCollectionDDLImportConflict)
	require.False(t, resp.GetRetriable())
	assertSchemaVersion(t, ctx, core, dbName, collectionName, 0)
	assertFieldNotExists(t, ctx, core, dbName, collectionName, "newField")
}
