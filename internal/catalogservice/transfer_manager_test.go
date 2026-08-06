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

package catalogservice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestTransferManagerMovesCollectionThroughRootCoordGate(t *testing.T) {
	ctx := context.Background()
	coll := testTransferCollection()
	src := mocks.NewRootCoordCatalog(t)
	dst := mocks.NewRootCoordCatalog(t)
	expectSourceAliases(src, coll)
	expectTargetAliases(dst, nil)
	var calls []string
	root := newRecordingTransferRootCoord(&calls)
	store := NewMemoryTransferJobStore()

	src.EXPECT().ListDatabases(mock.Anything, typeutil.MaxTimestamp).RunAndReturn(func(context.Context, typeutil.Timestamp) ([]*model.Database, error) {
		calls = append(calls, "src-list-db")
		return []*model.Database{{ID: 10, Name: "db"}}, nil
	})
	readCount := 0
	src.EXPECT().GetCollectionByName(mock.Anything, int64(10), "db", "coll", typeutil.MaxTimestamp).RunAndReturn(func(context.Context, int64, string, string, typeutil.Timestamp) (*model.Collection, error) {
		readCount++
		switch readCount {
		case 1:
			calls = append(calls, "src-read-before-prepare")
		case 2:
			calls = append(calls, "src-read-after-prepare")
		default:
			calls = append(calls, "src-read-before-drop")
		}
		return coll.Clone(), nil
	}).Times(3)
	dst.EXPECT().ListDatabases(mock.Anything, typeutil.MaxTimestamp).RunAndReturn(func(context.Context, typeutil.Timestamp) ([]*model.Database, error) {
		calls = append(calls, "dst-list-db")
		return []*model.Database{{ID: 10, Name: "db"}}, nil
	})
	dst.EXPECT().GetCollectionByID(mock.Anything, int64(10), typeutil.MaxTimestamp, int64(100)).Return(nil, nil)
	dst.EXPECT().GetCollectionByName(mock.Anything, int64(10), "db", "coll", typeutil.MaxTimestamp).Return(nil, nil)
	dst.EXPECT().Update(mock.Anything, uint64(99), mock.MatchedBy(func(action metastore.UpdateAction) bool {
		return action.Type == metastore.ActionAdd
	})).RunAndReturn(func(_ context.Context, _ typeutil.Timestamp, _ ...metastore.UpdateAction) error {
		calls = append(calls, "dst-create")
		return nil
	})
	dst.EXPECT().CreateAlias(mock.Anything, mock.MatchedBy(func(alias *model.Alias) bool {
		return alias.Name == "alias1" && alias.CollectionID == coll.CollectionID && alias.DbID == coll.DBID
	}), uint64(99)).RunAndReturn(func(context.Context, *model.Alias, typeutil.Timestamp) error {
		calls = append(calls, "dst-create-alias")
		return nil
	})
	src.EXPECT().Update(mock.Anything, uint64(99), mock.MatchedBy(func(action metastore.UpdateAction) bool {
		return action.Type == metastore.ActionDelete
	})).RunAndReturn(func(ctx context.Context, _ typeutil.Timestamp, _ ...metastore.UpdateAction) error {
		job, err := store.Get(ctx, "transfer-1")
		require.NoError(t, err)
		require.Equal(t, TransferStateSourceDropped, job.State)
		calls = append(calls, "src-drop")
		return nil
	})

	mgr := NewTransferManager(
		StaticRootCoordCatalogResolver(map[string]metastore.RootCoordCatalog{
			"milvus1": src,
			"milvus2": dst,
		}),
		StaticTransferRootCoordResolver(map[string]TransferRootCoord{
			"milvus1": root,
			"milvus2": root,
		}),
		store,
	)
	resp, err := mgr.StartCollectionTransfer(ctx, StartCollectionTransferRequest{
		TransferID:      "transfer-1",
		TransferEpoch:   11,
		SourceNamespace: "milvus1",
		TargetNamespace: "milvus2",
		DBName:          "db",
		CollectionName:  "coll",
		CommitTs:        99,
		CacheExpireTs:   100,
		DrainTimeoutMs:  2000,
	})
	require.NoError(t, err)
	require.Equal(t, TransferStateDone, resp.State)
	require.Equal(t, int64(100), resp.CollectionID)
	require.Equal(t, []string{
		"src-list-db",
		"src-read-before-prepare",
		"root-prepare",
		"src-read-after-prepare",
		"dst-list-db",
		"src-read-before-drop",
		"src-drop",
		"dst-create",
		"dst-create-alias",
		"root-deactivate",
		"root-apply",
	}, calls)
	require.Equal(t, int64(100), root.prepare.GetCollectionId())
	require.Equal(t, []string{"alias1"}, root.deactivate.GetAliases())
	require.Equal(t, "coll", root.apply.GetCollection().GetSchema().GetName())
	require.Len(t, root.apply.GetPartitions(), 1)
	require.Len(t, root.apply.GetAliases(), 1)
}

func TestTransferManagerAbortsSourceGateWhenCatalogMoveFailsBeforeTargetWrite(t *testing.T) {
	ctx := context.Background()
	coll := testTransferCollection()
	src := mocks.NewRootCoordCatalog(t)
	dst := mocks.NewRootCoordCatalog(t)
	expectSourceAliases(src, coll)
	root := newRecordingTransferRootCoord(nil)

	src.EXPECT().ListDatabases(mock.Anything, typeutil.MaxTimestamp).Return([]*model.Database{{ID: 10, Name: "db"}}, nil)
	src.EXPECT().GetCollectionByName(mock.Anything, int64(10), "db", "coll", typeutil.MaxTimestamp).Return(coll.Clone(), nil).Twice()
	dst.EXPECT().ListDatabases(mock.Anything, typeutil.MaxTimestamp).Return(nil, assertErr("target catalog unavailable"))

	mgr := NewTransferManager(
		StaticRootCoordCatalogResolver(map[string]metastore.RootCoordCatalog{"milvus1": src, "milvus2": dst}),
		StaticTransferRootCoordResolver(map[string]TransferRootCoord{"milvus1": root, "milvus2": root}),
		NewMemoryTransferJobStore(),
	)
	_, err := mgr.StartCollectionTransfer(ctx, StartCollectionTransferRequest{
		TransferID:      "transfer-1",
		TransferEpoch:   11,
		SourceNamespace: "milvus1",
		TargetNamespace: "milvus2",
		DBName:          "db",
		CollectionName:  "coll",
		CommitTs:        99,
		CacheExpireTs:   100,
		DrainTimeoutMs:  2000,
	})
	require.Error(t, err)
	require.NotNil(t, root.abort)
	require.Equal(t, int64(100), root.abort.GetCollectionId())
}

func TestTransferManagerRejectsTargetDatabaseIDMismatch(t *testing.T) {
	ctx := context.Background()
	coll := testTransferCollection()
	src := mocks.NewRootCoordCatalog(t)
	dst := mocks.NewRootCoordCatalog(t)
	expectSourceAliases(src, coll)
	root := newRecordingTransferRootCoord(nil)

	src.EXPECT().ListDatabases(mock.Anything, typeutil.MaxTimestamp).Return([]*model.Database{{ID: 10, Name: "db"}}, nil)
	src.EXPECT().GetCollectionByName(mock.Anything, int64(10), "db", "coll", typeutil.MaxTimestamp).Return(coll.Clone(), nil).Twice()
	dst.EXPECT().ListDatabases(mock.Anything, typeutil.MaxTimestamp).Return([]*model.Database{{ID: 20, Name: "db"}}, nil)

	mgr := NewTransferManager(
		StaticRootCoordCatalogResolver(map[string]metastore.RootCoordCatalog{"milvus1": src, "milvus2": dst}),
		StaticTransferRootCoordResolver(map[string]TransferRootCoord{"milvus1": root, "milvus2": root}),
		NewMemoryTransferJobStore(),
	)
	_, err := mgr.StartCollectionTransfer(ctx, StartCollectionTransferRequest{
		TransferID:      "transfer-1",
		TransferEpoch:   11,
		SourceNamespace: "milvus1",
		TargetNamespace: "milvus2",
		DBName:          "db",
		CollectionName:  "coll",
		CommitTs:        99,
		CacheExpireTs:   100,
		DrainTimeoutMs:  2000,
	})
	require.ErrorContains(t, err, "target database id mismatch")
	require.NotNil(t, root.abort)
}

func TestTransferManagerCreatesTargetDatabaseWhenMissing(t *testing.T) {
	ctx := context.Background()
	coll := testTransferCollection()
	src := mocks.NewRootCoordCatalog(t)
	dst := mocks.NewRootCoordCatalog(t)
	expectSourceAliases(src, coll)
	expectTargetAliases(dst, nil)
	var calls []string
	root := newRecordingTransferRootCoord(&calls)

	src.EXPECT().ListDatabases(mock.Anything, typeutil.MaxTimestamp).Return([]*model.Database{{ID: 10, Name: "db"}}, nil)
	src.EXPECT().GetCollectionByName(mock.Anything, int64(10), "db", "coll", typeutil.MaxTimestamp).Return(coll.Clone(), nil).Times(3)
	dst.EXPECT().ListDatabases(mock.Anything, typeutil.MaxTimestamp).RunAndReturn(func(context.Context, typeutil.Timestamp) ([]*model.Database, error) {
		calls = append(calls, "dst-list-db")
		return []*model.Database{{ID: 1, Name: "default"}}, nil
	})
	dst.EXPECT().GetCollectionByID(mock.Anything, int64(10), typeutil.MaxTimestamp, int64(100)).Return(nil, nil)
	dst.EXPECT().GetCollectionByName(mock.Anything, int64(10), "db", "coll", typeutil.MaxTimestamp).Return(nil, nil)
	dst.EXPECT().CreateDatabase(mock.Anything, mock.MatchedBy(func(db *model.Database) bool {
		return db != nil && db.ID == 10 && db.Name == "db"
	}), uint64(99)).RunAndReturn(func(context.Context, *model.Database, typeutil.Timestamp) error {
		calls = append(calls, "dst-create-db")
		return nil
	})
	dst.EXPECT().Update(mock.Anything, uint64(99), mock.MatchedBy(func(action metastore.UpdateAction) bool {
		return action.Type == metastore.ActionAdd
	})).RunAndReturn(func(_ context.Context, _ typeutil.Timestamp, _ ...metastore.UpdateAction) error {
		calls = append(calls, "dst-create")
		return nil
	})
	dst.EXPECT().CreateAlias(mock.Anything, mock.Anything, uint64(99)).Return(nil)
	src.EXPECT().Update(mock.Anything, uint64(99), mock.MatchedBy(func(action metastore.UpdateAction) bool {
		return action.Type == metastore.ActionDelete
	})).Return(nil)

	mgr := NewTransferManager(
		StaticRootCoordCatalogResolver(map[string]metastore.RootCoordCatalog{"milvus1": src, "milvus2": dst}),
		StaticTransferRootCoordResolver(map[string]TransferRootCoord{"milvus1": root, "milvus2": root}),
		NewMemoryTransferJobStore(),
	)
	resp, err := mgr.StartCollectionTransfer(ctx, StartCollectionTransferRequest{
		TransferID:      "transfer-1",
		TransferEpoch:   11,
		SourceNamespace: "milvus1",
		TargetNamespace: "milvus2",
		DBName:          "db",
		CollectionName:  "coll",
		CommitTs:        99,
		CacheExpireTs:   100,
		DrainTimeoutMs:  2000,
	})
	require.NoError(t, err)
	require.Equal(t, TransferStateDone, resp.State)
	require.Contains(t, calls, "dst-create-db")
}

func TestTransferManagerRejectsTargetCollectionIDConflictBeforeTargetWrite(t *testing.T) {
	ctx := context.Background()
	coll := testTransferCollection()
	conflict := coll.Clone()
	conflict.Name = "other"
	conflict.Aliases = nil
	src := mocks.NewRootCoordCatalog(t)
	dst := mocks.NewRootCoordCatalog(t)
	expectSourceAliases(src, coll)
	expectTargetAliases(dst, nil)
	root := newRecordingTransferRootCoord(nil)

	src.EXPECT().ListDatabases(mock.Anything, typeutil.MaxTimestamp).Return([]*model.Database{{ID: 10, Name: "db"}}, nil)
	src.EXPECT().GetCollectionByName(mock.Anything, int64(10), "db", "coll", typeutil.MaxTimestamp).Return(coll.Clone(), nil).Twice()
	dst.EXPECT().ListDatabases(mock.Anything, typeutil.MaxTimestamp).Return([]*model.Database{{ID: 10, Name: "db"}}, nil)
	dst.EXPECT().GetCollectionByID(mock.Anything, int64(10), typeutil.MaxTimestamp, int64(100)).Return(conflict, nil)

	mgr := NewTransferManager(
		StaticRootCoordCatalogResolver(map[string]metastore.RootCoordCatalog{"milvus1": src, "milvus2": dst}),
		StaticTransferRootCoordResolver(map[string]TransferRootCoord{"milvus1": root, "milvus2": root}),
		NewMemoryTransferJobStore(),
	)
	_, err := mgr.StartCollectionTransfer(ctx, StartCollectionTransferRequest{
		TransferID:      "transfer-1",
		TransferEpoch:   11,
		SourceNamespace: "milvus1",
		TargetNamespace: "milvus2",
		DBName:          "db",
		CollectionName:  "coll",
		CommitTs:        99,
		CacheExpireTs:   100,
		DrainTimeoutMs:  2000,
	})
	require.ErrorContains(t, err, "target collection id")
	require.NotNil(t, root.abort)
}

func TestTransferManagerRejectsTargetAliasConflictBeforeTargetWrite(t *testing.T) {
	ctx := context.Background()
	coll := testTransferCollection()
	src := mocks.NewRootCoordCatalog(t)
	dst := mocks.NewRootCoordCatalog(t)
	expectSourceAliases(src, coll)
	expectTargetAliases(dst, []*model.Alias{{
		Name:         "alias1",
		CollectionID: 200,
		State:        etcdpb.AliasState_AliasCreated,
		DbID:         10,
	}})
	root := newRecordingTransferRootCoord(nil)

	src.EXPECT().ListDatabases(mock.Anything, typeutil.MaxTimestamp).Return([]*model.Database{{ID: 10, Name: "db"}}, nil)
	src.EXPECT().GetCollectionByName(mock.Anything, int64(10), "db", "coll", typeutil.MaxTimestamp).Return(coll.Clone(), nil).Twice()
	dst.EXPECT().ListDatabases(mock.Anything, typeutil.MaxTimestamp).Return([]*model.Database{{ID: 10, Name: "db"}}, nil)
	dst.EXPECT().GetCollectionByID(mock.Anything, int64(10), typeutil.MaxTimestamp, int64(100)).Return(nil, nil)
	dst.EXPECT().GetCollectionByName(mock.Anything, int64(10), "db", "coll", typeutil.MaxTimestamp).Return(nil, nil)

	mgr := NewTransferManager(
		StaticRootCoordCatalogResolver(map[string]metastore.RootCoordCatalog{"milvus1": src, "milvus2": dst}),
		StaticTransferRootCoordResolver(map[string]TransferRootCoord{"milvus1": root, "milvus2": root}),
		NewMemoryTransferJobStore(),
	)
	_, err := mgr.StartCollectionTransfer(ctx, StartCollectionTransferRequest{
		TransferID:      "transfer-1",
		TransferEpoch:   11,
		SourceNamespace: "milvus1",
		TargetNamespace: "milvus2",
		DBName:          "db",
		CollectionName:  "coll",
		CommitTs:        99,
		CacheExpireTs:   100,
		DrainTimeoutMs:  2000,
	})
	require.ErrorContains(t, err, "target alias")
	require.NotNil(t, root.abort)
}

func TestTransferManagerRetriesAbortAfterAbortRPCFailure(t *testing.T) {
	ctx := context.Background()
	coll := testTransferCollection()
	src := mocks.NewRootCoordCatalog(t)
	dst := mocks.NewRootCoordCatalog(t)
	expectSourceAliases(src, coll)
	root := newRecordingTransferRootCoord(nil)
	root.abortErr = assertErr("abort unavailable")
	store := NewMemoryTransferJobStore()

	src.EXPECT().ListDatabases(mock.Anything, typeutil.MaxTimestamp).Return([]*model.Database{{ID: 10, Name: "db"}}, nil)
	src.EXPECT().GetCollectionByName(mock.Anything, int64(10), "db", "coll", typeutil.MaxTimestamp).Return(coll.Clone(), nil).Times(3)
	dst.EXPECT().ListDatabases(mock.Anything, typeutil.MaxTimestamp).Return(nil, assertErr("target preflight failed")).Once()

	mgr := NewTransferManager(
		StaticRootCoordCatalogResolver(map[string]metastore.RootCoordCatalog{"milvus1": src, "milvus2": dst}),
		StaticTransferRootCoordResolver(map[string]TransferRootCoord{"milvus1": root, "milvus2": root}),
		store,
	)
	_, err := mgr.StartCollectionTransfer(ctx, StartCollectionTransferRequest{
		TransferID:      "transfer-1",
		TransferEpoch:   11,
		SourceNamespace: "milvus1",
		TargetNamespace: "milvus2",
		DBName:          "db",
		CollectionName:  "coll",
		CommitTs:        99,
		CacheExpireTs:   100,
		DrainTimeoutMs:  2000,
	})
	require.ErrorContains(t, err, "abort unavailable")
	job, err := store.Get(ctx, "transfer-1")
	require.NoError(t, err)
	require.Equal(t, TransferStatePrepared, job.State)

	root.abortErr = nil
	dst.EXPECT().ListDatabases(mock.Anything, typeutil.MaxTimestamp).Return(nil, assertErr("target preflight failed")).Once()
	_, err = mgr.StartCollectionTransfer(ctx, StartCollectionTransferRequest{
		TransferID:      "transfer-1",
		TransferEpoch:   11,
		SourceNamespace: "milvus1",
		TargetNamespace: "milvus2",
		DBName:          "db",
		CollectionName:  "coll",
		CommitTs:        99,
		CacheExpireTs:   100,
		DrainTimeoutMs:  2000,
	})
	require.ErrorContains(t, err, "target preflight failed")
	job, err = store.Get(ctx, "transfer-1")
	require.NoError(t, err)
	require.Equal(t, TransferStateAborted, job.State)
}

func TestTransferManagerRetriesTargetApplyFromStoredSnapshotAfterSourceDrop(t *testing.T) {
	ctx := context.Background()
	coll := testTransferCollection()
	src := mocks.NewRootCoordCatalog(t)
	dst := mocks.NewRootCoordCatalog(t)
	expectSourceAliases(src, coll)
	expectTargetAliases(dst, nil)
	root := newRecordingTransferRootCoord(nil)
	store := NewMemoryTransferJobStore()

	src.EXPECT().ListDatabases(mock.Anything, typeutil.MaxTimestamp).Return([]*model.Database{{ID: 10, Name: "db"}}, nil).Once()
	src.EXPECT().GetCollectionByName(mock.Anything, int64(10), "db", "coll", typeutil.MaxTimestamp).Return(coll.Clone(), nil).Times(3)
	dst.EXPECT().ListDatabases(mock.Anything, typeutil.MaxTimestamp).Return([]*model.Database{{ID: 10, Name: "db"}}, nil).Once()
	dst.EXPECT().GetCollectionByID(mock.Anything, int64(10), typeutil.MaxTimestamp, int64(100)).Return(nil, nil).Once()
	dst.EXPECT().GetCollectionByName(mock.Anything, int64(10), "db", "coll", typeutil.MaxTimestamp).Return(nil, nil).Once()
	src.EXPECT().Update(mock.Anything, uint64(99), mock.Anything).Return(nil).Once()
	dst.EXPECT().Update(mock.Anything, uint64(99), mock.Anything).Return(nil).Once()
	dst.EXPECT().CreateAlias(mock.Anything, mock.Anything, uint64(99)).Return(nil).Once()
	root.applyErr = assertErr("target apply unavailable")

	mgr := NewTransferManager(
		StaticRootCoordCatalogResolver(map[string]metastore.RootCoordCatalog{"milvus1": src, "milvus2": dst}),
		StaticTransferRootCoordResolver(map[string]TransferRootCoord{"milvus1": root, "milvus2": root}),
		store,
	)
	_, err := mgr.StartCollectionTransfer(ctx, StartCollectionTransferRequest{
		TransferID:      "transfer-1",
		TransferEpoch:   11,
		SourceNamespace: "milvus1",
		TargetNamespace: "milvus2",
		DBName:          "db",
		CollectionName:  "coll",
		CommitTs:        99,
		CacheExpireTs:   100,
		DrainTimeoutMs:  2000,
	})
	require.ErrorContains(t, err, "target apply unavailable")

	root.applyErr = nil
	resp, err := mgr.StartCollectionTransfer(ctx, StartCollectionTransferRequest{
		TransferID:      "transfer-1",
		TransferEpoch:   11,
		SourceNamespace: "milvus1",
		TargetNamespace: "milvus2",
		DBName:          "db",
		CollectionName:  "coll",
		CommitTs:        99,
		CacheExpireTs:   100,
		DrainTimeoutMs:  2000,
	})
	require.NoError(t, err)
	require.Equal(t, TransferStateDone, resp.State)
	require.Equal(t, int64(100), root.apply.GetCollection().GetID())
}

func TestTransferManagerRetriesFromSourceDroppedUsingStoredSnapshot(t *testing.T) {
	ctx := context.Background()
	coll := testTransferCollection()
	src := mocks.NewRootCoordCatalog(t)
	dst := mocks.NewRootCoordCatalog(t)
	expectTargetAliases(dst, nil)
	root := newRecordingTransferRootCoord(nil)
	store := NewMemoryTransferJobStore()
	require.NoError(t, store.Save(ctx, &TransferJob{
		TransferID:      "transfer-1",
		TransferEpoch:   11,
		SourceNamespace: "milvus1",
		TargetNamespace: "milvus2",
		DBName:          "db",
		CollectionName:  "coll",
		CommitTs:        99,
		CacheExpireTs:   100,
		DrainTimeoutMs:  2000,
		CollectionID:    coll.CollectionID,
		Collection:      coll.Clone(),
		State:           TransferStateSourceDropped,
	}))

	src.EXPECT().GetCollectionByName(mock.Anything, int64(10), "db", "coll", typeutil.MaxTimestamp).Return(nil, nil).Once()
	dst.EXPECT().ListDatabases(mock.Anything, typeutil.MaxTimestamp).Return([]*model.Database{{ID: 10, Name: "db"}}, nil).Once()
	dst.EXPECT().GetCollectionByID(mock.Anything, int64(10), typeutil.MaxTimestamp, int64(100)).Return(nil, nil).Once()
	dst.EXPECT().GetCollectionByName(mock.Anything, int64(10), "db", "coll", typeutil.MaxTimestamp).Return(nil, nil).Once()
	dst.EXPECT().Update(mock.Anything, uint64(99), mock.MatchedBy(func(action metastore.UpdateAction) bool {
		return action.Type == metastore.ActionAdd
	})).Return(nil).Once()
	dst.EXPECT().CreateAlias(mock.Anything, mock.MatchedBy(func(alias *model.Alias) bool {
		return alias.Name == "alias1" && alias.CollectionID == coll.CollectionID && alias.DbID == coll.DBID
	}), uint64(99)).Return(nil).Once()

	mgr := NewTransferManager(
		StaticRootCoordCatalogResolver(map[string]metastore.RootCoordCatalog{"milvus1": src, "milvus2": dst}),
		StaticTransferRootCoordResolver(map[string]TransferRootCoord{"milvus1": root, "milvus2": root}),
		store,
	)
	resp, err := mgr.StartCollectionTransfer(ctx, StartCollectionTransferRequest{
		TransferID:      "transfer-1",
		TransferEpoch:   11,
		SourceNamespace: "milvus1",
		TargetNamespace: "milvus2",
		DBName:          "db",
		CollectionName:  "coll",
		CommitTs:        99,
		CacheExpireTs:   100,
		DrainTimeoutMs:  2000,
	})
	require.NoError(t, err)
	require.Equal(t, TransferStateDone, resp.State)
	require.Equal(t, int64(100), root.apply.GetCollection().GetID())
	require.NotNil(t, root.deactivate)
}

func TestTransferManagerRetriesSourceDropWhenSourceDroppedStateButSourceStillPresent(t *testing.T) {
	ctx := context.Background()
	coll := testTransferCollection()
	src := mocks.NewRootCoordCatalog(t)
	dst := mocks.NewRootCoordCatalog(t)
	expectSourceAliases(src, coll)
	expectTargetAliases(dst, nil)
	root := newRecordingTransferRootCoord(nil)
	store := NewMemoryTransferJobStore()
	require.NoError(t, store.Save(ctx, &TransferJob{
		TransferID:      "transfer-1",
		TransferEpoch:   11,
		SourceNamespace: "milvus1",
		TargetNamespace: "milvus2",
		DBName:          "db",
		CollectionName:  "coll",
		CommitTs:        99,
		CacheExpireTs:   100,
		DrainTimeoutMs:  2000,
		CollectionID:    coll.CollectionID,
		Collection:      coll.Clone(),
		State:           TransferStateSourceDropped,
	}))

	src.EXPECT().GetCollectionByName(mock.Anything, int64(10), "db", "coll", typeutil.MaxTimestamp).Return(coll.Clone(), nil).Once()
	src.EXPECT().Update(mock.Anything, uint64(99), mock.MatchedBy(func(action metastore.UpdateAction) bool {
		return action.Type == metastore.ActionDelete
	})).Return(nil).Once()
	dst.EXPECT().ListDatabases(mock.Anything, typeutil.MaxTimestamp).Return([]*model.Database{{ID: 10, Name: "db"}}, nil).Once()
	dst.EXPECT().GetCollectionByID(mock.Anything, int64(10), typeutil.MaxTimestamp, int64(100)).Return(nil, nil).Once()
	dst.EXPECT().GetCollectionByName(mock.Anything, int64(10), "db", "coll", typeutil.MaxTimestamp).Return(nil, nil).Once()
	dst.EXPECT().Update(mock.Anything, uint64(99), mock.MatchedBy(func(action metastore.UpdateAction) bool {
		return action.Type == metastore.ActionAdd
	})).Return(nil).Once()
	dst.EXPECT().CreateAlias(mock.Anything, mock.MatchedBy(func(alias *model.Alias) bool {
		return alias.Name == "alias1" && alias.CollectionID == coll.CollectionID && alias.DbID == coll.DBID
	}), uint64(99)).Return(nil).Once()

	mgr := NewTransferManager(
		StaticRootCoordCatalogResolver(map[string]metastore.RootCoordCatalog{"milvus1": src, "milvus2": dst}),
		StaticTransferRootCoordResolver(map[string]TransferRootCoord{"milvus1": root, "milvus2": root}),
		store,
	)
	resp, err := mgr.StartCollectionTransfer(ctx, StartCollectionTransferRequest{
		TransferID:      "transfer-1",
		TransferEpoch:   11,
		SourceNamespace: "milvus1",
		TargetNamespace: "milvus2",
		DBName:          "db",
		CollectionName:  "coll",
		CommitTs:        99,
		CacheExpireTs:   100,
		DrainTimeoutMs:  2000,
	})
	require.NoError(t, err)
	require.Equal(t, TransferStateDone, resp.State)
	require.NotNil(t, root.deactivate)
	require.NotNil(t, root.apply)
}

func TestTransferManagerRetriesCommitUncertainFromStoredSnapshot(t *testing.T) {
	ctx := context.Background()
	coll := testTransferCollection()
	src := mocks.NewRootCoordCatalog(t)
	dst := mocks.NewRootCoordCatalog(t)
	expectTargetAliases(dst, nil)
	root := newRecordingTransferRootCoord(nil)
	store := NewMemoryTransferJobStore()
	require.NoError(t, store.Save(ctx, &TransferJob{
		TransferID:      "transfer-1",
		TransferEpoch:   11,
		SourceNamespace: "milvus1",
		TargetNamespace: "milvus2",
		DBName:          "db",
		CollectionName:  "coll",
		CommitTs:        99,
		CacheExpireTs:   100,
		DrainTimeoutMs:  2000,
		CollectionID:    coll.CollectionID,
		Collection:      coll.Clone(),
		State:           TransferStateCommitUncertain,
		LastError:       "previous target write was uncertain",
	}))

	src.EXPECT().GetCollectionByName(mock.Anything, int64(10), "db", "coll", typeutil.MaxTimestamp).Return(nil, nil).Once()
	dst.EXPECT().ListDatabases(mock.Anything, typeutil.MaxTimestamp).Return([]*model.Database{{ID: 10, Name: "db"}}, nil).Once()
	dst.EXPECT().GetCollectionByID(mock.Anything, int64(10), typeutil.MaxTimestamp, int64(100)).Return(nil, nil).Once()
	dst.EXPECT().GetCollectionByName(mock.Anything, int64(10), "db", "coll", typeutil.MaxTimestamp).Return(nil, nil).Once()
	dst.EXPECT().Update(mock.Anything, uint64(99), mock.MatchedBy(func(action metastore.UpdateAction) bool {
		return action.Type == metastore.ActionAdd
	})).Return(nil).Once()
	dst.EXPECT().CreateAlias(mock.Anything, mock.MatchedBy(func(alias *model.Alias) bool {
		return alias.Name == "alias1" && alias.CollectionID == coll.CollectionID && alias.DbID == coll.DBID
	}), uint64(99)).Return(nil).Once()

	mgr := NewTransferManager(
		StaticRootCoordCatalogResolver(map[string]metastore.RootCoordCatalog{"milvus1": src, "milvus2": dst}),
		StaticTransferRootCoordResolver(map[string]TransferRootCoord{"milvus1": root, "milvus2": root}),
		store,
	)
	resp, err := mgr.StartCollectionTransfer(ctx, StartCollectionTransferRequest{
		TransferID:      "transfer-1",
		TransferEpoch:   11,
		SourceNamespace: "milvus1",
		TargetNamespace: "milvus2",
		DBName:          "db",
		CollectionName:  "coll",
		CommitTs:        99,
		CacheExpireTs:   100,
		DrainTimeoutMs:  2000,
	})
	require.NoError(t, err)
	require.Equal(t, TransferStateDone, resp.State)
	require.NotNil(t, root.deactivate)
	require.NotNil(t, root.apply)
}

func TestTransferManagerRetrySkipsExistingTargetAliasForSameCollection(t *testing.T) {
	ctx := context.Background()
	coll := testTransferCollection()
	src := mocks.NewRootCoordCatalog(t)
	dst := mocks.NewRootCoordCatalog(t)
	root := newRecordingTransferRootCoord(nil)
	store := NewMemoryTransferJobStore()
	require.NoError(t, store.Save(ctx, &TransferJob{
		TransferID:      "transfer-1",
		TransferEpoch:   11,
		SourceNamespace: "milvus1",
		TargetNamespace: "milvus2",
		DBName:          "db",
		CollectionName:  "coll",
		CommitTs:        99,
		CacheExpireTs:   100,
		DrainTimeoutMs:  2000,
		CollectionID:    coll.CollectionID,
		Collection:      coll.Clone(),
		State:           TransferStateSourceDropped,
	}))

	src.EXPECT().GetCollectionByName(mock.Anything, int64(10), "db", "coll", typeutil.MaxTimestamp).Return(nil, nil).Once()
	dst.EXPECT().ListDatabases(mock.Anything, typeutil.MaxTimestamp).Return([]*model.Database{{ID: 10, Name: "db"}}, nil).Once()
	dst.EXPECT().GetCollectionByID(mock.Anything, int64(10), typeutil.MaxTimestamp, int64(100)).Return(coll.Clone(), nil).Once()
	dst.EXPECT().GetCollectionByName(mock.Anything, int64(10), "db", "coll", typeutil.MaxTimestamp).Return(coll.Clone(), nil).Once()
	expectTargetAliases(dst, []*model.Alias{{
		Name:         "alias1",
		CollectionID: coll.CollectionID,
		State:        etcdpb.AliasState_AliasCreated,
		DbID:         coll.DBID,
	}})

	mgr := NewTransferManager(
		StaticRootCoordCatalogResolver(map[string]metastore.RootCoordCatalog{"milvus1": src, "milvus2": dst}),
		StaticTransferRootCoordResolver(map[string]TransferRootCoord{"milvus1": root, "milvus2": root}),
		store,
	)
	resp, err := mgr.StartCollectionTransfer(ctx, StartCollectionTransferRequest{
		TransferID:      "transfer-1",
		TransferEpoch:   11,
		SourceNamespace: "milvus1",
		TargetNamespace: "milvus2",
		DBName:          "db",
		CollectionName:  "coll",
		CommitTs:        99,
		CacheExpireTs:   100,
		DrainTimeoutMs:  2000,
	})
	require.NoError(t, err)
	require.Equal(t, TransferStateDone, resp.State)
	require.NotNil(t, root.deactivate)
	require.NotNil(t, root.apply)
}

func TestTransferManagerMarksCommitUncertainAfterPointOfNoReturnFailure(t *testing.T) {
	ctx := context.Background()
	coll := testTransferCollection()
	src := mocks.NewRootCoordCatalog(t)
	dst := mocks.NewRootCoordCatalog(t)
	expectTargetAliases(dst, nil)
	root := newRecordingTransferRootCoord(nil)
	store := NewMemoryTransferJobStore()
	require.NoError(t, store.Save(ctx, &TransferJob{
		TransferID:      "transfer-uncertain",
		TransferEpoch:   11,
		SourceNamespace: "milvus1",
		TargetNamespace: "milvus2",
		DBName:          "db",
		CollectionName:  "coll",
		CommitTs:        99,
		CacheExpireTs:   100,
		DrainTimeoutMs:  2000,
		CollectionID:    coll.CollectionID,
		Collection:      coll.Clone(),
		State:           TransferStateSourceDropped,
	}))

	src.EXPECT().GetCollectionByName(mock.Anything, int64(10), "db", "coll", typeutil.MaxTimestamp).Return(nil, nil).Once()
	dst.EXPECT().ListDatabases(mock.Anything, typeutil.MaxTimestamp).Return([]*model.Database{{ID: 10, Name: "db"}}, nil).Once()
	dst.EXPECT().GetCollectionByID(mock.Anything, int64(10), typeutil.MaxTimestamp, int64(100)).Return(nil, nil).Once()
	dst.EXPECT().GetCollectionByName(mock.Anything, int64(10), "db", "coll", typeutil.MaxTimestamp).Return(nil, nil).Once()
	dst.EXPECT().Update(mock.Anything, uint64(99), mock.Anything).Return(assertErr("target create uncertain")).Once()

	mgr := NewTransferManager(
		StaticRootCoordCatalogResolver(map[string]metastore.RootCoordCatalog{"milvus1": src, "milvus2": dst}),
		StaticTransferRootCoordResolver(map[string]TransferRootCoord{"milvus1": root, "milvus2": root}),
		store,
	)
	_, err := mgr.StartCollectionTransfer(ctx, StartCollectionTransferRequest{
		TransferID:      "transfer-uncertain",
		TransferEpoch:   11,
		SourceNamespace: "milvus1",
		TargetNamespace: "milvus2",
		DBName:          "db",
		CollectionName:  "coll",
		CommitTs:        99,
		CacheExpireTs:   100,
		DrainTimeoutMs:  2000,
	})
	require.ErrorContains(t, err, "target create uncertain")

	job, err := store.Get(ctx, "transfer-uncertain")
	require.NoError(t, err)
	require.Equal(t, TransferStateCommitUncertain, job.State)
	require.Contains(t, job.LastError, "target create uncertain")
}

func TestTransferManagerRejectsRetryWithDifferentSideEffectParameters(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryTransferJobStore()
	require.NoError(t, store.Save(ctx, &TransferJob{
		TransferID:      "transfer-1",
		TransferEpoch:   11,
		SourceNamespace: "milvus1",
		TargetNamespace: "milvus2",
		DBName:          "db",
		CollectionName:  "coll",
		CommitTs:        99,
		CacheExpireTs:   100,
		DrainTimeoutMs:  2000,
		State:           TransferStatePending,
	}))

	mgr := NewTransferManager(
		StaticRootCoordCatalogResolver(map[string]metastore.RootCoordCatalog{}),
		StaticTransferRootCoordResolver(map[string]TransferRootCoord{}),
		store,
	)
	_, err := mgr.StartCollectionTransfer(ctx, StartCollectionTransferRequest{
		TransferID:      "transfer-1",
		TransferEpoch:   11,
		SourceNamespace: "milvus1",
		TargetNamespace: "milvus2",
		DBName:          "db",
		CollectionName:  "coll",
		CommitTs:        100,
		CacheExpireTs:   100,
		DrainTimeoutMs:  2000,
	})
	require.ErrorContains(t, err, "different parameters")
}

func testTransferCollection() *model.Collection {
	return &model.Collection{
		CollectionID:         100,
		DBID:                 10,
		DBName:               "db",
		Name:                 "coll",
		State:                etcdpb.CollectionState_CollectionCreated,
		ShardsNum:            1,
		Aliases:              []string{"alias1"},
		VirtualChannelNames:  []string{"by-dev-rootcoord-dml_0_100v0"},
		PhysicalChannelNames: []string{"by-dev-rootcoord-dml_0"},
		Partitions: []*model.Partition{
			{
				PartitionID:   101,
				PartitionName: "_default",
				CollectionID:  100,
				State:         etcdpb.PartitionState_PartitionCreated,
			},
		},
	}
}

func expectSourceAliases(catalog *mocks.RootCoordCatalog, coll *model.Collection) {
	catalog.EXPECT().ListAliases(mock.Anything, coll.DBID, typeutil.MaxTimestamp).Return([]*model.Alias{{
		Name:         "alias1",
		CollectionID: coll.CollectionID,
		State:        etcdpb.AliasState_AliasCreated,
		DbID:         coll.DBID,
	}}, nil).Maybe()
}

func expectTargetAliases(catalog *mocks.RootCoordCatalog, aliases []*model.Alias) {
	catalog.EXPECT().ListAliases(mock.Anything, int64(10), typeutil.MaxTimestamp).Return(aliases, nil).Maybe()
}

type recordingTransferRootCoord struct {
	calls      *[]string
	prepare    *rootcoordpb.CatalogTransferPrepareRequest
	deactivate *rootcoordpb.CatalogTransferDeactivateRequest
	apply      *rootcoordpb.CatalogTransferApplyRequest
	abort      *rootcoordpb.CatalogTransferAbortRequest
	applyErr   error
	abortErr   error
}

func newRecordingTransferRootCoord(calls *[]string) *recordingTransferRootCoord {
	return &recordingTransferRootCoord{calls: calls}
}

func (r *recordingTransferRootCoord) record(call string) {
	if r.calls != nil {
		*r.calls = append(*r.calls, call)
	}
}

func (r *recordingTransferRootCoord) CatalogTransferPrepare(ctx context.Context, req *rootcoordpb.CatalogTransferPrepareRequest) error {
	r.record("root-prepare")
	r.prepare = req
	return nil
}

func (r *recordingTransferRootCoord) CatalogTransferDeactivate(ctx context.Context, req *rootcoordpb.CatalogTransferDeactivateRequest) error {
	r.record("root-deactivate")
	r.deactivate = req
	return nil
}

func (r *recordingTransferRootCoord) CatalogTransferApply(ctx context.Context, req *rootcoordpb.CatalogTransferApplyRequest) error {
	r.record("root-apply")
	r.apply = req
	return r.applyErr
}

func (r *recordingTransferRootCoord) CatalogTransferAbort(ctx context.Context, req *rootcoordpb.CatalogTransferAbortRequest) error {
	r.record("root-abort")
	r.abort = req
	return r.abortErr
}

type assertErr string

func (e assertErr) Error() string { return string(e) }
