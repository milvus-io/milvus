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
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/pkg/v3/common"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	viewTestDBID   = int64(7)
	viewTestDBName = "db_view"
	viewTestCollID = int64(100)
)

func newViewTestMetaTable(t *testing.T, catalog *mocks.RootCoordCatalog) *MetaTable {
	channel.ResetStaticPChannelStatsManager()
	channel.RecoverPChannelStatsManager(nil)
	t.Cleanup(channel.ResetStaticPChannelStatsManager)
	return &MetaTable{
		ctx:     context.Background(),
		catalog: catalog,
		dbName2Meta: map[string]*model.Database{
			viewTestDBName: {ID: viewTestDBID, Name: viewTestDBName},
		},
		collID2Meta:                  map[typeutil.UniqueID]*model.Collection{},
		partitionName2ID:             map[int64]map[string]int64{},
		availableCollectionCountByDB: map[int64]int{},
		fileResourceRefCnt:           map[int64]int{},
		names:                        newNameDb(),
		aliases:                      newNameDb(),
	}
}

func newViewTestCollection() *model.Collection {
	return &model.Collection{
		CollectionID: viewTestCollID,
		DBID:         viewTestDBID,
		// Stale on purpose: the view takes the name resolved from DBID.
		DBName:              "stale",
		Name:                "coll",
		State:               pb.CollectionState_CollectionCreated,
		CreateTime:          1000,
		ShardsNum:           1,
		ConsistencyLevel:    commonpb.ConsistencyLevel_Bounded,
		VirtualChannelNames: []string{"by-dev-rootcoord-dml_0_100v0"},
		Fields: []*model.Field{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		Partitions: []*model.Partition{
			{PartitionID: 1, PartitionName: "_default", CollectionID: viewTestCollID, State: pb.PartitionState_PartitionCreated},
		},
		Properties: []*commonpb.KeyValuePair{{Key: "k", Value: "v"}},
	}
}

// assertViewMatchesModel checks the published view against one built from
// the current model, and its schema against what describe returns.
func assertViewMatchesModel(t *testing.T, mt *MetaTable, collectionID int64) *metacache.CollectionInfo {
	t.Helper()
	view, ok := mt.CollectionSource().GetCollection(collectionID)
	require.True(t, ok)
	coll := mt.collID2Meta[collectionID]
	require.NotNil(t, coll)
	want := mt.buildCollectionViewLocked(coll)
	assert.True(t, proto.Equal(want.Schema, view.Schema))
	// Compare the rest on copies: the published view must stay untouched.
	// PropertyPairs holds protos too; it is checked against describe below.
	wantRest, gotRest := *want, *view
	wantRest.Schema, gotRest.Schema = nil, nil
	wantRest.PropertyPairs, gotRest.PropertyPairs = nil, nil
	assert.Equal(t, wantRest, gotRest)
	desc := convertModelToDesc(coll, nil, viewTestDBName)
	assert.True(t, proto.Equal(desc.GetSchema(), view.Schema))
	assert.Equal(t, desc.GetShardsNum(), view.ShardsNum)
	assert.Equal(t, desc.GetConsistencyLevel(), view.ConsistencyLevel)
	assert.Equal(t, desc.GetUpdateTimestamp(), view.UpdateTimestamp)
	assert.True(t, slices.EqualFunc(desc.GetProperties(), view.PropertyPairs, func(a, b *commonpb.KeyValuePair) bool {
		return proto.Equal(a, b)
	}))
	return view
}

func TestCollectionView_FollowsDDL(t *testing.T) {
	catalog := mocks.NewRootCoordCatalog(t)
	catalog.EXPECT().CreateCollection(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().CreatePartition(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().AlterPartition(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().DropPartition(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().AlterCollection(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().DeleteGrantByCollectionName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().DropCollection(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mt := newViewTestMetaTable(t, catalog)

	var added, removed, droppingIDs []int64
	mt.CollectionSource().OnCollectionAdded(func(id int64) { added = append(added, id) })
	mt.CollectionSource().OnCollectionRemoved(func(id int64) { removed = append(removed, id) })
	mt.CollectionSource().OnCollectionDropping(func(id int64) { droppingIDs = append(droppingIDs, id) })

	ctx := context.Background()
	steps := []struct {
		name     string
		apply    func() error
		absent   bool
		state    pb.CollectionState
		parts    []int64
		dropping []int64
		names    map[string]int64
		added    []int64
		removed  []int64
	}{
		{
			name:  "create collection",
			apply: func() error { return mt.AddCollection(ctx, newViewTestCollection()) },
			state: pb.CollectionState_CollectionCreated, parts: []int64{1}, names: map[string]int64{"_default": 1}, added: []int64{viewTestCollID},
		},
		{
			name:  "create collection again",
			apply: func() error { return mt.AddCollection(ctx, newViewTestCollection()) },
			state: pb.CollectionState_CollectionCreated, parts: []int64{1}, names: map[string]int64{"_default": 1}, added: []int64{viewTestCollID},
		},
		{
			name: "create partition",
			apply: func() error {
				return mt.AddPartition(ctx, &model.Partition{
					PartitionID: 2, PartitionName: "p2", CollectionID: viewTestCollID,
					State: pb.PartitionState_PartitionCreated,
				})
			},
			state: pb.CollectionState_CollectionCreated, parts: []int64{1, 2}, names: map[string]int64{"_default": 1, "p2": 2}, added: []int64{viewTestCollID},
		},
		{
			name:  "drop partition",
			apply: func() error { return mt.DropPartition(ctx, viewTestCollID, 2, 2000) },
			state: pb.CollectionState_CollectionCreated, parts: []int64{1}, dropping: []int64{2}, names: map[string]int64{"_default": 1, "p2": 2}, added: []int64{viewTestCollID},
		},
		{
			name:  "remove partition",
			apply: func() error { return mt.RemovePartition(ctx, viewTestCollID, 2, 2001) },
			state: pb.CollectionState_CollectionCreated, parts: []int64{1}, names: map[string]int64{"_default": 1}, added: []int64{viewTestCollID},
		},
		{
			name:  "drop collection",
			apply: func() error { return mt.DropCollection(ctx, viewTestCollID, 3000) },
			state: pb.CollectionState_CollectionDropping, parts: []int64{1}, names: map[string]int64{"_default": 1}, added: []int64{viewTestCollID},
		},
		{
			name:   "remove collection",
			apply:  func() error { return mt.RemoveCollection(ctx, viewTestCollID, 3001) },
			absent: true, added: []int64{viewTestCollID}, removed: []int64{viewTestCollID},
		},
	}
	for _, step := range steps {
		require.NoError(t, step.apply(), step.name)
		assert.Equal(t, step.added, added, step.name)
		assert.Equal(t, step.removed, removed, step.name)
		if step.absent {
			_, ok := mt.CollectionSource().GetCollection(viewTestCollID)
			assert.False(t, ok, step.name)
			assert.Empty(t, mt.CollectionSource().GetCollectionIDs(), step.name)
			continue
		}
		view := assertViewMatchesModel(t, mt, viewTestCollID)
		assert.Equal(t, step.state, view.State, step.name)
		assert.Equal(t, step.parts, view.Partitions, step.name)
		assert.Equal(t, step.dropping, view.DroppingPartitions, step.name)
		assert.Equal(t, step.names, view.PartitionIDsByName, step.name)
		assert.Equal(t, viewTestDBName, view.DatabaseName, step.name)
		assert.Equal(t, viewTestDBName, view.Schema.GetDbName(), step.name)
		assert.Equal(t, map[string]string{"k": "v"}, view.Properties, step.name)
		assert.Equal(t, []int64{viewTestCollID}, mt.CollectionSource().GetCollectionIDs(), step.name)
	}
	assert.Equal(t, []int64{viewTestCollID}, droppingIDs, "dropping fires once, on drop collection")
}

// An alter fires the altered hook with the views before and after it; an
// alter of a missing collection fires nothing.
func TestCollectionView_AlteredHook(t *testing.T) {
	catalog := mocks.NewRootCoordCatalog(t)
	catalog.EXPECT().CreateCollection(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().AlterCollection(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mt := newViewTestMetaTable(t, catalog)

	type altered struct{ old, updated *metacache.CollectionInfo }
	var events []altered
	mt.CollectionSource().OnCollectionAltered(func(old, updated *metacache.CollectionInfo) {
		events = append(events, altered{old, updated})
	})

	ctx := context.Background()
	require.NoError(t, mt.AddCollection(ctx, newViewTestCollection()))
	assert.Empty(t, events, "create is not an alter")

	schema := newViewTestCollection().ToCollectionSchemaPB()
	schema.Version = 7
	require.NoError(t, mt.AlterCollection(ctx, buildAlterCollectionSchemaResult(viewTestCollID, schema, 2500)))
	require.Len(t, events, 1)
	assert.Equal(t, int32(0), events[0].old.Schema.GetVersion())
	assert.Equal(t, int32(7), events[0].updated.Schema.GetVersion())
	current, _ := mt.CollectionSource().GetCollection(viewTestCollID)
	assert.Same(t, current, events[0].updated)

	assert.ErrorIs(t, mt.AlterCollection(ctx, buildAlterCollectionSchemaResult(viewTestCollID+1, schema, 2600)), errAlterCollectionNotFound)
	assert.Len(t, events, 1)
}

func TestCollectionView_DBNameFallback(t *testing.T) {
	mt := newViewTestMetaTable(t, mocks.NewRootCoordCatalog(t))
	coll := newViewTestCollection()

	tests := []struct {
		name   string
		dbID   int64
		dbName string
		want   string
	}{
		{"resolved from DBID", viewTestDBID, "stale", viewTestDBName},
		{"database gone", 99, "dropped_db", "dropped_db"},
		{"database gone, no name", 99, "", util.DefaultDBName},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := coll.Clone()
			c.DBID, c.DBName = tc.dbID, tc.dbName
			view := mt.buildCollectionViewLocked(c)
			assert.Equal(t, tc.want, view.DatabaseName)
			assert.Equal(t, tc.want, view.Schema.GetDbName())
		})
	}
}

func TestCollectionView_PartitionIDsByName(t *testing.T) {
	mt := newViewTestMetaTable(t, mocks.NewRootCoordCatalog(t))
	part := func(id int64, name string, state pb.PartitionState) *model.Partition {
		return &model.Partition{PartitionID: id, PartitionName: name, CollectionID: viewTestCollID, State: state}
	}
	for _, tc := range []struct {
		name  string
		parts []*model.Partition
		want  map[string]int64
	}{
		{"live and dropping", []*model.Partition{
			part(1, "a", pb.PartitionState_PartitionCreated), part(2, "b", pb.PartitionState_PartitionDropping),
		}, map[string]int64{"a": 1, "b": 2}},
		{"live wins a reused name", []*model.Partition{
			part(2, "a", pb.PartitionState_PartitionDropping), part(3, "a", pb.PartitionState_PartitionCreated),
		}, map[string]int64{"a": 3}},
		{"live wins whatever the order", []*model.Partition{
			part(3, "a", pb.PartitionState_PartitionCreated), part(2, "a", pb.PartitionState_PartitionDropping),
		}, map[string]int64{"a": 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			coll := newViewTestCollection()
			coll.Partitions = tc.parts
			assert.Equal(t, tc.want, mt.buildCollectionViewLocked(coll).PartitionIDsByName)
		})
	}
}

func TestCollectionView_ShardsNumFallback(t *testing.T) {
	mt := newViewTestMetaTable(t, mocks.NewRootCoordCatalog(t))
	for _, tc := range []struct {
		name      string
		shardsNum int32
		want      int32
	}{
		{"set", 4, 4},
		{"legacy, one per vchannel", 0, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			coll := newViewTestCollection()
			coll.ShardsNum = tc.shardsNum
			coll.VirtualChannelNames = []string{"v0", "v1"}
			assert.Equal(t, tc.want, mt.buildCollectionViewLocked(coll).ShardsNum)
			assert.Equal(t, tc.want, convertModelToDesc(coll, nil, viewTestDBName).GetShardsNum())
		})
	}
}

func TestCollectionView_ReloadPublishesDropping(t *testing.T) {
	created := newViewTestCollection()
	created.DBID = util.DefaultDBID
	dropping := newViewTestCollection()
	dropping.CollectionID, dropping.Name, dropping.DBID = 101, "dropping", util.DefaultDBID
	dropping.State = pb.CollectionState_CollectionDropping
	dropping.Partitions = []*model.Partition{
		{PartitionID: 3, CollectionID: 101, State: pb.PartitionState_PartitionDropping},
	}

	catalog := mocks.NewRootCoordCatalog(t)
	catalog.EXPECT().ListDatabases(mock.Anything, mock.Anything).Return([]*model.Database{model.NewDefaultDatabase(nil)}, nil)
	catalog.EXPECT().ListCollectionsForRecovery(mock.Anything, mock.Anything, mock.Anything).Return(
		map[int64][]*model.Collection{util.DefaultDBID: {created, dropping}}, nil)
	catalog.EXPECT().ListAliases(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListFileResource(mock.Anything).Return(nil, uint64(0), nil)

	channel.ResetStaticPChannelStatsManager()
	t.Cleanup(channel.ResetStaticPChannelStatsManager)
	mt := &MetaTable{ctx: context.Background(), catalog: catalog}
	require.NoError(t, mt.reload())

	assert.ElementsMatch(t, []int64{viewTestCollID, 101}, mt.CollectionSource().GetCollectionIDs())
	db, ok := mt.CollectionSource().GetDatabase(util.DefaultDBName)
	require.True(t, ok)
	assert.Equal(t, util.DefaultDBName, db.Name)
	view, ok := mt.CollectionSource().GetCollection(101)
	require.True(t, ok)
	assert.Equal(t, pb.CollectionState_CollectionDropping, view.State)
	assert.Empty(t, view.Partitions)
	assert.Equal(t, []int64{3}, view.DroppingPartitions)
	assert.Equal(t, util.DefaultDBName, view.DatabaseName)
	view, ok = mt.CollectionSource().GetCollection(viewTestCollID)
	require.True(t, ok)
	assert.Equal(t, pb.CollectionState_CollectionCreated, view.State)
}

// TestCollectionView_ReadsDoNotWaitOnDDLock blocks a writer's catalog call
// while it holds ddLock and checks that collection reads still return, with
// the pre-DDL view, and see the new view once the writer finishes.
func TestCollectionView_ReadsDoNotWaitOnDDLock(t *testing.T) {
	tests := []struct {
		name    string
		p2State pb.PartitionState // state of partition 2 before the DDL
		block   func(catalog *mocks.RootCoordCatalog, entered, release chan struct{})
		apply   func(mt *MetaTable) error
		before  []int64 // Partitions while the DDL is blocked
		after   []int64 // Partitions after the DDL
	}{
		{
			name:    "create partition",
			p2State: pb.PartitionState_PartitionCreated,
			block: func(catalog *mocks.RootCoordCatalog, entered, release chan struct{}) {
				catalog.EXPECT().CreatePartition(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					RunAndReturn(func(context.Context, int64, *model.Partition, uint64) error {
						close(entered)
						<-release
						return nil
					})
			},
			apply: func(mt *MetaTable) error {
				return mt.AddPartition(context.Background(), &model.Partition{
					PartitionID: 3, PartitionName: "p3", CollectionID: viewTestCollID,
					State: pb.PartitionState_PartitionCreated,
				})
			},
			before: []int64{1, 2},
			after:  []int64{1, 2, 3},
		},
		{
			name:    "remove partition from the tombstone sweeper",
			p2State: pb.PartitionState_PartitionDropping,
			block: func(catalog *mocks.RootCoordCatalog, entered, release chan struct{}) {
				catalog.EXPECT().DropPartition(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					RunAndReturn(func(context.Context, int64, int64, int64, uint64) error {
						close(entered)
						<-release
						return nil
					})
			},
			apply:  func(mt *MetaTable) error { return mt.RemovePartition(context.Background(), viewTestCollID, 2, 2000) },
			before: []int64{1},
			after:  []int64{1},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			catalog := mocks.NewRootCoordCatalog(t)
			mt := newViewTestMetaTable(t, catalog)
			coll := newViewTestCollection()
			coll.Partitions = append(coll.Partitions, &model.Partition{
				PartitionID: 2, PartitionName: "p2", CollectionID: viewTestCollID, State: tc.p2State,
			})
			mt.ddLock.Lock()
			mt.setCollectionLocked(coll)
			mt.ddLock.Unlock()
			viewBefore, _ := mt.CollectionSource().GetCollection(viewTestCollID)

			entered, release := make(chan struct{}), make(chan struct{})
			tc.block(catalog, entered, release)
			done := make(chan error, 1)
			go func() { done <- tc.apply(mt) }()
			<-entered // the writer now holds ddLock

			read := make(chan *metacache.CollectionInfo, 1)
			go func() {
				view, _ := mt.CollectionSource().GetCollection(viewTestCollID)
				mt.CollectionSource().GetCollections()
				mt.CollectionSource().GetCollectionIDs()
				read <- view
			}()
			select {
			case view := <-read:
				assert.Same(t, viewBefore, view)
				assert.Equal(t, tc.before, view.Partitions)
			case <-time.After(5 * time.Second):
				t.Fatal("collection read waited on ddLock")
			}

			close(release)
			require.NoError(t, <-done)
			view, _ := mt.CollectionSource().GetCollection(viewTestCollID)
			assert.Equal(t, tc.after, view.Partitions)
			assert.Empty(t, view.DroppingPartitions)
		})
	}
}

func TestCollectionView_GetCollectionByName(t *testing.T) {
	mt := newViewTestMetaTable(t, mocks.NewRootCoordCatalog(t))
	mt.setDatabaseLocked(&model.Database{ID: viewTestDBID, Name: viewTestDBName})
	mt.setDatabaseLocked(&model.Database{ID: util.DefaultDBID, Name: util.DefaultDBName})
	coll := newViewTestCollection()
	mt.setCollectionLocked(coll)
	mt.names.insert(viewTestDBName, "coll", viewTestCollID)
	mt.aliases.insert(viewTestDBName, "alias", viewTestCollID)
	mt.names.insert(viewTestDBName, "no_view", 999)
	dropping := newViewTestCollection()
	dropping.CollectionID, dropping.State = 101, pb.CollectionState_CollectionDropping
	mt.setCollectionLocked(dropping)
	mt.names.insert(viewTestDBName, "dropping", 101)
	// Mid-rename: the name map already points at the collection, whose view
	// still carries its old name.
	mt.names.insert(viewTestDBName, "renamed", viewTestCollID)

	tests := []struct {
		name   string
		dbName string
		coll   string
		found  bool
	}{
		{"by name", viewTestDBName, "coll", true},
		{"by alias", viewTestDBName, "alias", true},
		{"other database", util.DefaultDBName, "coll", false},
		{"unknown name", viewTestDBName, "missing", false},
		{"name without a view", viewTestDBName, "no_view", false},
		{"dropping collection", viewTestDBName, "dropping", false},
		{"rename not yet published", viewTestDBName, "renamed", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			view, ok := mt.CollectionSource().GetCollectionByName(tc.dbName, tc.coll)
			assert.Equal(t, tc.found, ok)
			if tc.found {
				assert.Equal(t, viewTestCollID, view.ID)
			}
		})
	}
}

func TestDatabaseView_FollowsDDL(t *testing.T) {
	catalog := mocks.NewRootCoordCatalog(t)
	catalog.EXPECT().CreateDatabase(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().AlterDatabase(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().DropDatabase(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mt := newViewTestMetaTable(t, catalog)
	ctx := context.Background()
	props := func(v string) []*commonpb.KeyValuePair {
		return []*commonpb.KeyValuePair{{Key: common.DatabaseReplicaNumber, Value: v}}
	}

	require.NoError(t, mt.CreateDatabase(ctx, model.NewDatabase(9, "db9", pb.DatabaseState_DatabaseCreated, props("1")), 1))
	db, ok := mt.CollectionSource().GetDatabase("db9")
	require.True(t, ok)
	assert.Equal(t, int64(9), db.ID)
	assert.Equal(t, props("1"), db.Properties)
	assert.Equal(t, "db9", mt.dbID2Name[9])

	require.NoError(t, mt.AlterDatabase(ctx, model.NewDatabase(9, "db9", pb.DatabaseState_DatabaseCreated, props("2")), 2))
	db, ok = mt.CollectionSource().GetDatabase("db9")
	require.True(t, ok)
	assert.Equal(t, "2", db.Properties[0].GetValue())

	require.NoError(t, mt.DropDatabase(ctx, "db9", 3))
	_, ok = mt.CollectionSource().GetDatabase("db9")
	assert.False(t, ok)
	assert.NotContains(t, mt.dbID2Name, int64(9))
}
