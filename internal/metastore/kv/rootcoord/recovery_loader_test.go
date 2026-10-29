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
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	tikvkv "github.com/milvus-io/milvus/internal/kv/tikv"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tikv"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type recoveryWalkRecorder struct {
	mu    sync.Mutex
	calls map[string]int
}

func newRecoveryMetaKV(
	t *testing.T,
	records map[string][]byte,
	recorder *recoveryWalkRecorder,
) *mocks.MetaKv {
	t.Helper()
	const rootPath = "test-root"

	metaKV := mocks.NewMetaKv(t)
	metaKV.On("GetPath", mock.Anything).Return(func(key string) string {
		return rootPath + "/" + key
	})
	metaKV.On(
		"WalkWithPrefix",
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(func(
		ctx context.Context,
		prefix string,
		_ int,
		fn func([]byte, []byte) error,
	) error {
		recorder.mu.Lock()
		recorder.calls[prefix]++
		recorder.mu.Unlock()

		keys := make([]string, 0, len(records))
		for key := range records {
			if strings.HasPrefix(key, prefix) {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		for _, key := range keys {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := fn([]byte(rootPath+"/"+key), records[key]); err != nil {
				return err
			}
		}
		return nil
	})
	return metaKV
}

func marshalRecoveryProto(t *testing.T, value proto.Message) []byte {
	t.Helper()
	bytes, err := proto.Marshal(value)
	require.NoError(t, err)
	return bytes
}

func findRecoveryCollection(t *testing.T, collections []*model.Collection, collectionID int64) *model.Collection {
	t.Helper()
	for _, collection := range collections {
		if collection.CollectionID == collectionID {
			return collection
		}
	}
	t.Fatalf("collection %d not found", collectionID)
	return nil
}

func TestCatalogListCollectionsForRecoverySkipsChildScansForEmbeddedMetadata(t *testing.T) {
	collection := &pb.CollectionInfo{
		ID:                         100,
		DbId:                       1,
		PartitionIDs:               []int64{10},
		PartitionNames:             []string{"p100"},
		PartitionCreatedTimestamps: []uint64{1},
		Schema: &schemapb.CollectionSchema{
			Name:   "c100",
			Fields: []*schemapb.FieldSchema{{FieldID: 11, Name: "f100"}},
		},
		State: pb.CollectionState_CollectionCreated,
	}
	records := map[string][]byte{
		CollectionInfoMetaPrefix + "/1/100": marshalRecoveryProto(t, collection),
	}
	recorder := &recoveryWalkRecorder{calls: make(map[string]int)}
	metaKV := newRecoveryMetaKV(t, records, recorder)
	catalog := NewCatalog(metaKV).(*Catalog)

	collectionsByDB, err := catalog.ListCollectionsForRecovery(context.Background(), []int64{1}, 0)
	require.NoError(t, err)
	require.Len(t, collectionsByDB[1], 1)
	require.Len(t, collectionsByDB[1][0].Partitions, 1)
	require.Len(t, collectionsByDB[1][0].Fields, 1)
	assert.Equal(t, 1, recorder.calls[CollectionInfoMetaPrefix+"/"])
	assert.Zero(t, recorder.calls[CollectionMetaPrefix+"/"])
	assert.Zero(t, recorder.calls[PartitionMetaPrefix+"/"])
	assert.Zero(t, recorder.calls[FieldMetaPrefix+"/"])
	assert.Zero(t, recorder.calls[StructArrayFieldMetaPrefix+"/"])
	assert.Zero(t, recorder.calls[FunctionMetaPrefix+"/"])
}

func TestCatalogListCollectionsForRecoveryRequiresPrefixWalker(t *testing.T) {
	catalog := NewCatalog(mocks.NewTxnKV(t)).(*Catalog)

	_, err := catalog.ListCollectionsForRecovery(context.Background(), []int64{util.DefaultDBID}, 0)
	assert.ErrorIs(t, err, merr.ErrServiceUnimplemented)
}

func TestCatalogListCollectionsForRecoveryRejectsCorruptTargetMetadata(t *testing.T) {
	records := map[string][]byte{
		CollectionInfoMetaPrefix + "/1/100": []byte("corrupt-collection-metadata"),
	}
	metaKV := newRecoveryMetaKV(t, records, &recoveryWalkRecorder{calls: make(map[string]int)})
	catalog := NewCatalog(metaKV).(*Catalog)

	_, err := catalog.ListCollectionsForRecovery(context.Background(), []int64{1}, 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrDataIntegrity)
}

func TestCatalogListCollectionsForRecoveryRejectsCorruptTargetChildMetadata(t *testing.T) {
	collection := &pb.CollectionInfo{
		ID:     100,
		DbId:   1,
		Schema: &schemapb.CollectionSchema{Name: "c100"},
		State:  pb.CollectionState_CollectionCreated,
	}
	records := map[string][]byte{
		CollectionInfoMetaPrefix + "/1/100": marshalRecoveryProto(t, collection),
		FieldMetaPrefix + "/100/10":         []byte("corrupt-field-metadata"),
	}
	metaKV := newRecoveryMetaKV(t, records, &recoveryWalkRecorder{calls: make(map[string]int)})
	catalog := NewCatalog(metaKV).(*Catalog)

	_, err := catalog.ListCollectionsForRecovery(context.Background(), []int64{1}, 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrDataIntegrity)
}

func TestCatalogListCollectionsForRecoveryPreservesScanError(t *testing.T) {
	injected := merr.WrapErrIoFailedReason("injected recovery scan failure", "test")
	metaKV := mocks.NewMetaKv(t)
	metaKV.On("GetPath", mock.Anything).Return(func(key string) string {
		return "test-root/" + key
	})
	metaKV.On(
		"WalkWithPrefix",
		mock.Anything,
		CollectionInfoMetaPrefix+"/",
		mock.Anything,
		mock.Anything,
	).Return(injected).Once()
	catalog := NewCatalog(metaKV).(*Catalog)

	_, err := catalog.ListCollectionsForRecovery(context.Background(), []int64{1}, 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, injected)
	assert.ErrorIs(t, err, merr.ErrIoFailed)
}

func TestCatalogListCollectionsForRecoveryHonorsCancellationDuringWalk(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	metaKV := mocks.NewMetaKv(t)
	metaKV.On("GetPath", mock.Anything).Return(func(key string) string {
		return "test-root/" + key
	})
	metaKV.On(
		"WalkWithPrefix",
		mock.Anything,
		CollectionInfoMetaPrefix+"/",
		mock.Anything,
		mock.Anything,
	).Return(func(
		_ context.Context,
		_ string,
		_ int,
		fn func([]byte, []byte) error,
	) error {
		cancel()
		return fn(
			[]byte("test-root/"+CollectionInfoMetaPrefix+"/1/100"),
			[]byte("unused"),
		)
	}).Once()
	catalog := NewCatalog(metaKV).(*Catalog)

	_, err := catalog.ListCollectionsForRecovery(ctx, []int64{1}, 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestParseRecoveryKeyID(t *testing.T) {
	prefix := []byte("root/root-coord/partitions/")
	cases := []struct {
		key string
		id  int64
		ok  bool
	}{
		{key: "root/root-coord/partitions/100/1", id: 100, ok: true},
		{key: "root/root-coord/partitions/100/", id: 100, ok: true},
		{key: "root/root-coord/partitions/100/1/extra", id: 100, ok: true},
		{key: "root/root-coord/partitions/-5/1", id: -5, ok: true},
		{key: "root/root-coord/partitions/100", ok: false},
		{key: "root/root-coord/partitions//1", ok: false},
		{key: "root/root-coord/partitions/0100/1", ok: false},
		{key: "root/root-coord/partitions/+100/1", ok: false},
		{key: "root/root-coord/partitions/abc/1", ok: false},
		{key: "root/root-coord/partitions/99999999999999999999/1", ok: false},
		{key: "root/root-coord/fields/100/1", ok: false},
	}
	for _, c := range cases {
		id, ok := parseRecoveryKeyID(prefix, []byte(c.key))
		assert.Equal(t, c.ok, ok, c.key)
		assert.Equal(t, c.id, id, c.key)
	}
}

// countingMetaKV counts the prefix reads issued to a metadata store.
type countingMetaKV struct {
	kv.MetaKv
	loadWithPrefix atomic.Int64
	walkWithPrefix atomic.Int64
}

func (c *countingMetaKV) LoadWithPrefix(ctx context.Context, key string) ([]string, []string, error) {
	c.loadWithPrefix.Add(1)
	return c.MetaKv.LoadWithPrefix(ctx, key)
}

func (c *countingMetaKV) WalkWithPrefix(ctx context.Context, prefix string, paginationSize int, fn func([]byte, []byte) error) error {
	c.walkWithPrefix.Add(1)
	return c.MetaKv.WalkWithPrefix(ctx, prefix, paginationSize, fn)
}

func newTiKVRecoveryStore(t *testing.T) kv.MetaKv {
	client := tikv.SetupLocalTxn()
	t.Cleanup(func() { client.Close() })
	return tikvkv.NewTiKV(client, fmt.Sprintf("test/meta/rootcoord-recovery-%d", rand.Int()))
}

// seedRecoveryFixture writes collections in every layout ListCollections
// reads, plus keys that no per-database or per-collection read returns.
func seedRecoveryFixture(t *testing.T, store kv.MetaKv) {
	marshal := func(value proto.Message) string { return string(marshalRecoveryProto(t, value)) }
	// childKeys stores partitions and fields under the child prefixes.
	childKeys := func(id, dbID int64, name string) string {
		return marshal(&pb.CollectionInfo{
			ID:                   id,
			DbId:                 dbID,
			Schema:               &schemapb.CollectionSchema{Name: name, Description: "child keys"},
			State:                pb.CollectionState_CollectionCreated,
			VirtualChannelNames:  []string{fmt.Sprintf("dml_0_%dv0", id)},
			PhysicalChannelNames: []string{"dml_0"},
			ShardsNum:            1,
			Properties:           []*commonpb.KeyValuePair{{Key: "k", Value: name}},
		})
	}
	// embedded stores partitions and fields in the collection value.
	embedded := func(id, dbID int64, name string) string {
		return marshal(&pb.CollectionInfo{
			ID:                         id,
			DbId:                       dbID,
			PartitionIDs:               []int64{id*10 + 1},
			PartitionNames:             []string{name + "-partition"},
			PartitionCreatedTimestamps: []uint64{7},
			Schema: &schemapb.CollectionSchema{
				Name:   name,
				Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: name + "-pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64}},
			},
			State: pb.CollectionState_CollectionCreated,
		})
	}
	partition := func(collectionID, partitionID int64) string {
		return marshal(&pb.PartitionInfo{
			CollectionId:              collectionID,
			PartitionID:               partitionID,
			PartitionName:             fmt.Sprintf("p%d", partitionID),
			PartitionCreatedTimestamp: uint64(partitionID),
			State:                     pb.PartitionState_PartitionCreated,
		})
	}
	field := func(fieldID int64) string {
		return marshal(&schemapb.FieldSchema{
			FieldID:    fieldID,
			Name:       fmt.Sprintf("f%d", fieldID),
			DataType:   schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}},
		})
	}
	structField := func(fieldID int64) string {
		return marshal(&schemapb.StructArrayFieldSchema{
			FieldID: fieldID,
			Name:    fmt.Sprintf("s%d", fieldID),
			Fields:  []*schemapb.FieldSchema{{FieldID: fieldID + 1, Name: "sub", DataType: schemapb.DataType_Array}},
		})
	}
	function := func(functionID int64) string {
		return marshal(&schemapb.FunctionSchema{
			Id:               functionID,
			Name:             fmt.Sprintf("fn%d", functionID),
			Type:             schemapb.FunctionType_BM25,
			InputFieldIds:    []int64{1},
			OutputFieldNames: []string{"sparse"},
		})
	}
	// mixed embeds its partitions but stores its fields under the child
	// prefixes, so its child keys replace the embedded partitions.
	mixed := marshal(&pb.CollectionInfo{
		ID:                         102,
		DbId:                       util.DefaultDBID,
		PartitionIDs:               []int64{1021},
		PartitionNames:             []string{"embedded-partition"},
		PartitionCreatedTimestamps: []uint64{1},
		Schema:                     &schemapb.CollectionSchema{Name: "mixed"},
		State:                      pb.CollectionState_CollectionCreated,
	})

	records := map[string]string{
		// Stored without a database. 300 is migrated to the default database.
		BuildCollectionKey(util.NonDBID, 300): childKeys(300, util.NonDBID, "legacy-child-keys"),
		BuildCollectionKey(util.NonDBID, 301): embedded(301, util.DefaultDBID, "legacy-embedded"),
		BuildCollectionKey(util.NonDBID, 302): tombstone(),

		// Default database. 9 and 10 sort as "10" < "9".
		BuildCollectionKey(util.DefaultDBID, 9):   childKeys(9, util.DefaultDBID, "c9"),
		BuildCollectionKey(util.DefaultDBID, 10):  childKeys(10, util.DefaultDBID, "c10"),
		BuildCollectionKey(util.DefaultDBID, 100): childKeys(100, util.DefaultDBID, "c100"),
		BuildCollectionKey(util.DefaultDBID, 101): embedded(101, util.DefaultDBID, "c101"),
		BuildCollectionKey(util.DefaultDBID, 102): mixed,
		BuildCollectionKey(util.DefaultDBID, 103): tombstone(),

		// Database 2, and database 10 which is not recovered.
		BuildCollectionKey(2, 1000):  childKeys(1000, 2, "c1000"),
		BuildCollectionKey(2, 1001):  tombstone(),
		BuildCollectionKey(10, 2000): childKeys(2000, 10, "c2000"),

		BuildPartitionKey(9, 1):            partition(9, 1),
		BuildPartitionKey(9, 2):            partition(9, 2),
		BuildPartitionKey(9, 10):           partition(9, 10),
		BuildPartitionKey(9, 11):           tombstone(),
		BuildFieldKey(9, 100):              field(100),
		BuildFieldKey(9, 101):              field(101),
		BuildFieldKey(9, 11):               field(11),
		BuildFieldKey(9, 0):                "",
		BuildStructArrayFieldKey(9, 200):   structField(200),
		BuildFunctionKey(9, 300):           function(300),
		BuildFunctionKey(9, 301):           tombstone(),
		BuildPartitionKey(10, 1):           partition(10, 1),
		BuildFieldKey(10, 100):             field(100),
		BuildPartitionKey(100, 1):          partition(100, 1),
		BuildFieldKey(100, 100):            field(100),
		BuildFunctionKey(101, 1):           function(1),
		BuildPartitionKey(102, 1022):       partition(102, 1022),
		BuildFieldKey(102, 100):            field(100),
		BuildPartitionKey(300, 1):          partition(300, 1),
		BuildFieldKey(300, 100):            field(100),
		BuildStructArrayFieldKey(300, 200): structField(200),
		BuildFunctionKey(300, 1):           function(1),
		BuildPartitionKey(1000, 1):         partition(1000, 1),
		BuildFieldKey(1000, 100):           field(100),
		BuildFieldKey(2000, 100):           field(100),

		// No per-database or per-collection read returns these. They must be
		// skipped without being decoded.
		BuildPartitionKey(999, 1):                   "corrupt",
		BuildFieldKey(999, 1):                       "corrupt",
		CollectionInfoMetaPrefix + "/not-a-db/1000": "corrupt",
		FieldMetaPrefix + "/not-a-collection/1":     "corrupt",
		PartitionMetaPrefix + "/0100/1":             "corrupt",
	}
	for key, value := range records {
		require.NoError(t, store.Save(context.Background(), key, value))
	}
}

func dumpRecoveryStore(t *testing.T, store kv.MetaKv) map[string]string {
	keys, values, err := store.LoadWithPrefix(context.Background(), ComponentPrefix+"/")
	require.NoError(t, err)
	rootPrefix := strings.TrimSuffix(store.GetPath(ComponentPrefix+"/"), ComponentPrefix+"/")
	dump := make(map[string]string, len(keys))
	for i, key := range keys {
		dump[strings.TrimPrefix(key, rootPrefix)] = values[i]
	}
	return dump
}

func recoveryJSON(t *testing.T, collectionsByDB map[int64][]*model.Collection) string {
	encoded, err := json.Marshal(collectionsByDB)
	require.NoError(t, err)
	return string(encoded)
}

func TestCatalogListCollectionsForRecoveryMatchesPerDatabaseReads(t *testing.T) {
	params := paramtable.Get()
	params.Save(params.MetaStoreCfg.PaginationSize.Key, "2")
	defer params.Reset(params.MetaStoreCfg.PaginationSize.Key)

	// The order of MetaTable.reload: collections without a database first.
	dbIDs := []int64{util.NonDBID, util.DefaultDBID, 2, 3}
	stores := map[string]func(t *testing.T) kv.MetaKv{
		"etcd": func(t *testing.T) kv.MetaKv {
			store, cleanup := newTombstoneTestMetaKV(t)
			t.Cleanup(cleanup)
			return store
		},
		"tikv": newTiKVRecoveryStore,
	}
	for name, newStore := range stores {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			pointStore := newStore(t)
			batchStore := &countingMetaKV{MetaKv: newStore(t)}
			seedRecoveryFixture(t, pointStore)
			seedRecoveryFixture(t, batchStore)

			pointCatalog := NewCatalog(pointStore).(*Catalog)
			expected := make(map[int64][]*model.Collection, len(dbIDs))
			for _, dbID := range dbIDs {
				collections, err := pointCatalog.ListCollections(ctx, dbID, typeutil.MaxTimestamp)
				require.NoError(t, err)
				expected[dbID] = collections
			}

			batchCatalog := NewCatalog(batchStore).(*Catalog)
			actual, err := batchCatalog.ListCollectionsForRecovery(ctx, dbIDs, typeutil.MaxTimestamp)
			require.NoError(t, err)

			assert.Equal(t, recoveryJSON(t, expected), recoveryJSON(t, actual))
			require.Len(t, actual[util.NonDBID], 2)
			require.Len(t, actual[util.DefaultDBID], 6)
			require.Len(t, actual[2], 1)
			require.NotNil(t, actual[3])
			assert.Empty(t, actual[3])
			assert.NotContains(t, actual, int64(10))
			c9 := findRecoveryCollection(t, actual[util.DefaultDBID], 9)
			assert.Len(t, c9.Partitions, 3)
			assert.Len(t, c9.Fields, 4)
			assert.Len(t, c9.StructArrayFields, 1)
			assert.Len(t, c9.Functions, 1)

			// The migrated legacy collection is returned twice; the two
			// collections must not share child models.
			legacy := findRecoveryCollection(t, actual[util.NonDBID], 300)
			migrated := findRecoveryCollection(t, actual[util.DefaultDBID], 300)
			require.Len(t, legacy.Partitions, 1)
			require.Len(t, legacy.Fields, 1)
			require.Len(t, legacy.StructArrayFields, 1)
			require.Len(t, legacy.Functions, 1)
			assert.NotSame(t, legacy.Partitions[0], migrated.Partitions[0])
			assert.NotSame(t, legacy.Fields[0], migrated.Fields[0])
			assert.NotSame(t, legacy.StructArrayFields[0], migrated.StructArrayFields[0])
			assert.NotSame(t, legacy.Functions[0], migrated.Functions[0])

			// Both paths migrate the legacy collections in the same way.
			assert.Equal(t, dumpRecoveryStore(t, pointStore), dumpRecoveryStore(t, batchStore.MetaKv))

			// One scan per prefix, and no per-database or per-collection reads.
			assert.Zero(t, batchStore.loadWithPrefix.Load())
			assert.EqualValues(t, 6, batchStore.walkWithPrefix.Load())
		})
	}
}
