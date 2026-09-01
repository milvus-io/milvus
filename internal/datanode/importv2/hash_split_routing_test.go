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

package importv2

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func routingTestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "import_routing",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
	}
}

func hashShardInfo(vchannel string, state schemapb.ShardState, residues ...uint64) *schemapb.CollectionShardInfo {
	info := &schemapb.CollectionShardInfo{VchannelName: vchannel, State: state}
	if len(residues) > 0 {
		info.Routing = &schemapb.CollectionShardInfo_HashRouting{
			HashRouting: &schemapb.HashRouting{Buckets: residues},
		}
	}
	return info
}

// splitCollectionTask is the topology a doubling leaves behind: at modulus 4 the
// untouched shard was rebased onto {1,3}, the two targets own {0} and {2}, and
// the fenced source still sits in the vchannel list owning nothing.
func splitCollectionTask(t *testing.T) (*MockTask, []string) {
	vchannels := []string{"src-v0", "keep-v1", "tgt-v2", "tgt-v3"}
	infos := []*schemapb.CollectionShardInfo{
		hashShardInfo("src-v0", schemapb.ShardState_ShardSplitting),
		hashShardInfo("keep-v1", schemapb.ShardState_ShardNormal, 1, 3),
		hashShardInfo("tgt-v2", schemapb.ShardState_ShardCreating, 0),
		hashShardInfo("tgt-v3", schemapb.ShardState_ShardCreating, 2),
	}
	task := NewMockTask(t)
	task.On("GetSchema").Return(routingTestSchema()).Maybe()
	task.On("GetVchannels").Return(vchannels).Maybe()
	task.On("GetShardInfos").Return(infos).Maybe()
	task.On("GetRoutingModulus").Return(uint64(4)).Maybe()
	task.On("GetPartitionIDs").Return([]int64{1}).Maybe()
	return task, vchannels
}

func TestImportRoutesBySplitBucketsNotByPosition(t *testing.T) {
	// An import that routes by position puts rows on shards that do not own
	// their keys. A fan-out read still finds them, so it looks fine -- until a
	// delete of such a row resolves to the shard that does own the key and
	// finds nothing there.
	task, vchannels := splitCollectionTask(t)
	pkField := &schemapb.FieldSchema{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true}

	route, err := vchannelRouter(task, pkField)
	require.NoError(t, err)

	for pk := int64(0); pk < 400; pk++ {
		idx, err := route(pk)
		require.NoError(t, err)
		hash, err := typeutil.Hash32Int64(pk)
		require.NoError(t, err)
		switch vchannels[idx] {
		case "keep-v1":
			assert.Equal(t, uint32(1), hash%2, "keep-v1 owns {2,1}")
		case "tgt-v2":
			assert.Equal(t, uint32(0), hash%4, "tgt-v2 owns {4,0}")
		case "tgt-v3":
			assert.Equal(t, uint32(2), hash%4, "tgt-v3 owns {4,2}")
		default:
			t.Fatalf("row routed to %q, which owns no keys", vchannels[idx])
		}
	}
}

func TestImportNeverRoutesToAFencedSplitSource(t *testing.T) {
	// The fenced source is still in the job's vchannel list -- the list is what
	// the per-channel buffers are indexed by -- but it owns no keys and rejects
	// writes.
	task, vchannels := splitCollectionTask(t)
	pkField := &schemapb.FieldSchema{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true}

	route, err := vchannelRouter(task, pkField)
	require.NoError(t, err)
	for pk := int64(0); pk < 400; pk++ {
		idx, err := route(pk)
		require.NoError(t, err)
		assert.NotEqual(t, "src-v0", vchannels[idx])
	}
}

func TestImportRoutingDisagreesWithTheLegacyModulo(t *testing.T) {
	// Guards the tests above against being vacuous.
	task, _ := splitCollectionTask(t)
	pkField := &schemapb.FieldSchema{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true}

	route, err := vchannelRouter(task, pkField)
	require.NoError(t, err)
	legacy := hashByVChannel(4, pkField)

	differs := false
	for pk := int64(0); pk < 400 && !differs; pk++ {
		idx, err := route(pk)
		require.NoError(t, err)
		if idx != legacy(pk) {
			differs = true
		}
	}
	assert.True(t, differs, "bucket routing must not coincide with hash%%4 over the vchannel list")
}

func TestImportKeepsLegacyRoutingForANeverSplitCollection(t *testing.T) {
	// No routing meta at all: the compatibility path, and it must not move a
	// single row.
	vchannels := []string{"v0", "v1", "v2"}
	task := NewMockTask(t)
	task.On("GetVchannels").Return(vchannels).Maybe()
	task.On("GetShardInfos").Return([]*schemapb.CollectionShardInfo(nil)).Maybe()
	task.On("GetRoutingModulus").Return(uint64(0)).Maybe()
	pkField := &schemapb.FieldSchema{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true}

	route, err := vchannelRouter(task, pkField)
	require.NoError(t, err)
	legacy := hashByVChannel(int64(len(vchannels)), pkField)
	for pk := int64(0); pk < 200; pk++ {
		idx, err := route(pk)
		require.NoError(t, err)
		assert.Equal(t, legacy(pk), idx)
	}
}

func TestImportRoutingRoutesVarCharKeys(t *testing.T) {
	vchannels := []string{"v0", "v1"}
	task := NewMockTask(t)
	task.On("GetVchannels").Return(vchannels).Maybe()
	task.On("GetShardInfos").Return([]*schemapb.CollectionShardInfo{
		hashShardInfo("v0", schemapb.ShardState_ShardNormal, 0),
		hashShardInfo("v1", schemapb.ShardState_ShardNormal, 1),
	}).Maybe()
	task.On("GetRoutingModulus").Return(uint64(2)).Maybe()
	pkField := &schemapb.FieldSchema{FieldID: 100, DataType: schemapb.DataType_VarChar, IsPrimaryKey: true}

	route, err := vchannelRouter(task, pkField)
	require.NoError(t, err)
	for _, key := range []string{"alpha", "beta", "gamma", "delta"} {
		idx, err := route(key)
		require.NoError(t, err)
		assert.Equal(t, int64(typeutil.HashString2Uint32(key)%2), idx)
	}
}

func TestHashDataPlacesRowsOnTheOwningShard(t *testing.T) {
	// End to end through HashData: the per-channel buffers are indexed by the
	// job's vchannel list, so a routed index has to line up with it.
	task, vchannels := splitCollectionTask(t)
	schema := routingTestSchema()
	rows, err := storage.NewInsertData(schema)
	require.NoError(t, err)
	for pk := int64(0); pk < 200; pk++ {
		require.NoError(t, rows.Append(map[int64]interface{}{100: pk}))
	}

	hashed, err := HashData(task, rows)
	require.NoError(t, err)
	require.Len(t, hashed, len(vchannels))

	srcIdx := 0
	assert.Equal(t, "src-v0", vchannels[srcIdx])
	assert.Zero(t, hashed[srcIdx][0].GetRowNum(), "a fenced source must receive no imported row")

	total := 0
	for _, perPartition := range hashed {
		for _, data := range perPartition {
			total += data.GetRowNum()
		}
	}
	assert.Equal(t, 200, total, "no row is dropped or duplicated")
}

func TestImportRoutingCompatibility(t *testing.T) {
	pkField := &schemapb.FieldSchema{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true}
	vchannels := []string{"v0", "v1", "v2"}
	legacy := hashByVChannel(int64(len(vchannels)), pkField)
	sameAsLegacy := func(t *testing.T, route func(pk any) (int64, error)) {
		t.Helper()
		for pk := int64(0); pk < 200; pk++ {
			idx, err := route(pk)
			require.NoError(t, err)
			require.Equal(t, legacy(pk), idx)
		}
	}
	newTask := func(infos []*schemapb.CollectionShardInfo, modulus uint64) *MockTask {
		task := NewMockTask(t)
		task.On("GetVchannels").Return(vchannels).Maybe()
		task.On("GetShardInfos").Return(infos).Maybe()
		task.On("GetRoutingModulus").Return(modulus).Maybe()
		return task
	}

	t.Run("a job with no routing snapshot keeps the legacy modulo", func(t *testing.T) {
		// A job persisted before the field existed, or assembled by an older
		// datacoord mid-upgrade. Its rows must land where that version put them.
		route, err := vchannelRouter(newTask(nil, 0), pkField)
		require.NoError(t, err)
		sameAsLegacy(t, route)
	})

	t.Run("a snapshot with a modulus but no residues keeps the legacy modulo", func(t *testing.T) {
		// The regression this guards: deriving a table from no shards at all is
		// an error, so a job whose snapshot is empty would have died here
		// instead of importing.
		route, err := vchannelRouter(newTask(nil, 4), pkField)
		require.NoError(t, err)
		sameAsLegacy(t, route)
	})

	t.Run("a hash collection that has never been split keeps the legacy modulo", func(t *testing.T) {
		infos := []*schemapb.CollectionShardInfo{
			{VchannelName: "v0", State: schemapb.ShardState_ShardNormal},
			{VchannelName: "v1", State: schemapb.ShardState_ShardNormal},
			{VchannelName: "v2", State: schemapb.ShardState_ShardNormal},
		}
		route, err := vchannelRouter(newTask(infos, 0), pkField)
		require.NoError(t, err)
		sameAsLegacy(t, route)
	})

	t.Run("a snapshot that does not describe the vchannels fails loudly", func(t *testing.T) {
		// Present but inconsistent is corruption, not compatibility: routing by
		// position here would silently scatter a split collection's rows.
		infos := []*schemapb.CollectionShardInfo{
			hashShardInfo("v0", schemapb.ShardState_ShardNormal, 0),
		}
		_, err := vchannelRouter(newTask(infos, 2), pkField)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "does not describe the job's vchannels")
	})
}
