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

package split

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/blang/semver/v4"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// triggerEnv is a trigger over fakes. Issues run inline unless async is set.
type triggerEnv struct {
	t       *testing.T
	catalog *fakeCatalog
	store   *Store
	manager *Manager

	mu          sync.Mutex
	collections []Collection
	sizes       map[string]int64
	rows        map[string]int64
	dataView    map[int64][]string
	pchannels   []string
	nextID      int64
	primary     bool
	roleErr     error
	imports     map[int64]bool
	restores    map[int64]bool
	replicating bool
	replErr     error
	nodes       []NodeVersion
	nodesErr    error
	flushErr    error
	issueErr    error
	allocErr    error
	pchannelErr error
	dataViewErr error
	flushed     []string
	issued      []Request
	// onIssue runs inside Issue, as the ack callback would.
	onIssue func(request Request)
	// onFlush runs inside FlushSource, before the resource key is taken.
	onFlush func()
	// admitted counts the admissions Issue ran under the key.
	admitted int
}

// testRelease is the release the tests declare as the one that ships shard
// split.
var testRelease = semver.MustParse("3.1.0")

// openGates opens the read path and version gates for one test, as layer 6
// and the release will.
func openGates(t *testing.T) {
	SetReadPathReady(true)
	SetMinimalNodeVersion(testRelease)
	t.Cleanup(func() {
		SetReadPathReady(false)
		SetMinimalNodeVersion(semver.Version{})
	})
}

func newTriggerEnv(t *testing.T) *triggerEnv {
	openGates(t)
	catalog := newFakeCatalog()
	env := &triggerEnv{
		t:       t,
		catalog: catalog,
		store:   mustStore(t, catalog),
		collections: []Collection{
			{ID: testCollection, VChannels: []string{testSource, "by-dev-rootcoord-dml_3_100v3"}},
		},
		sizes:     map[string]int64{},
		rows:      map[string]int64{},
		dataView:  map[int64][]string{testCollection: {testSource, "by-dev-rootcoord-dml_3_100v3"}},
		pchannels: []string{"by-dev-rootcoord-dml_0", "by-dev-rootcoord-dml_1", "by-dev-rootcoord-dml_2", "by-dev-rootcoord-dml_3"},
		nextID:    1000,
		primary:   true,
		imports:   map[int64]bool{},
		restores:  map[int64]bool{},
		nodes: []NodeVersion{
			{Role: "streamingnode", ServerID: 1, Version: testRelease},
			{Role: "proxy", ServerID: 2, Version: semver.MustParse("3.2.0")},
		},
	}
	env.manager = NewManager(context.Background(), Dependencies{
		Store: env.store,
		// The default fixture has no remote replication members to wait for.
		CheckParentRetired: func(context.Context, *datapb.SplitShardTask) error { return nil },
		Collections: func() []Collection {
			env.mu.Lock()
			defer env.mu.Unlock()
			return append([]Collection(nil), env.collections...)
		},
		Collection: func(collectionID int64) (Collection, bool) {
			env.mu.Lock()
			defer env.mu.Unlock()
			for _, collection := range env.collections {
				if collection.ID == collectionID {
					return collection, true
				}
			}
			return Collection{}, false
		},
		ShardStats: func(vchannel string) (int64, int64) {
			env.mu.Lock()
			defer env.mu.Unlock()
			return env.sizes[vchannel], env.rows[vchannel]
		},
		DataViewShards: func(_ context.Context, collectionID int64) ([]string, error) {
			return env.dataView[collectionID], env.dataViewErr
		},
		PChannels: func(context.Context) ([]string, error) { return env.pchannels, env.pchannelErr },
		AllocID: func(context.Context) (int64, error) {
			if env.allocErr != nil {
				return 0, env.allocErr
			}
			env.nextID++
			return env.nextID, nil
		},
		IsPrimary:     func(context.Context) (bool, error) { return env.primary, env.roleErr },
		IsReplicating: func(context.Context) (bool, error) { return env.replicating, env.replErr },
		NodeVersions: func(context.Context) ([]NodeVersion, error) {
			env.mu.Lock()
			defer env.mu.Unlock()
			return append([]NodeVersion(nil), env.nodes...), env.nodesErr
		},
		HasPendingImport:  func(_ context.Context, collectionID int64) bool { return env.imports[collectionID] },
		HasPendingRestore: func(_ context.Context, collectionID int64) bool { return env.restores[collectionID] },
		FlushSource: func(_ context.Context, _ int64, vchannel string) error {
			if env.onFlush != nil {
				env.onFlush()
			}
			if env.flushErr != nil {
				return env.flushErr
			}
			env.mu.Lock()
			defer env.mu.Unlock()
			env.flushed = append(env.flushed, vchannel)
			return nil
		},
		// Issue is RootCoord's BroadcastSplitShard: the admission runs first,
		// under the key, and a refusal appends nothing.
		Issue: func(ctx context.Context, request Request, admit func(ctx context.Context) error) error {
			env.admitted++
			if err := admit(ctx); err != nil {
				return err
			}
			if env.onIssue != nil {
				env.onIssue(request)
			}
			if env.issueErr != nil {
				return env.issueErr
			}
			env.mu.Lock()
			defer env.mu.Unlock()
			env.issued = append(env.issued, request)
			return nil
		},
	})
	env.manager.spawn = func(issue func()) { issue() }
	return env
}

func saveParam(t *testing.T, key string, value string) {
	paramtable.Get().Save(key, value)
	t.Cleanup(func() { paramtable.Get().Reset(key) })
}

func enableTrigger(t *testing.T) {
	params := &paramtable.Get().DataCoordCfg
	saveParam(t, params.ShardSplitEnable.Key, "true")
	saveParam(t, params.ShardSplitMaxShardSize.Key, "1")
	saveParam(t, params.ShardSplitMaxShardRows.Key, "1000")
}

const megabyte = int64(1024 * 1024)

func TestTriggerIsOffByDefault(t *testing.T) {
	env := newTriggerEnv(t)
	// A shard far over every default threshold.
	env.sizes[testSource] = 1 << 60
	env.rows[testSource] = 1 << 60
	require.False(t, paramtable.Get().DataCoordCfg.ShardSplitEnable.GetAsBool())
	require.Empty(t, env.manager.TriggerOnce())
	require.Empty(t, env.issued)
	require.Empty(t, env.flushed)
	require.Empty(t, env.catalog.writes())
}

// Both gates refuse by default, whatever dataCoord.shardSplit.enable says.
func TestGatesRefuseByDefault(t *testing.T) {
	require.False(t, ReadPathReady())
	require.True(t, MinimalNodeVersion().Equals(semver.Version{}))
	require.False(t, paramtable.Get().DataCoordCfg.ShardSplitAllowWithReplication.GetAsBool())
}

func TestTriggerGates(t *testing.T) {
	enableTrigger(t)
	ctx := context.Background()

	t.Run("the read path is not ready", func(t *testing.T) {
		env := newTriggerEnv(t)
		env.sizes[testSource] = megabyte
		SetReadPathReady(false)
		require.Empty(t, env.manager.TriggerOnce())
		_, err := env.manager.Split(ctx, testCollection, testSource)
		require.ErrorIs(t, err, merr.ErrOperationNotSupported)
		require.ErrorContains(t, err, "read path")
		require.Empty(t, env.flushed)
		require.Empty(t, env.issued)
	})

	t.Run("no release ships the feature", func(t *testing.T) {
		env := newTriggerEnv(t)
		env.sizes[testSource] = megabyte
		SetMinimalNodeVersion(semver.Version{})
		require.Empty(t, env.manager.TriggerOnce())
		_, err := env.manager.Split(ctx, testCollection, testSource)
		require.ErrorIs(t, err, merr.ErrOperationNotSupported)
		require.Empty(t, env.issued)
	})

	t.Run("a node is older than the release", func(t *testing.T) {
		env := newTriggerEnv(t)
		env.sizes[testSource] = megabyte
		env.nodes = append(env.nodes, NodeVersion{Role: "querynode", ServerID: 3, Version: semver.MustParse("3.1.0-beta")})
		require.Empty(t, env.manager.TriggerOnce())
		_, err := env.manager.Split(ctx, testCollection, testSource)
		// A rolling upgrade passes: transient.
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		require.ErrorContains(t, err, "querynode 3")
		require.Empty(t, env.issued)

		// Upgraded: the split goes.
		env.nodes[2].Version = testRelease
		require.Len(t, env.manager.TriggerOnce(), 1)
		require.Len(t, env.issued, 1)
	})

	t.Run("the versions cannot be read", func(t *testing.T) {
		env := newTriggerEnv(t)
		env.nodesErr = errors.New("mock")
		_, err := env.manager.Split(ctx, testCollection, testSource)
		require.ErrorIs(t, err, env.nodesErr)
	})

	t.Run("a cluster with a replicate topology", func(t *testing.T) {
		env := newTriggerEnv(t)
		env.sizes[testSource] = megabyte
		env.replicating = true
		require.Empty(t, env.manager.TriggerOnce())
		_, err := env.manager.Split(ctx, testCollection, testSource)
		require.ErrorIs(t, err, merr.ErrOperationNotSupported)
		require.ErrorContains(t, err, "replicate")

		env.replErr = errors.New("mock")
		_, err = env.manager.Split(ctx, testCollection, testSource)
		require.ErrorIs(t, err, env.replErr)
		env.replErr = nil

		// The explicit override, for a topology whose clusters are all
		// upgraded.
		saveParam(t, paramtable.Get().DataCoordCfg.ShardSplitAllowWithReplication.Key, "true")
		require.Len(t, env.manager.TriggerOnce(), 1)
		require.Len(t, env.issued, 1)
	})

	t.Run("a gate that closes while the source flushes refuses under the key", func(t *testing.T) {
		env := newTriggerEnv(t)
		env.onFlush = func() {
			env.mu.Lock()
			env.nodes = append(env.nodes, NodeVersion{Role: "streamingnode", ServerID: 9, Version: semver.MustParse("3.0.0")})
			env.mu.Unlock()
		}
		_, err := env.manager.Split(ctx, testCollection, testSource)
		require.NoError(t, err)
		require.Equal(t, 1, env.admitted)
		require.Empty(t, env.issued)
	})
}

func TestTriggerSplitsAShardOverTheThresholds(t *testing.T) {
	enableTrigger(t)
	env := newTriggerEnv(t)

	// Nothing is over the thresholds.
	env.sizes[testSource] = megabyte - 1
	env.rows[testSource] = 999
	require.Empty(t, env.manager.TriggerOnce())

	// By size.
	env.sizes[testSource] = megabyte
	started := env.manager.TriggerOnce()
	require.Len(t, started, 1)
	require.Equal(t, env.issued, started)
	request := started[0]
	require.Equal(t, testCollection, request.CollectionID)
	require.Equal(t, testSource, request.Source)
	require.NotZero(t, request.SplitTaskID)
	// The targets are on the pchannels that hold no vchannel of the
	// collection, with names no vchannel of the collection has.
	require.Equal(t, [2]string{"by-dev-rootcoord-dml_1_100v4", "by-dev-rootcoord-dml_2_100v5"}, request.Targets)
	// The source was flushed before the split was issued.
	require.Equal(t, []string{testSource}, env.flushed)
	// The trigger writes nothing: the record is the callback's.
	require.Empty(t, env.catalog.writes())
	require.Empty(t, env.store.List())

	// By rows, on the other shard, without the flush.
	saveParam(t, paramtable.Get().DataCoordCfg.ShardSplitFlushBeforeSplit.Key, "false")
	env.sizes[testSource] = 0
	env.rows["by-dev-rootcoord-dml_3_100v3"] = 1000
	started = env.manager.TriggerOnce()
	require.Len(t, started, 1)
	require.Equal(t, "by-dev-rootcoord-dml_3_100v3", started[0].Source)
	require.Equal(t, []string{testSource}, env.flushed)
}

func TestTriggerRunsOnThePrimaryOnly(t *testing.T) {
	enableTrigger(t)
	env := newTriggerEnv(t)
	env.sizes[testSource] = megabyte

	env.primary = false
	require.Empty(t, env.manager.TriggerOnce())
	_, err := env.manager.Split(context.Background(), testCollection, testSource)
	require.ErrorIs(t, err, merr.ErrOperationNotSupported)

	env.primary, env.roleErr = true, errors.New("mock")
	require.Empty(t, env.manager.TriggerOnce())
	_, err = env.manager.Split(context.Background(), testCollection, testSource)
	require.Error(t, err)
	require.Empty(t, env.issued)
}

func TestTriggerAdmissionRefusals(t *testing.T) {
	enableTrigger(t)
	ctx := context.Background()

	t.Run("an external collection", func(t *testing.T) {
		env := newTriggerEnv(t)
		env.sizes[testSource] = megabyte
		env.collections[0].External = true
		require.Empty(t, env.manager.TriggerOnce())
		_, err := env.manager.Split(ctx, testCollection, testSource)
		require.ErrorIs(t, err, merr.ErrOperationNotSupported)
	})

	t.Run("a namespace collection", func(t *testing.T) {
		env := newTriggerEnv(t)
		env.sizes[testSource] = megabyte
		env.collections[0].Properties = []*commonpb.KeyValuePair{{Key: common.NamespaceShardingEnabledKey, Value: "true"}}
		require.Empty(t, env.manager.TriggerOnce())
		_, err := env.manager.Split(ctx, testCollection, testSource)
		require.Error(t, err)
	})

	// The project owner's ruling: no namespace collection is split, in any
	// namespace mode.
	namespaceCases := []struct {
		name  string
		apply func(collection *Collection)
	}{
		{"namespaces enabled by the schema", func(c *Collection) { c.EnableNamespace = true }},
		{"namespace.mode partition", func(c *Collection) {
			c.Properties = []*commonpb.KeyValuePair{{Key: common.NamespaceModeKey, Value: common.NamespaceModePartition}}
		}},
		{"namespace.mode partition_key on the schema", func(c *Collection) {
			c.SchemaProperties = []*commonpb.KeyValuePair{{Key: common.NamespaceModeKey, Value: common.NamespaceModePartitionKey}}
		}},
		{"namespace sharding on the schema", func(c *Collection) {
			c.SchemaProperties = []*commonpb.KeyValuePair{{Key: common.NamespaceShardingEnabledKey, Value: "true"}}
		}},
	}
	for _, c := range namespaceCases {
		t.Run(c.name, func(t *testing.T) {
			env := newTriggerEnv(t)
			env.sizes[testSource] = megabyte
			c.apply(&env.collections[0])
			require.Empty(t, env.manager.TriggerOnce())
			_, err := env.manager.Split(ctx, testCollection, testSource)
			require.Error(t, err)
			require.Equal(t, merr.SystemError, merr.GetErrorType(err))
			require.Empty(t, env.issued)
		})
	}

	t.Run("a collection without namespaces", func(t *testing.T) {
		env := newTriggerEnv(t)
		env.collections[0].Properties = []*commonpb.KeyValuePair{{Key: common.NamespaceShardingEnabledKey, Value: "false"}}
		require.NoError(t, env.manager.CheckAdmission(ctx, env.collections[0]))
	})

	t.Run("a collection with a split in its window, a target included", func(t *testing.T) {
		env := newTriggerEnv(t)
		_, err := env.store.Record(ctx, testRecord(7))
		require.NoError(t, err)
		// RootCoord lists the targets as the shards now.
		env.collections[0].VChannels = []string{testTarget1, testTarget2, "by-dev-rootcoord-dml_3_100v3"}
		env.sizes[testTarget1] = megabyte
		saveParam(t, paramtable.Get().DataCoordCfg.ShardSplitMaxConcurrent.Key, "10")
		require.Empty(t, env.manager.TriggerOnce())
		_, err = env.manager.Split(ctx, testCollection, testTarget1)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)

		// Until the adoption. Then a target can be split.
		require.NoError(t, env.store.MarkSourceAcked(ctx, 7, testCollection, testSource, nil))
		require.Empty(t, env.manager.TriggerOnce())
		require.NoError(t, env.store.Advance(ctx, 7, datapb.SplitShardTaskState_SplitShardTaskAdopted))
		// Then a target can be split. The pchannels of the source and of both
		// targets stay excluded for the collection, as is the one of the other
		// shard: no pchannel is left for new targets.
		require.Empty(t, env.manager.TriggerOnce())
		_, err = env.manager.Split(ctx, testCollection, testTarget1)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		require.ErrorContains(t, err, "pchannels")

		env.pchannels = append(env.pchannels, "by-dev-rootcoord-dml_4", "by-dev-rootcoord-dml_5")
		started := env.manager.TriggerOnce()
		require.Len(t, started, 1)
		require.Equal(t, testTarget1, started[0].Source)
		require.Equal(t, [2]string{"by-dev-rootcoord-dml_4_100v4", "by-dev-rootcoord-dml_5_100v5"}, started[0].Targets)
	})

	t.Run("a collection with an import job that is not finished", func(t *testing.T) {
		env := newTriggerEnv(t)
		env.sizes[testSource] = megabyte
		env.imports[testCollection] = true
		require.Empty(t, env.manager.TriggerOnce())
		_, err := env.manager.Split(ctx, testCollection, testSource)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	})

	t.Run("a collection with a snapshot restore that is not finished", func(t *testing.T) {
		env := newTriggerEnv(t)
		env.sizes[testSource] = megabyte
		env.restores[testCollection] = true
		require.Empty(t, env.manager.TriggerOnce())
		_, err := env.manager.Split(ctx, testCollection, testSource)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		require.ErrorContains(t, err, "restore")
	})

	t.Run("a collection or a shard that does not exist", func(t *testing.T) {
		env := newTriggerEnv(t)
		_, err := env.manager.Split(ctx, testCollection+1, testSource)
		require.ErrorIs(t, err, merr.ErrCollectionNotFound)
		_, err = env.manager.Split(ctx, testCollection, testTarget1)
		require.ErrorIs(t, err, merr.ErrChannelNotFound)
	})
}

// The admission that decides runs under the resource key of the collection,
// after the flush of the source: what starts while the source flushes is seen.
func TestTriggerAdmitsUnderTheResourceKey(t *testing.T) {
	enableTrigger(t)
	ctx := context.Background()

	cases := []struct {
		name string
		// during runs while the source flushes, before the key is taken.
		during func(env *triggerEnv)
	}{
		{"an import starts", func(env *triggerEnv) { env.imports[testCollection] = true }},
		{"a snapshot restore starts", func(env *triggerEnv) { env.restores[testCollection] = true }},
		{
			// An earlier split, appended before a restart, whose callback ran
			// while this issue waited for the key.
			"an earlier split is recorded", func(env *triggerEnv) {
				_, err := env.store.Record(ctx, testRecord(7))
				require.NoError(env.t, err)
			},
		},
		{
			"the collection is dropped", func(env *triggerEnv) {
				env.mu.Lock()
				env.collections = nil
				env.mu.Unlock()
			},
		},
		{
			"the collection is an external one", func(env *triggerEnv) { env.collections[0].External = true },
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			env := newTriggerEnv(t)
			env.onFlush = func() { c.during(env) }
			request, err := env.manager.Split(ctx, testCollection, testSource)
			// The early check passed and the issue started.
			require.NoError(t, err)
			require.NotZero(t, request.SplitTaskID)
			require.Equal(t, []string{testSource}, env.flushed)
			// Under the key the split is refused: nothing appended.
			require.Equal(t, 1, env.admitted)
			require.Empty(t, env.issued)
			require.Zero(t, env.manager.issuingCount())
			require.Nil(t, env.store.Get(request.SplitTaskID))
		})
	}

	t.Run("nothing started meanwhile", func(t *testing.T) {
		env := newTriggerEnv(t)
		_, err := env.manager.Split(ctx, testCollection, testSource)
		require.NoError(t, err)
		require.Equal(t, 1, env.admitted)
		require.Len(t, env.issued, 1)
	})
}

func TestTriggerLimitsTheSplitsInTheirWindow(t *testing.T) {
	enableTrigger(t)
	ctx := context.Background()
	env := newTriggerEnv(t)
	other := Collection{ID: 200, VChannels: []string{"by-dev-rootcoord-dml_0_200v0"}}
	env.collections = append(env.collections, other)
	env.dataView[200] = other.VChannels
	env.sizes[testSource] = megabyte
	env.sizes["by-dev-rootcoord-dml_0_200v0"] = megabyte
	// The callback records the split while the issue is in flight.
	env.onIssue = func(request Request) {
		record := testRecord(request.SplitTaskID)
		record.CollectionID = request.CollectionID
		record.Source = request.Source
		_, err := env.store.Record(ctx, record)
		require.NoError(t, err)
	}

	// One at a time by default: the second collection waits.
	started := env.manager.TriggerOnce()
	require.Len(t, started, 1)
	require.Equal(t, testCollection, started[0].CollectionID)
	require.Empty(t, env.manager.TriggerOnce())

	saveParam(t, paramtable.Get().DataCoordCfg.ShardSplitMaxConcurrent.Key, "2")
	started = env.manager.TriggerOnce()
	require.Len(t, started, 1)
	require.EqualValues(t, 200, started[0].CollectionID)
}

func TestTriggerCountsAnIssueInFlight(t *testing.T) {
	enableTrigger(t)
	env := newTriggerEnv(t)
	env.sizes[testSource] = megabyte
	var pending []func()
	env.manager.spawn = func(issue func()) { pending = append(pending, issue) }

	require.Len(t, env.manager.TriggerOnce(), 1)
	// The message is not issued yet and nothing is recorded, but the
	// collection is taken and counts against the limit.
	require.Empty(t, env.issued)
	require.Empty(t, env.manager.TriggerOnce())
	_, err := env.manager.Split(context.Background(), testCollection, testSource)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)

	// The callback records the split while the issue is still in flight: the
	// split is one active split, not two.
	env.onIssue = func(request Request) {
		_, err := env.store.Record(context.Background(), testRecord(request.SplitTaskID))
		require.NoError(t, err)
		require.Equal(t, 1, env.manager.activeCount())
	}

	pending[0]()
	require.Len(t, env.issued, 1)
	require.Zero(t, env.manager.issuingCount())
	require.Equal(t, 1, env.manager.activeCount())
}

func TestTriggerPlacementAndIssueFailuresLeaveNothingBehind(t *testing.T) {
	enableTrigger(t)
	ctx := context.Background()

	t.Run("not enough pchannels", func(t *testing.T) {
		env := newTriggerEnv(t)
		env.sizes[testSource] = megabyte
		env.pchannels = env.pchannels[:2]
		require.Empty(t, env.manager.TriggerOnce())
		_, err := env.manager.Split(ctx, testCollection, testSource)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		require.Empty(t, env.issued)
	})

	t.Run("a shard of the DataView that RootCoord no longer lists keeps its pchannel", func(t *testing.T) {
		env := newTriggerEnv(t)
		env.dataView[testCollection] = append(env.dataView[testCollection], "by-dev-rootcoord-dml_1_100v9")
		_, err := env.manager.Split(ctx, testCollection, testSource)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	})

	t.Run("a target takes an index no vchannel of the collection has", func(t *testing.T) {
		env := newTriggerEnv(t)
		env.dataView[testCollection] = append(env.dataView[testCollection], "by-dev-rootcoord-dml_8_100v9", "not-a-vchannel")
		env.pchannels = []string{"by-dev-rootcoord-dml_8", "by-dev-rootcoord-dml_2", "by-dev-rootcoord-dml_2", "by-dev-rootcoord-dml_4"}
		request, err := env.manager.Split(ctx, testCollection, testSource)
		require.NoError(t, err)
		require.Equal(t, [2]string{"by-dev-rootcoord-dml_2_100v10", "by-dev-rootcoord-dml_4_100v11"}, request.Targets)
	})

	t.Run("the dependencies fail", func(t *testing.T) {
		env := newTriggerEnv(t)
		env.dataViewErr = errors.New("mock")
		_, err := env.manager.Split(ctx, testCollection, testSource)
		require.Error(t, err)
		env.dataViewErr, env.pchannelErr = nil, errors.New("mock")
		_, err = env.manager.Split(ctx, testCollection, testSource)
		require.Error(t, err)
		env.pchannelErr, env.allocErr = nil, errors.New("mock")
		_, err = env.manager.Split(ctx, testCollection, testSource)
		require.Error(t, err)
		require.Empty(t, env.issued)
		require.Zero(t, env.manager.issuingCount())
	})

	t.Run("the flush before the split fails: the message is not issued", func(t *testing.T) {
		env := newTriggerEnv(t)
		env.flushErr = errors.New("mock")
		_, err := env.manager.Split(ctx, testCollection, testSource)
		require.NoError(t, err)
		require.Empty(t, env.issued)
		require.Empty(t, env.catalog.writes())
		require.Zero(t, env.manager.issuingCount())
		// The next round tries again.
		env.flushErr = nil
		_, err = env.manager.Split(ctx, testCollection, testSource)
		require.NoError(t, err)
		require.Len(t, env.issued, 1)
	})

	t.Run("the issue fails before the message is appended", func(t *testing.T) {
		env := newTriggerEnv(t)
		env.issueErr = errors.New("mock")
		_, err := env.manager.Split(ctx, testCollection, testSource)
		require.NoError(t, err)
		require.Empty(t, env.store.List())
		require.Empty(t, env.catalog.writes())
		require.Zero(t, env.manager.issuingCount())
	})

	t.Run("the issue returns an error after the split was recorded: the split stays", func(t *testing.T) {
		env := newTriggerEnv(t)
		env.issueErr = errors.New("mock")
		env.onIssue = func(request Request) {
			_, err := env.store.Record(ctx, testRecord(request.SplitTaskID))
			require.NoError(t, err)
		}
		request, err := env.manager.Split(ctx, testCollection, testSource)
		require.NoError(t, err)
		require.NotNil(t, env.store.Get(request.SplitTaskID))
		require.NotNil(t, env.store.WindowOf(testCollection))
	})
}

func TestManagerLoopSweepsStalePendingRecords(t *testing.T) {
	ctx := context.Background()
	env := newTriggerEnv(t)
	saveParam(t, paramtable.Get().DataCoordCfg.ShardSplitCheckInterval.Key, "0")
	// A pending record of a collection that is gone, and one of a live one.
	require.NoError(t, env.store.MarkSourceAcked(ctx, 8, 999, "s8", nil))
	require.NoError(t, env.store.MarkSourceAcked(ctx, 9, testCollection, testSource, nil))
	env.store.mu.Lock()
	env.store.tasks[8].RecordTime = time.Now().Add(-time.Hour).Unix()
	env.store.tasks[9].RecordTime = time.Now().Add(-time.Hour).Unix()
	env.store.mu.Unlock()

	env.manager.Start()
	require.Eventually(t, func() bool { return env.store.Get(8) == nil }, 5*time.Second, 5*time.Millisecond)
	env.manager.Stop()
	require.NotNil(t, env.store.Get(9))
	// The trigger is off: the loop issued nothing.
	require.Empty(t, env.issued)

	// A failed sweep is logged and tried again by the next round.
	env.catalog.dropErr = errors.New("mock")
	require.NoError(t, env.store.MarkSourceAcked(ctx, 10, 999, "s10", nil))
	env.store.mu.Lock()
	env.store.tasks[10].RecordTime = time.Now().Add(-time.Hour).Unix()
	env.store.mu.Unlock()
	env.manager.sweep()
	require.NotNil(t, env.store.Get(10))
}

func TestManagerStopDoesNotWaitForAnIssueInFlight(t *testing.T) {
	enableTrigger(t)
	env := newTriggerEnv(t)
	manager := NewManager(context.Background(), env.manager.deps)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	env.onIssue = func(Request) {
		close(entered)
		<-release
	}
	manager.Start()
	_, err := manager.Split(context.Background(), testCollection, testSource)
	require.NoError(t, err)
	<-entered
	// The issue is blocked, as on the resource key of its collection. Stop
	// returns nevertheless, and the issue ends on its own later.
	manager.Stop()
	require.Equal(t, 1, manager.issuingCount())
	go func() {
		for manager.issuingCount() != 0 {
			time.Sleep(time.Millisecond)
		}
		close(done)
	}()
	close(release)
	<-done
}

func TestSplitCapacityRefusalRunsUnderTheCollectionKey(t *testing.T) {
	enableTrigger(t)
	env := newTriggerEnv(t)
	calls := 0
	env.manager.deps.CheckRewrite = func(context.Context, int64, string) error {
		calls++
		if calls == 1 {
			return nil
		} // early check passes, under-key recheck refuses
		return merr.WrapErrOperationNotSupportedMsg("atomic adoption would exceed transaction capacity")
	}
	_, err := env.manager.Split(context.Background(), testCollection, testSource)
	require.NoError(t, err)
	require.Equal(t, 1, env.admitted)
	require.Empty(t, env.issued, "capacity refusal must precede the SplitShard message")
}

func TestSplitRechecksTargetReadinessBeforeFence(t *testing.T) {
	paramtable.Init()
	env := newTriggerEnv(t)
	// Planning saw both targets, but a pre-flush/lock wait can outlive their
	// assignment. No SplitShard message may be issued from that stale sample.
	saveParam(t, "dataCoord.shardSplit.flushBeforeSplit", "true")
	env.onFlush = func() { env.pchannels = []string{"by-dev-rootcoord-dml_0", "by-dev-rootcoord-dml_3"} }
	_, err := env.manager.Split(t.Context(), testCollection, testSource)
	require.NoError(t, err, "the asynchronous request was accepted before readiness changed")
	require.Empty(t, env.issued, "no fence may be emitted after a target loses readiness")
	require.Equal(t, 1, env.admitted)
}

func TestAdmitChildWaitsForParentRetirementOnPeers(t *testing.T) {
	enableTrigger(t)
	ctx := context.Background()
	env := newTriggerEnv(t)
	record := testRecord(7)
	_, err := env.store.Record(ctx, record)
	require.NoError(t, err)
	require.NoError(t, env.store.MarkSourceAcked(ctx, 7, testCollection, testSource, []string{testTarget1, testTarget2}))
	require.NoError(t, env.store.Advance(ctx, 7, datapb.SplitShardTaskState_SplitShardTaskRetired))
	ready := false
	checks := 0
	env.manager.deps.CheckParentRetired = func(_ context.Context, parent *datapb.SplitShardTask) error {
		checks++
		require.EqualValues(t, 7, parent.GetTaskId())
		if !ready {
			return context.DeadlineExceeded
		}
		return nil
	}
	req := Request{CollectionID: testCollection, Source: testTarget1, Targets: [2]string{testTarget1, testTarget2}}
	require.ErrorIs(t, env.manager.admit(ctx, req), context.DeadlineExceeded)
	ready = true
	require.NoError(t, env.manager.admit(ctx, req))
	req.Source = "independent"
	require.NoError(t, env.manager.admit(ctx, req))
	require.Equal(t, 2, checks)
}
