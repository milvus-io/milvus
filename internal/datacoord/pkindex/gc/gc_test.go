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

package gc

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/pkindex/manifest"
	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

const testRoot = "files"

var testConfig = Config{Interval: time.Minute, Grace: time.Hour, RetainVersions: 2, RetainDuration: time.Hour}

// env is one GC over in-memory stores, with a manager that commits real
// versions and a clock that the test moves.
type env struct {
	now      time.Time
	pointers *manifest.FakeMetaStore
	store    *manifest.FakeObjectStore
	mgr      *manifest.Manager
	gc       *GC
	// inFlight is the in-flight compaction output hook of the GC; nil means none.
	inFlight InFlightOutputs
}

func newEnv(t *testing.T, cfg Config) *env {
	e := &env{now: time.Unix(1_000_000, 0)}
	e.pointers = manifest.NewFakeMetaStore()
	e.store = manifest.NewFakeObjectStore()
	e.store.Now = func() time.Time { return e.now }
	e.mgr = manifest.New(e.pointers, e.store, testRoot)
	g, err := New(e.pointers, e.store, testRoot, cfg, func(ctx context.Context, collectionID int64, vchannel string) ([]int64, error) {
		if e.inFlight == nil {
			return nil, nil
		}
		return e.inFlight(ctx, collectionID, vchannel)
	})
	require.NoError(t, err)
	g.now = func() time.Time { return e.now }
	e.gc = g
	return e
}

// create assigns the pchannel of vchannel at term 1 in the pchannel meta and
// commits an empty first flush at checkpoint 1, which creates version 1.
func (e *env) create(t *testing.T, vchannel string) {
	e.pointers.AssignPChannel(funcutil.ToPhysicalChannel(vchannel), 1)
	flushWith(t, e.mgr, vchannel, 1)
}

func sstKey(vchannel string, id int64) string {
	return pkindex.SSTKey(pkindex.SSTBasePath(testRoot, 1, vchannel), id)
}

func refs(ids ...int64) []*pkindexpb.SSTRef {
	out := []*pkindexpb.SSTRef{}
	for _, id := range ids {
		out = append(out, &pkindexpb.SSTRef{Id: id})
	}
	return out
}

func (e *env) upload(t *testing.T, vchannel string, ids ...int64) {
	for _, id := range ids {
		require.NoError(t, e.store.Write(context.Background(), sstKey(vchannel, id), []byte("x")))
	}
}

func flushWith(t *testing.T, m *manifest.Manager, vchannel string, tick uint64, ids ...int64) {
	_, err := m.CommitFlush(context.Background(), &pkindexpb.CommitFlushRequest{
		CollectionId: 1, Vchannel: vchannel, PchannelTerm: 1,
		AddSsts: refs(ids...), CheckpointTimeTick: tick,
	})
	require.NoError(t, err)
}

func (e *env) flush(t *testing.T, vchannel string, tick uint64, ids ...int64) {
	flushWith(t, e.mgr, vchannel, tick, ids...)
}

func (e *env) has(key string) bool {
	_, err := e.store.Read(context.Background(), key)
	return err == nil
}

// bodies returns the manifest body keys of a vchannel.
func (e *env) bodies(t *testing.T, vchannel string) []string {
	keys := []string{}
	err := e.store.Walk(context.Background(), pkindex.ManifestDir(testRoot, 1, vchannel)+"/", func(k string, _ time.Time) bool {
		keys = append(keys, k)
		return true
	})
	require.NoError(t, err)
	return keys
}

// pointer returns the stored manifest pointer of a vchannel.
func (e *env) pointer(t *testing.T, vchannel string) *pkindexpb.ManifestPointer {
	v, found, err := e.pointers.Load(context.Background(), manifest.PointerKey(1, vchannel))
	require.NoError(t, err)
	require.True(t, found)
	p := &pkindexpb.ManifestPointer{}
	require.NoError(t, proto.Unmarshal([]byte(v), p))
	return p
}

// current returns the current manifest of a vchannel from the stores.
func (e *env) current(t *testing.T, vchannel string) *pkindexpb.Manifest {
	b, err := e.store.Read(context.Background(), e.pointer(t, vchannel).GetBodyKey())
	require.NoError(t, err)
	m := &pkindexpb.Manifest{}
	require.NoError(t, proto.Unmarshal(b, m))
	return m
}

func (e *env) writeBody(t *testing.T, key string, m *pkindexpb.Manifest) {
	b, err := proto.Marshal(m)
	require.NoError(t, err)
	require.NoError(t, e.store.Write(context.Background(), key, b))
}

func TestSSTSweep(t *testing.T) {
	e := newEnv(t, testConfig)
	ctx := context.Background()
	e.create(t, "v")
	e.upload(t, "v", 1, 2, 3, 4, 5)
	e.flush(t, "v", 10, 2, 1)
	_, err := e.mgr.CommitCompaction(ctx, 1, "v", manifest.Compaction{Inputs: []int64{2, 1}, Outputs: refs(3)})
	require.NoError(t, err)
	e.inFlight = func(context.Context, int64, string) ([]int64, error) { return []int64{4}, nil }

	// within the grace window nothing is deleted
	require.NoError(t, e.gc.RunOnce(ctx))
	for _, id := range []int64{1, 2, 3, 4, 5} {
		assert.True(t, e.has(sstKey("v", id)), "id %d", id)
	}

	e.now = e.now.Add(3 * time.Hour)
	require.NoError(t, e.gc.RunOnce(ctx))
	assert.True(t, e.has(sstKey("v", 3)), "referenced by the current version")
	assert.True(t, e.has(sstKey("v", 4)), "output of a running compaction, older than the grace window")
	assert.False(t, e.has(sstKey("v", 5)), "never referenced")
	// 1 and 2 are referenced by the version before the compaction, which is
	// still within RetainVersions
	assert.True(t, e.has(sstKey("v", 1)))
	assert.True(t, e.has(sstKey("v", 2)))
}

func TestBodyRetention(t *testing.T) {
	e := newEnv(t, testConfig)
	ctx := context.Background()
	e.create(t, "v")
	for i := int64(1); i <= 5; i++ {
		e.upload(t, "v", i)
		e.flush(t, "v", uint64(i*10), i)
	}
	assert.Len(t, e.bodies(t, "v"), 6)
	e.now = e.now.Add(3 * time.Hour)
	require.NoError(t, e.gc.RunOnce(ctx))
	assert.Len(t, e.bodies(t, "v"), 2, "keeps RetainVersions bodies")

	cur := e.current(t, "v")
	for _, s := range cur.GetSsts() {
		assert.True(t, e.has(sstKey("v", s.GetId())))
	}
}

func TestBodyRetentionKeepsRecentBodies(t *testing.T) {
	e := newEnv(t, testConfig)
	ctx := context.Background()
	e.create(t, "v")       // version 1
	e.flush(t, "v", 10, 1) // version 2
	e.flush(t, "v", 20, 2) // version 3
	e.now = e.now.Add(2 * time.Hour)
	e.flush(t, "v", 30, 3) // version 4: outside RetainVersions, within RetainDuration
	e.flush(t, "v", 40, 4) // version 5
	e.flush(t, "v", 50, 5) // version 6
	e.now = e.now.Add(30 * time.Minute)

	require.NoError(t, e.gc.RunOnce(ctx))
	got := map[int64]bool{}
	for _, k := range e.bodies(t, "v") {
		v, ok := pkindex.ParseManifestBodyKey(pkindex.ManifestDir(testRoot, 1, "v"), k)
		require.True(t, ok)
		got[v] = true
	}
	assert.Equal(t, map[int64]bool{4: true, 5: true, 6: true}, got)
}

func TestKeepsCASLosersWithinRetainedVersions(t *testing.T) {
	e := newEnv(t, testConfig)
	ctx := context.Background()
	e.create(t, "v")
	e.flush(t, "v", 10, 1)
	e.upload(t, "v", 9)
	loser := pkindex.ManifestBodyKey(pkindex.ManifestDir(testRoot, 1, "v"), 2, "loser")
	e.writeBody(t, loser, &pkindexpb.Manifest{Version: 2, Ssts: refs(9)})
	e.now = e.now.Add(3 * time.Hour)

	require.NoError(t, e.gc.RunOnce(ctx))
	assert.True(t, e.has(loser), "version 2 is within RetainVersions")
	assert.True(t, e.has(sstKey("v", 9)), "referenced by a retained body")
}

func TestGCDoesNotDependOnManagerCache(t *testing.T) {
	e := newEnv(t, testConfig)
	ctx := context.Background()
	e.create(t, "v")
	e.upload(t, "v", 1, 2, 3, 4, 5)
	e.flush(t, "v", 10, 1) // version 2, in the cache of e.mgr

	// another coordinator commits newer versions; the cache of e.mgr is stale
	other := manifest.New(e.pointers, e.store, testRoot)
	flushWith(t, other, "v", 20, 2) // version 3
	// version 4
	_, err := other.CommitCompaction(ctx, 1, "v", manifest.Compaction{Inputs: []int64{2, 1}, Outputs: refs(3)})
	require.NoError(t, err)
	flushWith(t, other, "v", 30, 4) // version 5
	flushWith(t, other, "v", 40, 5) // version 6
	// a retry of the applied flush at checkpoint 10 returns the cached version
	stale, err := e.mgr.CommitFlush(ctx, &pkindexpb.CommitFlushRequest{
		CollectionId: 1, Vchannel: "v", PchannelTerm: 1, CheckpointTimeTick: 10,
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, stale.GetVersion())

	e.now = e.now.Add(3 * time.Hour)
	require.NoError(t, e.gc.RunOnce(ctx))
	assert.Len(t, e.bodies(t, "v"), 2)
	assert.True(t, e.has(e.pointer(t, "v").GetBodyKey()))
	for _, id := range []int64{3, 4, 5} {
		assert.True(t, e.has(sstKey("v", id)), "id %d is referenced by the current version", id)
	}
	assert.False(t, e.has(sstKey("v", 1)))
	assert.False(t, e.has(sstKey("v", 2)))
}

func TestOrphanBodyAfterGrace(t *testing.T) {
	e := newEnv(t, testConfig)
	ctx := context.Background()
	e.create(t, "v")
	e.upload(t, "v", 9, 10)
	// bodies of commits that never swapped the pointer
	oldOrphan := pkindex.ManifestBodyKey(pkindex.ManifestDir(testRoot, 1, "v"), 2, "old")
	e.writeBody(t, oldOrphan, &pkindexpb.Manifest{Version: 2, Ssts: refs(9)})
	e.now = e.now.Add(2 * time.Hour)
	youngOrphan := pkindex.ManifestBodyKey(pkindex.ManifestDir(testRoot, 1, "v"), 2, "young")
	e.writeBody(t, youngOrphan, &pkindexpb.Manifest{Version: 2, Ssts: refs(10)})
	e.now = e.now.Add(10 * time.Minute)

	require.NoError(t, e.gc.RunOnce(ctx))
	assert.False(t, e.has(oldOrphan))
	assert.False(t, e.has(sstKey("v", 9)))
	assert.True(t, e.has(youngOrphan))
	assert.True(t, e.has(sstKey("v", 10)), "referenced by a body within the grace window")
}

func TestCompactionCommittedBetweenReads(t *testing.T) {
	e := newEnv(t, testConfig)
	ctx := context.Background()
	e.create(t, "v")
	t0 := e.now
	e.upload(t, "v", 1, 2, 3)
	e.flush(t, "v", 10, 2, 1)
	e.now = e.now.Add(3 * time.Hour)

	// The compaction wrote its body before the grace window and swaps the
	// pointer after the GC read the in-flight outputs.
	e.inFlight = func(context.Context, int64, string) ([]int64, error) {
		e.store.Now = func() time.Time { return t0 }
		defer func() { e.store.Now = func() time.Time { return e.now } }()
		_, err := e.mgr.CommitCompaction(ctx, 1, "v", manifest.Compaction{Inputs: []int64{2, 1}, Outputs: refs(3)})
		require.NoError(t, err)
		return nil, nil
	}

	require.NoError(t, e.gc.RunOnce(ctx))
	assert.True(t, e.has(e.pointer(t, "v").GetBodyKey()))
	assert.True(t, e.has(sstKey("v", 3)))
}

func TestUnreadableCurrentBodySkipsVChannel(t *testing.T) {
	cases := map[string]func(e *env, key string){
		"missing": func(e *env, key string) {
			require.NoError(t, e.store.MultiRemove(context.Background(), []string{key}))
		},
		"corrupt": func(e *env, key string) {
			require.NoError(t, e.store.Write(context.Background(), key, []byte("not a manifest")))
		},
		"version differs from pointer": func(e *env, key string) {
			e.writeBody(t, key, &pkindexpb.Manifest{Version: 99})
		},
	}
	for name, damage := range cases {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, testConfig)
			ctx := context.Background()
			for _, vch := range []string{"bad", "good"} {
				e.create(t, vch)
				for i := int64(1); i <= 3; i++ {
					e.upload(t, vch, i)
					e.flush(t, vch, uint64(i*10), i)
				}
				e.upload(t, vch, 9)
			}
			damage(e, e.pointer(t, "bad").GetBodyKey())
			badBodies := len(e.bodies(t, "bad"))
			e.now = e.now.Add(3 * time.Hour)

			require.NoError(t, e.gc.RunOnce(ctx))
			assert.Len(t, e.bodies(t, "bad"), badBodies)
			for _, id := range []int64{1, 2, 3, 9} {
				assert.True(t, e.has(sstKey("bad", id)), "id %d", id)
			}
			assert.Len(t, e.bodies(t, "good"), 2)
			assert.False(t, e.has(sstKey("good", 9)))
		})
	}
}

func TestUndecodableRetainedBodySkipsSSTSweep(t *testing.T) {
	e := newEnv(t, testConfig)
	ctx := context.Background()
	e.create(t, "v")       // version 1
	e.flush(t, "v", 10, 1) // version 2
	e.flush(t, "v", 20, 2) // version 3
	e.upload(t, "v", 1, 2, 9)
	var v1, v2 string
	for _, k := range e.bodies(t, "v") {
		switch v, _ := pkindex.ParseManifestBodyKey(pkindex.ManifestDir(testRoot, 1, "v"), k); v {
		case 1:
			v1 = k
		case 2:
			v2 = k
		}
	}
	require.NoError(t, e.store.Write(ctx, v2, []byte("not a manifest")))
	e.now = e.now.Add(3 * time.Hour)

	require.NoError(t, e.gc.RunOnce(ctx))
	assert.False(t, e.has(v1), "outside the retention")
	assert.True(t, e.has(v2))
	assert.True(t, e.has(sstKey("v", 9)), "the live set is incomplete")
}

func TestForeignObjectsAreKept(t *testing.T) {
	e := newEnv(t, testConfig)
	ctx := context.Background()
	e.create(t, "v")
	foreign := []string{
		pkindex.SSTBasePath(testRoot, 1, "v") + "/x.tmp",
		pkindex.SSTBasePath(testRoot, 1, "v") + "/sub/7.sst",
		pkindex.ManifestDir(testRoot, 1, "v") + "/x.tmp",
		pkindex.ManifestDir(testRoot, 1, "v") + "/1.pb",
	}
	for _, k := range foreign {
		require.NoError(t, e.store.Write(ctx, k, []byte("x")))
	}
	e.now = e.now.Add(3 * time.Hour)

	require.NoError(t, e.gc.RunOnce(ctx))
	for _, k := range foreign {
		assert.True(t, e.has(k), k)
	}
}

func TestVChannelsAreIndependent(t *testing.T) {
	e := newEnv(t, testConfig)
	ctx := context.Background()
	// "v" is a string prefix of "v1"
	e.create(t, "v")
	e.create(t, "v1")
	e.upload(t, "v", 1, 5)
	e.upload(t, "v1", 1, 2, 6)
	e.flush(t, "v", 10, 1)
	e.flush(t, "v1", 10, 2, 1)
	e.now = e.now.Add(3 * time.Hour)

	require.NoError(t, e.gc.RunOnce(ctx))
	assert.True(t, e.has(sstKey("v", 1)))
	assert.False(t, e.has(sstKey("v", 5)))
	assert.True(t, e.has(sstKey("v1", 1)))
	assert.True(t, e.has(sstKey("v1", 2)))
	assert.False(t, e.has(sstKey("v1", 6)))
	assert.Len(t, e.bodies(t, "v"), 2)
	assert.Len(t, e.bodies(t, "v1"), 2)
}

func TestRunOnceListError(t *testing.T) {
	e := newEnv(t, testConfig)
	e.pointers.ListErr = errors.New("etcd down")
	err := e.gc.RunOnce(context.Background())
	assert.True(t, errors.Is(err, pkindex.ErrUnavailable))
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	for name, cfg := range map[string]Config{
		"interval":        {Interval: 0, Grace: time.Hour},
		"zero grace":      {Interval: time.Minute, Grace: 0},
		"negative grace":  {Interval: time.Minute, Grace: -1},
		"grace below min": {Interval: time.Minute, Grace: MinGrace - time.Second},
		"retain versions": {Interval: time.Minute, Grace: time.Hour, RetainVersions: -1},
		"retain duration": {Interval: time.Minute, Grace: time.Hour, RetainDuration: -1},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := New(manifest.NewFakeMetaStore(), manifest.NewFakeObjectStore(), testRoot, cfg, nil)
			assert.True(t, errors.Is(err, ErrInvalidConfig))
		})
	}
}

func TestNewAcceptsMinGrace(t *testing.T) {
	_, err := New(manifest.NewFakeMetaStore(), manifest.NewFakeObjectStore(), testRoot,
		Config{Interval: time.Minute, Grace: MinGrace}, nil)
	assert.NoError(t, err)
}

func TestStartRunsRounds(t *testing.T) {
	cfg := testConfig
	cfg.Interval = 10 * time.Millisecond
	e := newEnv(t, cfg)
	e.create(t, "v")
	e.upload(t, "v", 9)
	e.now = e.now.Add(3 * time.Hour)

	e.gc.Start(context.Background())
	assert.Eventually(t, func() bool { return !e.has(sstKey("v", 9)) }, 5*time.Second, 10*time.Millisecond)
	e.gc.Close()
}

func TestCloseBeforeStart(t *testing.T) {
	e := newEnv(t, testConfig)
	e.gc.Close()
}

func TestCurrentBodyIsAlwaysKept(t *testing.T) {
	cfg := testConfig
	cfg.RetainVersions = 0
	cfg.RetainDuration = 0
	e := newEnv(t, cfg)
	e.create(t, "v")
	e.upload(t, "v", 1)
	e.flush(t, "v", 10, 1)
	e.now = e.now.Add(3 * time.Hour)

	require.NoError(t, e.gc.RunOnce(context.Background()))
	assert.Equal(t, []string{e.pointer(t, "v").GetBodyKey()}, e.bodies(t, "v"))
	assert.True(t, e.has(sstKey("v", 1)))
}

func TestLayoutMismatchSkipsVChannel(t *testing.T) {
	// each case replaces the current version of "bad" with one whose body key
	// or sst_base_path is not the layout's
	cases := map[string]func(m *pkindexpb.Manifest) string{
		"body key outside the manifest dir": func(m *pkindexpb.Manifest) string {
			return pkindex.ManifestDir(testRoot, 1, "other") + "/" + "9-x.pb"
		},
		"sst base path differs": func(m *pkindexpb.Manifest) string {
			m.SstBasePath = pkindex.SSTBasePath(testRoot, 1, "other")
			return pkindex.ManifestBodyKey(pkindex.ManifestDir(testRoot, 1, "bad"), m.GetVersion(), "x")
		},
		"empty sst base path": func(m *pkindexpb.Manifest) string {
			m.SstBasePath = ""
			return pkindex.ManifestBodyKey(pkindex.ManifestDir(testRoot, 1, "bad"), m.GetVersion(), "x")
		},
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, testConfig)
			ctx := context.Background()
			for _, vch := range []string{"bad", "good", "other"} {
				e.create(t, vch)
				for i := int64(1); i <= 3; i++ {
					e.upload(t, vch, i)
					e.flush(t, vch, uint64(i*10), i)
				}
				e.upload(t, vch, 9)
			}
			p := e.pointer(t, "bad")
			b, err := e.store.Read(ctx, p.GetBodyKey())
			require.NoError(t, err)
			m := &pkindexpb.Manifest{}
			require.NoError(t, proto.Unmarshal(b, m))
			key := change(m)
			e.writeBody(t, key, m)
			ptr, err := proto.Marshal(&pkindexpb.ManifestPointer{Version: m.GetVersion(), BodyKey: key})
			require.NoError(t, err)
			e.pointers.Put(manifest.PointerKey(1, "bad"), string(ptr))
			badBodies, otherBodies := len(e.bodies(t, "bad")), len(e.bodies(t, "other"))
			e.now = e.now.Add(3 * time.Hour)

			// collect "bad" alone, so that "other" stays as it was
			require.NoError(t, e.gc.runVChannel(ctx, 1, "bad"))
			assert.Len(t, e.bodies(t, "bad"), badBodies)
			assert.Len(t, e.bodies(t, "other"), otherBodies)
			for _, vch := range []string{"bad", "other"} {
				for _, id := range []int64{1, 2, 3, 9} {
					assert.True(t, e.has(sstKey(vch, id)), "%s id %d", vch, id)
				}
			}

			require.NoError(t, e.gc.RunOnce(ctx))
			assert.Len(t, e.bodies(t, "good"), 2)
			assert.False(t, e.has(sstKey("good", 9)))
		})
	}
}

func TestInFlightErrorIsUnavailable(t *testing.T) {
	e := newEnv(t, testConfig)
	e.create(t, "v")
	e.upload(t, "v", 9)
	e.now = e.now.Add(3 * time.Hour)
	e.inFlight = func(context.Context, int64, string) ([]int64, error) {
		return nil, errors.New("task meta unavailable")
	}

	err := e.gc.runVChannel(context.Background(), 1, "v")
	assert.True(t, errors.Is(err, pkindex.ErrUnavailable))
	assert.True(t, e.has(sstKey("v", 9)))
}

func TestRunOnceStopsWhenContextDone(t *testing.T) {
	e := newEnv(t, testConfig)
	e.create(t, "v")
	e.create(t, "w")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	e.inFlight = func(context.Context, int64, string) ([]int64, error) {
		calls++
		cancel()
		return nil, nil
	}

	require.NoError(t, e.gc.RunOnce(ctx))
	assert.Equal(t, 1, calls)
}

func TestRemoveFailureRecoversNextRound(t *testing.T) {
	e := newEnv(t, testConfig)
	ctx := context.Background()
	e.create(t, "v")
	e.upload(t, "v", 9)
	e.now = e.now.Add(3 * time.Hour)

	e.store.RemoveErr = errors.New("s3 throttled")
	require.NoError(t, e.gc.RunOnce(ctx))
	assert.True(t, e.has(sstKey("v", 9)))

	require.NoError(t, e.gc.RunOnce(ctx))
	assert.False(t, e.has(sstKey("v", 9)))
}

func TestRetainedBodyReadFailureSkipsSSTSweep(t *testing.T) {
	e := newEnv(t, testConfig)
	ctx := context.Background()
	e.create(t, "v")       // version 1
	e.flush(t, "v", 10, 1) // version 2
	e.flush(t, "v", 20, 2) // version 3
	e.upload(t, "v", 1, 2, 9)
	var v1, v2 string
	for _, k := range e.bodies(t, "v") {
		switch v, _ := pkindex.ParseManifestBodyKey(pkindex.ManifestDir(testRoot, 1, "v"), k); v {
		case 1:
			v1 = k
		case 2:
			v2 = k
		}
	}
	e.now = e.now.Add(3 * time.Hour)

	e.store.ReadErr, e.store.ReadErrKey = errors.New("s3 throttled"), v2
	require.NoError(t, e.gc.RunOnce(ctx))
	assert.False(t, e.has(v1), "outside the retention")
	assert.True(t, e.has(sstKey("v", 9)), "the live set is incomplete")

	require.NoError(t, e.gc.RunOnce(ctx))
	assert.False(t, e.has(sstKey("v", 9)))
}
