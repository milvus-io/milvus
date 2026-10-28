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

	"github.com/milvus-io/milvus/internal/datacoord/pkindex/manifest"
	"github.com/milvus-io/milvus/internal/pkindex"
)

const (
	dropTolerance    = time.Hour
	missingTolerance = 2 * time.Hour
	testVChannel     = "p_1v0"
)

// indexEnv is an env whose GC also recovers dropped indexes, with datacoord
// facts that the test sets.
type indexEnv struct {
	*env
	presence      IndexPresence
	collection    bool
	collectionErr error
	paused        bool
}

func newIndexEnv(t *testing.T) *indexEnv {
	cfg := testConfig
	cfg.DropTolerance, cfg.MissingTolerance = dropTolerance, missingTolerance
	e := &indexEnv{env: newEnv(t, cfg), presence: IndexLive, collection: true}
	e.gc.EnableIndexRecovery(IndexDeps{
		IndexPresence: func(collectionID, indexID int64) IndexPresence { return e.presence },
		HasCollection: func(ctx context.Context, collectionID int64) (bool, error) {
			return e.collection, e.collectionErr
		},
		CollectionPaused: func(collectionID int64) bool { return e.paused },
	}, e.mgr)
	// collection 1, index 2: a pointer, a manifest body and one SST
	e.create(t, testVChannel)
	e.upload(t, testVChannel, 9)
	e.flush(t, testVChannel, 2, 9)
	return e
}

// objects returns the number of objects of collection 1, index 2.
func (e *indexEnv) objects(t *testing.T) int {
	n := 0
	err := e.store.Walk(context.Background(), pkindex.IndexDir(testRoot, 1, 2)+"/", func(string, time.Time) bool {
		n++
		return true
	})
	require.NoError(t, err)
	return n
}

func (e *indexEnv) hasPointer(t *testing.T) bool {
	_, found, err := e.pointers.Load(context.Background(), manifest.PointerKey(1, 2, testVChannel))
	require.NoError(t, err)
	return found
}

func (e *indexEnv) requireKept(t *testing.T) {
	t.Helper()
	assert.True(t, e.hasPointer(t))
	assert.NotZero(t, e.objects(t))
}

func (e *indexEnv) requireRemoved(t *testing.T) {
	t.Helper()
	assert.False(t, e.hasPointer(t))
	assert.Zero(t, e.objects(t))
}

func TestIndexGCKeepsALiveIndex(t *testing.T) {
	e := newIndexEnv(t)
	e.now = e.now.Add(100 * time.Hour)
	require.NoError(t, e.gc.RunOnce(context.Background()))
	e.requireKept(t)
}

func TestIndexGCOfAMissingIndex(t *testing.T) {
	e := newIndexEnv(t)
	e.presence = IndexMissing
	e.now = e.now.Add(missingTolerance)
	require.NoError(t, e.gc.RunOnce(context.Background()))
	e.requireKept(t)

	e.now = e.now.Add(time.Second)
	require.NoError(t, e.gc.RunOnce(context.Background()))
	e.requireRemoved(t)
	// a second round finds nothing and changes nothing
	require.NoError(t, e.gc.RunOnce(context.Background()))
	e.requireRemoved(t)
}

func TestIndexGCOfADeletedIndex(t *testing.T) {
	e := newIndexEnv(t)
	e.presence = IndexDeleted
	e.now = e.now.Add(dropTolerance)
	require.NoError(t, e.gc.RunOnce(context.Background()))
	e.requireKept(t)

	e.now = e.now.Add(time.Second)
	require.NoError(t, e.gc.RunOnce(context.Background()))
	e.requireRemoved(t)
}

func TestIndexGCOfADroppedCollection(t *testing.T) {
	e := newIndexEnv(t)
	e.collection = false // the presence of the index does not matter then
	e.now = e.now.Add(dropTolerance + time.Second)
	require.NoError(t, e.gc.RunOnce(context.Background()))
	e.requireRemoved(t)
}

func TestIndexGCSkipsTheRoundOnACollectionCheckError(t *testing.T) {
	e := newIndexEnv(t)
	e.presence = IndexMissing
	e.collectionErr = errors.New("rootcoord is down")
	e.now = e.now.Add(100 * time.Hour)
	assert.Error(t, e.gc.RunOnce(context.Background()))
	e.requireKept(t)
}

func TestIndexGCSkipsAPausedCollection(t *testing.T) {
	e := newIndexEnv(t)
	e.presence = IndexMissing
	e.paused = true
	e.now = e.now.Add(100 * time.Hour)
	require.NoError(t, e.gc.RunOnce(context.Background()))
	e.requireKept(t)
}

// A crash after the pointers were removed leaves objects without a pointer.
// The next round removes them.
func TestIndexGCRemovesTheObjectsLeftByAFailedRound(t *testing.T) {
	e := newIndexEnv(t)
	e.presence = IndexMissing
	e.now = e.now.Add(100 * time.Hour)
	e.store.RemoveErr = errors.New("s3 throttled")
	require.NoError(t, e.gc.RunOnce(context.Background()))
	assert.False(t, e.hasPointer(t))
	assert.NotZero(t, e.objects(t))

	require.NoError(t, e.gc.RunOnce(context.Background()))
	e.requireRemoved(t)
}

// A flush that arrives after the drop writes a pointer again under the old
// index ID. A later round removes it once its objects are old enough.
func TestIndexGCRemovesTheObjectsOfALateFlush(t *testing.T) {
	e := newIndexEnv(t)
	e.presence = IndexMissing
	e.now = e.now.Add(100 * time.Hour)
	require.NoError(t, e.gc.RunOnce(context.Background()))
	e.requireRemoved(t)

	e.upload(t, testVChannel, 10)
	e.flush(t, testVChannel, 3, 10)
	require.NoError(t, e.gc.RunOnce(context.Background()))
	e.requireKept(t)

	e.now = e.now.Add(missingTolerance + time.Second)
	require.NoError(t, e.gc.RunOnce(context.Background()))
	e.requireRemoved(t)
}

// Objects of an index with no pointer at all, for example a vchannel whose
// first manifest commit never succeeded, are found by the walk.
func TestIndexGCRemovesObjectsWithoutAPointer(t *testing.T) {
	e := newIndexEnv(t)
	other := pkindex.SSTKey(pkindex.SSTBasePath(testRoot, 1, 5, testVChannel), 1)
	require.NoError(t, e.store.Write(context.Background(), other, []byte("x")))
	e.presence = IndexMissing
	e.now = e.now.Add(100 * time.Hour)
	require.NoError(t, e.gc.RunOnce(context.Background()))
	assert.False(t, e.has(other))
}

func TestIndexGCIgnoresForeignKeys(t *testing.T) {
	e := newIndexEnv(t)
	foreign := []string{testRoot + "/pk_index/x/2/v/sst/1.sst", testRoot + "/pk_index/1/2"}
	for _, key := range foreign {
		require.NoError(t, e.store.Write(context.Background(), key, []byte("x")))
	}
	e.presence = IndexMissing
	e.now = e.now.Add(100 * time.Hour)
	require.NoError(t, e.gc.RunOnce(context.Background()))
	for _, key := range foreign {
		assert.True(t, e.has(key), key)
	}
}

func TestNewRejectsNegativeTolerances(t *testing.T) {
	for name, cfg := range map[string]Config{
		"drop":    {Interval: time.Minute, Grace: time.Hour, DropTolerance: -1},
		"missing": {Interval: time.Minute, Grace: time.Hour, MissingTolerance: -1},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := New(manifest.NewFakeMetaStore(), manifest.NewFakeObjectStore(), testRoot, cfg, nil)
			assert.True(t, errors.Is(err, ErrInvalidConfig))
		})
	}
}
