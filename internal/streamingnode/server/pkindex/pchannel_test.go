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

package pkindex

import (
	"context"
	"math"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestIsFatal(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		fatal bool
	}{
		{"fenced", errors.Wrap(index.ErrFenced, "v0"), true},
		{"rejected", errors.Wrap(index.ErrRejected, "v0"), true},
		{"corrupted", errors.Mark(errors.New("bad block"), pkroot.ErrCorrupted), true},
		{"unavailable", errors.Mark(errors.New("connection reset"), pkroot.ErrUnavailable), false},
		{"io", errors.Mark(errors.New("disk full"), pkroot.ErrIO), false},
		// The Committer translates the codes of the manifest service. A code
		// that it did not translate is retryable.
		{"untranslated service internal", merr.WrapErrServiceInternal("bug"), false},
		{"deadline", context.DeadlineExceeded, false},
		{"canceled", context.Canceled, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.fatal, index.IsFatal(c.err))
		})
	}
}

func TestClosedChannelAndIndex(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	ch.Close()

	_, _, err := ch.CreateCollection(context.Background(), "v1", testCollectionID, schemapb.DataType_Int64)
	assert.True(t, errors.Is(err, engine.ErrClosed))
	_, err = idx.Authority().Decide(context.Background(), insertRequest(idx, 1))
	assert.True(t, errors.Is(err, engine.ErrClosed))
	assert.True(t, errors.Is(err, pkroot.ErrUnavailable))
	assert.Nil(t, ch.get("v0"))
}

func TestManagerCloseClosesChannels(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	env.add(ch, "v0")
	env.mgr.Close()
	assert.Nil(t, ch.get("v0"))
	closed := env.channel(0)
	_, _, err := closed.CreateCollection(context.Background(), "v0", testCollectionID, schemapb.DataType_Int64)
	assert.True(t, errors.Is(err, engine.ErrClosed))
}

func TestAddOfAnExistingIndexReportsNotCreated(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	insert(t, ch, idx, 10, 1)

	_, created, err := ch.CreateCollection(context.Background(), "v0", testCollectionID, schemapb.DataType_Int64)
	require.NoError(t, err)
	assert.False(t, created, "a caller that rolls back must not remove the Index in use")
	assert.Same(t, idx, ch.get("v0"))
	assert.EqualValues(t, 10, segmentOf(t, idx, 1))
}

func TestDropOfAClosedChannelFails(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	env.add(ch, "v0")
	ch.Close()

	err := ch.DropCollection(context.Background(), "v0")
	require.ErrorIs(t, err, engine.ErrClosed)
	assert.DirExists(t, filepath.Join(env.root, "v0"))
}

// TestLastAckedAfterClose: a closed PChannelIndexManager keeps the last value
// it returned. Only one that never had an Index reports +inf.
func TestLastAckedAfterClose(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	empty := env.openChannel(0)
	empty.Start()
	empty.ObserveTimeTick(10)
	empty.Close()
	assert.EqualValues(t, uint64(math.MaxUint64), empty.LastAcked(), "never had an Index")

	// Close forgot the first PChannelIndexManager of the pchannel; a new one takes its place
	ch := env.channel(0)
	idx := env.add(ch, "v0")
	ch.ObserveTimeTick(10)
	require.EqualValues(t, 10, ch.LastAcked())
	insert(t, ch, idx, 1, 1)
	ch.ObserveTimeTick(20)
	require.EqualValues(t, 10, ch.LastAcked(), "the dirty Index holds it")
	ch.Close()
	assert.EqualValues(t, 10, ch.LastAcked(), "the uncommitted writes of the closed Index are not committed by the close")
	ch.ObserveTimeTick(30)
	assert.EqualValues(t, 10, ch.LastAcked())
}

// TestCloseKeepsLastAckedWithoutAnEarlierCall: Close records the LastAcked of
// its moment, so a first call after Close does not see an empty heap.
func TestCloseKeepsLastAckedWithoutAnEarlierCall(t *testing.T) {
	env := newTestEnv(t, syncScheduler{})
	ch := env.channel(0)
	env.add(ch, "v0")
	ch.ObserveTimeTick(100)
	ch.Close()
	assert.EqualValues(t, 100, ch.LastAcked(), "the clean Index followed lastObserved at the close")
}
