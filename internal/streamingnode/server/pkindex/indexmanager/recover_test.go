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

package indexmanager

import (
	"context"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/pkindex/manifest"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

// batchCommitter records the ref count of each GetRecoveryManifests call and
// passes it on.
type batchCommitter struct {
	manifest.Committer
	mu      sync.Mutex
	batches []int
}

func (c *batchCommitter) GetRecoveryManifests(ctx context.Context, refs []manifest.VChannelRef) ([]manifest.RecoveryManifest, error) {
	c.mu.Lock()
	c.batches = append(c.batches, len(refs))
	c.mu.Unlock()
	return c.Committer.GetRecoveryManifests(ctx, refs)
}

func TestRecoverIndexesLooksManifestsUpInBatches(t *testing.T) {
	ctx := context.Background()
	env := NewFakeEnv(t, t.TempDir())
	committer := &batchCommitter{Committer: env.Committer}
	env.Manager.SetCommitterForTest(committer)
	env.Committer.SetManifest("v5", &manifest.FakeManifest{View: manifest.View{Version: 1, CheckpointTimeTick: 10, SSTBasePath: "root/5/v5"}})
	old := manifestLookupBatch
	manifestLookupBatch = 2
	t.Cleanup(func() { manifestLookupBatch = old })

	specs := []IndexSpec{
		{VChannel: "v1", CollectionID: 1, IndexID: 1},
		{VChannel: "v2", CollectionID: 2, IndexID: 1},
		{VChannel: "v3", CollectionID: 3, IndexID: 1},
		{VChannel: "v4", CollectionID: 4, IndexID: 1},
		{VChannel: "v5", CollectionID: 5, IndexID: 1},
	}
	m := env.Manager.New(PChannelConfig{PChannel: types.PChannelInfo{Name: "p", Term: 3}})
	defer m.Close()
	indexes, err := m.RecoverIndexes(ctx, specs)
	require.NoError(t, err)
	require.Len(t, indexes, 5)

	committer.mu.Lock()
	batches := append([]int(nil), committer.batches...)
	committer.mu.Unlock()
	sort.Sort(sort.Reverse(sort.IntSlice(batches)))
	assert.Equal(t, []int{2, 2, 1}, batches)

	var v5, v1 *index.Index
	for _, idx := range indexes {
		switch idx.VChannel() {
		case "v5":
			v5 = idx
		case "v1":
			v1 = idx
		}
	}
	assert.Equal(t, uint64(10), v5.LastCommittedTimeTick())
	assert.False(t, v5.Ready(), "v5 waits for the install")
	assert.True(t, v1.Ready(), "v1 has no manifest and is ready at once")
}
