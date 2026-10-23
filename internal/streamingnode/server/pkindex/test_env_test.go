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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
)

func TestFakeCommitterServesManifests(t *testing.T) {
	c := NewFakeCommitter()
	c.SetManifest("v1", &index.FakeManifest{View: index.ManifestView{Version: 3, CheckpointTimeTick: 100, SSTBasePath: "root/1/v1"}})
	answers, err := c.GetRecoveryManifests(context.Background(), []VChannelRef{{CollectionID: 1, VChannel: "v1"}, {CollectionID: 2, VChannel: "v2"}})
	require.NoError(t, err)
	require.True(t, answers[0].Found)
	require.False(t, answers[1].Found)
	view, err := c.ReadManifest(context.Background(), answers[0].BodyKey)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), view.CheckpointTimeTick)
	_, err = c.ReadManifest(context.Background(), "manifest/none/1.pb")
	assert.ErrorIs(t, err, pkroot.ErrCorrupted)

	c.SetManifest("v1", nil)
	answers, err = c.GetRecoveryManifests(context.Background(), []VChannelRef{{CollectionID: 1, VChannel: "v1"}})
	require.NoError(t, err)
	assert.False(t, answers[0].Found)
	_, err = c.ReadManifest(context.Background(), "manifest/v1/3.pb")
	assert.ErrorIs(t, err, pkroot.ErrCorrupted)
}
