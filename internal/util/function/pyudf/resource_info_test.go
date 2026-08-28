// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pyudf

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestResourceInfoSnapshotAndVersion(t *testing.T) {
	info := newResourceInfo()
	assert.False(t, info.Snapshot().ready)

	wheel := testWheelResource(2, "rank_udf")
	upperWheel := testWheelResource(3, "upper_udf")
	upperWheel.Path = "/remote/upper.WHL"
	notWheel := testWheelResource(4, "archive")
	notWheel.Path = "/remote/archive.zip"
	require.NoError(t, info.OnFileResourceSync(fileresource.SyncEvent{
		Version:   2,
		Resources: []*fileresource.ResolvedFileResource{nil, wheel, upperWheel, notWheel},
	}))

	snapshot := info.Snapshot()
	assert.True(t, snapshot.ready)
	assert.Equal(t, uint64(2), snapshot.version)
	require.Len(t, snapshot.resources, 2)
	assert.Equal(t, *wheel, snapshot.resources[wheel.Name])
	assert.Equal(t, *upperWheel, snapshot.resources[upperWheel.Name])

	oldWheel := testWheelResource(1, "rank_udf")
	require.NoError(t, info.OnFileResourceSync(fileresource.SyncEvent{
		Version:   1,
		Resources: []*fileresource.ResolvedFileResource{oldWheel},
	}))
	assert.Equal(t, *wheel, info.Snapshot().resources[wheel.Name])

	require.NoError(t, info.OnFileResourceSync(fileresource.SyncEvent{Version: 3}))
	assert.Empty(t, info.Snapshot().resources)
	assert.Equal(t, *wheel, snapshot.resources[wheel.Name])
}

func TestResourceInfoCurrentIdentity(t *testing.T) {
	info := newResourceInfo()
	wheel := testWheelResource(1, "rank_udf")
	require.NoError(t, info.OnFileResourceSync(fileresource.SyncEvent{
		Version:   1,
		Resources: []*fileresource.ResolvedFileResource{wheel},
	}))

	assert.True(t, info.IsCurrent(*wheel))
	replaced := *wheel
	replaced.ID++
	assert.False(t, info.IsCurrent(replaced))
	assert.False(t, (*resourceInfo)(nil).IsCurrent(*wheel))
	assert.Nil(t, (*resourceInfo)(nil).Snapshot())
}

func TestResourceInfoResolve(t *testing.T) {
	info := newResourceInfo()
	_, _, err := info.Resolve("rank_udf")
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable)

	wheel := testWheelResource(1, "rank_udf")
	require.NoError(t, info.OnFileResourceSync(fileresource.SyncEvent{
		Version:   7,
		Resources: []*fileresource.ResolvedFileResource{wheel},
	}))
	resolved, version, err := info.Resolve(wheel.Name)
	require.NoError(t, err)
	assert.Equal(t, *wheel, resolved)
	assert.Equal(t, uint64(7), version)

	_, version, err = info.Resolve("missing")
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.Equal(t, uint64(7), version)
}

func TestResourceInfoSubscribeReplayAndUnsubscribe(t *testing.T) {
	info := newResourceInfo()
	wheel := testWheelResource(1, "rank_udf")
	require.NoError(t, info.OnFileResourceSync(fileresource.SyncEvent{
		Version:   1,
		Resources: []*fileresource.ResolvedFileResource{wheel},
	}))

	versions := make([]uint64, 0, 2)
	unsubscribe := info.Subscribe(func(snapshot *resourceSnapshot) {
		versions = append(versions, snapshot.version)
	})
	assert.Equal(t, []uint64{1}, versions)

	require.NoError(t, info.OnFileResourceSync(fileresource.SyncEvent{Version: 2}))
	assert.Equal(t, []uint64{1, 2}, versions)

	unsubscribe()
	unsubscribe()
	require.NoError(t, info.OnFileResourceSync(fileresource.SyncEvent{Version: 3}))
	assert.Equal(t, []uint64{1, 2}, versions)
}
