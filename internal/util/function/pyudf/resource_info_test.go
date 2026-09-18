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
	"path/filepath"
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

func testWheelResource(id int64, name string) *fileresource.ResolvedFileResource {
	return &fileresource.ResolvedFileResource{ID: id, Name: name, Path: "/remote/" + name + ".whl", LocalPath: "local/" + name + ".whl"}
}

func TestResolveResourcePath(t *testing.T) {
	old := globalResourceInfo
	globalResourceInfo = newResourceInfo()
	defer func() { globalResourceInfo = old }()
	_, err := ResolveResourcePath("rank")
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	resource := testWheelResource(1, "rank")
	require.NoError(t, globalResourceInfo.OnFileResourceSync(fileresource.SyncEvent{Version: 1, Resources: []*fileresource.ResolvedFileResource{resource}}))
	local, err := ResolveResourcePath("rank")
	require.NoError(t, err)
	expected, err := filepath.Abs(resource.LocalPath)
	require.NoError(t, err)
	require.Equal(t, expected, local)
	_, err = ResolveResourcePath("missing")
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	resource.LocalPath = ""
	require.NoError(t, globalResourceInfo.OnFileResourceSync(fileresource.SyncEvent{Version: 2, Resources: []*fileresource.ResolvedFileResource{resource}}))
	_, err = ResolveResourcePath("rank")
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}
