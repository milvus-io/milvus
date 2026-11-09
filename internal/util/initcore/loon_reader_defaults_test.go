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

package initcore

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// newIsolatedParams returns a param table of its own, so saving loon reader
// keys in a resolve test cannot fire the hot-reload watchers other tests
// registered on the global table: those would apply the value and could
// create the process-wide reader pool, which cannot be destroyed.
func newIsolatedParams() *paramtable.ComponentParam {
	params := &paramtable.ComponentParam{}
	params.Init(paramtable.NewBaseTable(paramtable.SkipRemote(true), paramtable.SkipEnv(true)))
	return params
}

// withRole runs the test under the given process role and restores the
// previous one afterwards.
func withRole(t *testing.T, role string) {
	previous := paramtable.GetRole()
	paramtable.SetRole(role)
	t.Cleanup(func() { paramtable.SetRole(previous) })
}

func TestResolveLoonReaderThreadPoolSize(t *testing.T) {
	paramtable.Init()
	pt := newIsolatedParams()
	key := pt.CommonCfg.StorageReaderThreadPoolSize.Key

	cpuThreads := min(int64(max(1, hardware.GetCPUNum())), maxStorageReaderThreadPoolSize)
	for _, tc := range []struct {
		name, role, value string
		want              int64
		wantErr           bool
	}{
		{name: "default stays off", role: typeutil.DataNodeRole, value: "", want: 0},
		{name: "explicit off", role: typeutil.DataNodeRole, value: "0", want: 0},
		{name: "explicit size", role: typeutil.DataNodeRole, value: "16", want: 16},
		{name: "explicit size ignores role", role: typeutil.StandaloneRole, value: "16", want: 16},
		{name: "explicit max", role: typeutil.DataNodeRole, value: "1024", want: 1024},
		{name: "auto on datanode", role: typeutil.DataNodeRole, value: "-1", want: cpuThreads},
		{name: "auto on standalone", role: typeutil.StandaloneRole, value: "-1", want: 0},
		{name: "auto on querynode", role: typeutil.QueryNodeRole, value: "-1", want: 0},
		{name: "auto without role", role: "", value: "-1", want: 0},
		{name: "other negative", role: typeutil.DataNodeRole, value: "-2", wantErr: true},
		{name: "above max", role: typeutil.DataNodeRole, value: "1025", wantErr: true},
		{name: "would wrap int32", role: typeutil.DataNodeRole, value: "4294967296", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withRole(t, tc.role)
			if tc.value == "" {
				require.NoError(t, pt.Reset(key))
			} else {
				require.NoError(t, pt.Save(key, tc.value))
			}
			got, err := ResolveLoonReaderThreadPoolSize(pt)
			if tc.wantErr {
				assert.ErrorIs(t, err, merr.ErrParameterInvalid)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestResolveIndexBuildReadWindowBytes(t *testing.T) {
	paramtable.Init()
	pt := newIsolatedParams()
	key := pt.CommonCfg.IndexBuildReadWindowBytes.Key
	rangeKey := pt.CommonCfg.ArrowReaderRangeSizeLimitBytes.Key
	// the arrow IO pool size is resolved from the global table, like the
	// storage runtime itself
	global := paramtable.Get()
	ioKey := global.CommonCfg.ArrowIOThreadPoolCoefficient.Key
	defer global.Reset(ioKey)

	ioThreads, err := ResolveArrowIOThreadPoolCapacity()
	require.NoError(t, err)
	autoDefaultRange := min(int64(ioThreads)*2*arrowDefaultRangeSizeLimitBytes, int64(maxIndexBuildReadWindowBytes))
	autoConfiguredRange := min(int64(ioThreads)*2*(8<<20), int64(maxIndexBuildReadWindowBytes))

	for _, tc := range []struct {
		name, role, value, rangeSize string
		want                         int64
		wantErr                      bool
	}{
		{name: "default is auto on datanode", role: typeutil.DataNodeRole, value: "", want: autoDefaultRange},
		{name: "default is off on standalone", role: typeutil.StandaloneRole, value: "", want: 0},
		{name: "auto on querynode", role: typeutil.QueryNodeRole, value: "-1", want: 0},
		{name: "auto follows the configured range size", role: typeutil.DataNodeRole, value: "-1", rangeSize: "8388608", want: autoConfiguredRange},
		{name: "auto is capped at 4GB", role: typeutil.DataNodeRole, value: "-1", rangeSize: "9223372036854775807", want: maxIndexBuildReadWindowBytes},
		{name: "explicit loon default", role: typeutil.DataNodeRole, value: "0", want: 0},
		{name: "explicit size", role: typeutil.DataNodeRole, value: "536870912", want: 536870912},
		{name: "explicit size ignores role", role: typeutil.StandaloneRole, value: "536870912", want: 536870912},
		{name: "explicit max", role: typeutil.DataNodeRole, value: "4294967296", want: maxIndexBuildReadWindowBytes},
		{name: "other negative", role: typeutil.DataNodeRole, value: "-2", wantErr: true},
		{name: "above max", role: typeutil.DataNodeRole, value: "8589934592", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withRole(t, tc.role)
			if tc.value == "" {
				require.NoError(t, pt.Reset(key))
			} else {
				require.NoError(t, pt.Save(key, tc.value))
			}
			if tc.rangeSize == "" {
				require.NoError(t, pt.Reset(rangeKey))
			} else {
				require.NoError(t, pt.Save(rangeKey, tc.rangeSize))
			}
			got, err := ResolveIndexBuildReadWindowBytes(pt)
			if tc.wantErr {
				assert.ErrorIs(t, err, merr.ErrParameterInvalid)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}

	t.Run("auto surfaces an invalid IO pool config", func(t *testing.T) {
		withRole(t, typeutil.DataNodeRole)
		require.NoError(t, pt.Reset(key))
		require.NoError(t, pt.Reset(rangeKey))
		require.NoError(t, global.Save(ioKey, "-3"))
		defer global.Reset(ioKey)
		_, err := ResolveIndexBuildReadWindowBytes(pt)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})
}

func TestAutoIndexBuildReadWindowBytes(t *testing.T) {
	const mb = int64(1 << 20)
	assert.Equal(t, 768*mb, autoIndexBuildReadWindowBytes(12, 32*mb))
	assert.Equal(t, 2*32*mb, autoIndexBuildReadWindowBytes(1, 32*mb))
	// a non-positive thread count still sizes for one thread
	assert.Equal(t, 2*32*mb, autoIndexBuildReadWindowBytes(0, 32*mb))
	assert.Equal(t, int64(maxIndexBuildReadWindowBytes), autoIndexBuildReadWindowBytes(64, 64*mb))
	assert.Equal(t, int64(maxIndexBuildReadWindowBytes), autoIndexBuildReadWindowBytes(math.MaxInt32, math.MaxInt64))
}

func TestLogLoonReaderConfig(t *testing.T) {
	paramtable.Init()
	pt := newIsolatedParams()

	withRole(t, typeutil.DataNodeRole)
	assert.NotPanics(t, func() { LogLoonReaderConfig(pt, "test") })

	// an unresolvable setting is reported instead of logged as in effect
	require.NoError(t, pt.Save(pt.CommonCfg.IndexBuildReadWindowBytes.Key, "-2"))
	assert.NotPanics(t, func() { LogLoonReaderConfig(pt, "test") })
}

func TestInitLoonReaderConfigAutoWindowOnDataNode(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	defer pt.Reset(pt.CommonCfg.IndexBuildReadWindowBytes.Key)
	defer pt.Reset(pt.CommonCfg.ArrowReaderRangeSizeLimitBytes.Key)

	// Only the window is automatic by default: applying it on a DataNode must
	// succeed and still leave the (non-destroyable) reader pool uncreated.
	withRole(t, typeutil.DataNodeRole)
	require.NoError(t, pt.Reset(pt.CommonCfg.IndexBuildReadWindowBytes.Key))
	require.NoError(t, InitLoonReaderConfig(pt))
	assert.LessOrEqual(t, EffectiveLoonReaderThreadPoolSize(), int32(1))

	// The watcher re-resolves the automatic window when the arrow read range
	// size changes, since the window is sized from it.
	RegisterLoonReaderConfigWatchers(pt, "test")
	assert.NotPanics(t, func() {
		require.NoError(t, pt.Save(pt.CommonCfg.ArrowReaderRangeSizeLimitBytes.Key, "16777216"))
	})
	window, err := ResolveIndexBuildReadWindowBytes(pt)
	require.NoError(t, err)
	ioThreads, err := ResolveArrowIOThreadPoolCapacity()
	require.NoError(t, err)
	assert.Equal(t, min(int64(ioThreads)*2*(16<<20), int64(maxIndexBuildReadWindowBytes)), window)
	assert.LessOrEqual(t, EffectiveLoonReaderThreadPoolSize(), int32(1))
}
