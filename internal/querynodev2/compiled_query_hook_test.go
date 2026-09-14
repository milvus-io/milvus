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

package querynodev2

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/searchutil/optimizers"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// recordingQueryHook is a compiled-in query hook that remembers how it was
// initialized and can refuse either step.
type recordingQueryHook struct {
	initErr, tuningErr error

	initWith   []string
	tuningWith []map[string]string
}

func (h *recordingQueryHook) Run(map[string]any) error { return nil }
func (h *recordingQueryHook) Init(s string) error {
	h.initWith = append(h.initWith, s)
	return h.initErr
}

func (h *recordingQueryHook) InitTuningConfig(m map[string]string) error {
	h.tuningWith = append(h.tuningWith, m)
	return h.tuningErr
}
func (h *recordingQueryHook) DeleteTuningConfig(string) error                 { return nil }
func (h *recordingQueryHook) CalculateEffectiveSegmentNum([]int64, int64) int { return 0 }

func installQueryHook(t *testing.T, h extension.QueryHook) *QueryNode {
	t.Helper()
	paramtable.Init()
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	extension.SetQueryHook(h)
	return &QueryNode{ctx: context.Background()}
}

// saveQueryNodeKey writes one configuration key and resets it when the test
// ends. Save and Reset both dispatch to the config watchers synchronously, on
// this goroutine, so a watcher initHook registered sees the reset only after
// the test's assertions have run.
func saveQueryNodeKey(t *testing.T, key, value string) {
	t.Helper()
	p := paramtable.Get()
	require.NoError(t, p.Save(key, value))
	t.Cleanup(func() { p.Reset(key) })
}

func TestInitHookUsesTheCompiledInQueryHook(t *testing.T) {
	h := &recordingQueryHook{}
	node := installQueryHook(t, h)
	saveQueryNodeKey(t, paramtable.Get().AutoIndexConfig.AutoIndexSearchConfig.Key, `{"level": 2}`)

	require.NoError(t, node.initHook())

	assert.Same(t, h, node.queryHook)
	assert.Equal(t, []string{`{"level": 2}`}, h.initWith,
		"the compiled-in hook is initialized with autoIndex.params.search, as a plug-in is")
	require.Len(t, h.tuningWith, 1, "and its tuning configuration, as a plug-in is")
	assert.Equal(t, paramtable.Get().AutoIndexConfig.AutoIndexTuningConfig.GetValue(), h.tuningWith[0])
}

// Two tuners for the same search is a deployment mistake, and it is reported
// rather than silently resolved by start-up order.
func TestInitHookRefusesACompiledInQueryHookBesideAPlugin(t *testing.T) {
	node := installQueryHook(t, &recordingQueryHook{})
	saveQueryNodeKey(t, paramtable.Get().QueryNodeCfg.SoPath.Key, "/tmp/some-tuner.so")

	err := node.initHook()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only one can")
	assert.Nil(t, node.queryHook)
}

// With nothing compiled in - a stock binary - nothing changes: an empty
// queryNode.soPath is still the error it always was.
func TestInitHookWithoutACompiledInQueryHookIsUnchanged(t *testing.T) {
	node := installQueryHook(t, nil)

	err := node.initHook()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fail to set the plugin path")
	assert.Nil(t, node.queryHook)
}

// A queryNode.soPath that does not load is still the load error it always
// was: the plug-in branch is master's, untouched.
func TestInitHookWithoutACompiledInQueryHookStillLoadsThePlugin(t *testing.T) {
	node := installQueryHook(t, nil)
	saveQueryNodeKey(t, paramtable.Get().QueryNodeCfg.SoPath.Key, "/nonexistent/some-tuner.so")

	err := node.initHook()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "/nonexistent/some-tuner.so")
	assert.Nil(t, node.queryHook)
}

// A plug-in that loads is initialized and installed through the same path a
// compiled-in hook takes, which is what makes the two interchangeable.
func TestInitHookInitializesALoadedPluginAsItAlwaysDid(t *testing.T) {
	h := &recordingQueryHook{}
	node := installQueryHook(t, nil)
	saveQueryNodeKey(t, paramtable.Get().QueryNodeCfg.SoPath.Key, "/tmp/some-tuner.so")
	loaded := mockey.Mock(hookutil.LoadPlugin[optimizers.QueryHook], mockey.OptGeneric).
		Return(h, nil).Build()
	defer loaded.UnPatch()

	require.NoError(t, node.initHook())

	assert.Same(t, h, node.queryHook)
	assert.Equal(t, []string{paramtable.Get().AutoIndexConfig.AutoIndexSearchConfig.GetValue()}, h.initWith,
		"a plug-in is initialized with the same autoIndex.params.search the compiled-in hook receives")
	require.Len(t, h.tuningWith, 1)
	assert.Equal(t, paramtable.Get().AutoIndexConfig.AutoIndexTuningConfig.GetValue(), h.tuningWith[0])
}

func TestInitHookReportsACompiledInQueryHookThatCannotInitialize(t *testing.T) {
	t.Run("search config", func(t *testing.T) {
		node := installQueryHook(t, &recordingQueryHook{initErr: errors.New("bad search config")})
		err := node.initHook()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "bad search config")
		assert.Nil(t, node.queryHook, "a hook that cannot initialize is not installed")
	})
	t.Run("tuning config", func(t *testing.T) {
		node := installQueryHook(t, &recordingQueryHook{tuningErr: errors.New("bad tuning config")})
		err := node.initHook()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "bad tuning config")
		assert.Nil(t, node.queryHook)
	})
}
