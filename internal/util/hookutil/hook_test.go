/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hookutil

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestInitHook(t *testing.T) {
	paramtable.Init()
	Params := paramtable.Get()
	paramtable.Get().Save(Params.ProxyCfg.SoPath.Key, "")
	initHook()
	assert.IsType(t, DefaultHook{}, GetHook())

	paramtable.Get().Save(Params.ProxyCfg.SoPath.Key, "/a/b/hook.so")
	err := initHook()
	assert.Error(t, err)
	paramtable.Get().Save(Params.ProxyCfg.SoPath.Key, "")
}

func TestHookInitPanicError(t *testing.T) {
	paramtable.Init()
	p := paramtable.Get()
	p.Save(p.ProxyCfg.SoPath.Key, "/a/b/hook.so")
	defer p.Reset(p.ProxyCfg.SoPath.Key)
	err := initHook()
	assert.Error(t, err)
	assert.Panics(t, func() {
		initOnce = sync.Once{}
		InitOnceHook()
	})
}

func TestHookInitLogError(t *testing.T) {
	paramtable.Init()
	p := paramtable.Get()
	p.Save(p.ProxyCfg.SoPath.Key, "/a/b/hook.so")
	defer p.Reset(p.ProxyCfg.SoPath.Key)
	p.Save(p.CommonCfg.PanicWhenPluginFail.Key, "false")
	defer p.Reset(p.CommonCfg.PanicWhenPluginFail.Key)
	err := initHook()
	assert.Error(t, err)
	assert.NotPanics(t, func() {
		initOnce = sync.Once{}
		InitOnceHook()
	})
}

func TestDefaultHook(t *testing.T) {
	d := &DefaultHook{}
	assert.NoError(t, d.Init(nil))
	{
		_, err := d.VerifyAPIKey("key")
		assert.Error(t, err)
	}
	assert.NotPanics(t, func() {
		d.Release()
	})
}
