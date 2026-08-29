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

package paramtable

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestVersionGateSwitcher_EffectiveValue(t *testing.T) {
	// ComponentParam.Init is guarded by sync.Once, so one initialized instance
	// is shared by all subtests (they never run in parallel).
	params := &ComponentParam{}
	params.Init(NewBaseTable(SkipRemote(true)))

	t.Run("no switcher keeps original behavior", func(t *testing.T) {
		item := &params.FunctionCfg.BatchFactor // no VersionGateSwitcher
		v, ok := item.EffectiveValue()
		assert.True(t, ok)
		assert.Equal(t, item.GetValue(), v)
	})

	t.Run("write-before-materialization default is auto (default equals sentinel)", func(t *testing.T) {
		item := &params.FunctionCfg.EnableWriteBeforeMaterialization
		assert.NotNil(t, item.VersionGateSwitcher)
		assert.Equal(t, "auto", item.VersionGateSwitcher.EnableAutoSwitchValue)
		assert.Equal(t, "false", item.VersionGateSwitcher.PreSwitchValue)
		assert.Equal(t, "2.6.23", item.VersionGateSwitcher.GateVersion)
		assert.Equal(t, "true", item.VersionGateSwitcher.TargetValue)
		assert.Equal(t, "auto", item.DefaultValue) // default == sentinel -> auto switch by default
	})

	t.Run("sentinel value -> not activated, keeps pre-switch value", func(t *testing.T) {
		item := &params.FunctionCfg.EnableWriteBeforeMaterialization
		v, ok := item.EffectiveValue()
		assert.False(t, ok) // the gate has not flipped (value is still the sentinel)
		assert.Equal(t, "false", v)
		assert.False(t, item.GetAsBoolEffective())
	})

	t.Run("explicit false keeps legacy value", func(t *testing.T) {
		item := &params.FunctionCfg.EnableWriteBeforeMaterialization
		old := item.SwapTempValue("false")
		defer item.SwapTempValue(old)
		v, ok := item.EffectiveValue()
		assert.True(t, ok)
		assert.Equal(t, "false", v)
		assert.False(t, item.GetAsBoolEffective())
	})

	t.Run("explicit true force enables and bypasses the gate", func(t *testing.T) {
		item := &params.FunctionCfg.EnableWriteBeforeMaterialization
		old := item.SwapTempValue("true")
		defer item.SwapTempValue(old)
		v, ok := item.EffectiveValue()
		assert.True(t, ok)
		assert.Equal(t, "true", v)
		assert.True(t, item.GetAsBoolEffective())
	})

	t.Run("different switchers keep their own pre-switch values", func(t *testing.T) {
		a := &ParamItem{
			Key:          "test.a",
			DefaultValue: "auto",
			VersionGateSwitcher: &VersionGateSwitcher{
				EnableAutoSwitchValue: "auto",
				PreSwitchValue:        "false",
				GateVersion:           "2.6.23",
				TargetValue:           "true",
				SwitchDelay:           time.Second,
			},
		}
		b := &ParamItem{
			Key:          "test.b",
			DefaultValue: "auto",
			VersionGateSwitcher: &VersionGateSwitcher{
				EnableAutoSwitchValue: "auto",
				PreSwitchValue:        "zstd-v1",
				GateVersion:           "3.0.0",
				TargetValue:           "zstd-v2",
				SwitchDelay:           time.Second,
			},
		}
		// Both items read the sentinel value (default): neither is activated,
		// and each falls back to its own PreSwitchValue.
		// (same-package test: set tempValue directly, bare ParamItem has no manager)
		auto := "auto"
		a.tempValue.Store(&auto)
		b.tempValue.Store(&auto)
		av, aok := a.EffectiveValue()
		assert.False(t, aok)
		assert.Equal(t, "false", av)
		bv, bok := b.EffectiveValue()
		assert.False(t, bok)
		assert.Equal(t, "zstd-v1", bv)
	})
}
