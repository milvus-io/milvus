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

func TestVersionGateSwitcher_Validate(t *testing.T) {
	valid := func() *VersionGateSwitcher {
		return &VersionGateSwitcher{
			EnableAutoSwitchValue: "auto",
			PreSwitchValue:        "false",
			GateVersion:           "2.6.23",
			TargetValue:           "true",
			SwitchDelay:           time.Second,
		}
	}

	t.Run("valid switcher passes", func(t *testing.T) {
		assert.NotPanics(t, func() { valid().Validate() })
	})

	cases := []struct {
		name   string
		mutate func(*VersionGateSwitcher)
	}{
		{"empty sentinel", func(s *VersionGateSwitcher) { s.EnableAutoSwitchValue = "" }},
		{"empty pre-switch value", func(s *VersionGateSwitcher) { s.PreSwitchValue = "" }},
		{"empty gate version", func(s *VersionGateSwitcher) { s.GateVersion = "" }},
		{"malformed gate version", func(s *VersionGateSwitcher) { s.GateVersion = "not-a-version" }},
		{"empty target value", func(s *VersionGateSwitcher) { s.TargetValue = "" }},
		{"negative switch delay", func(s *VersionGateSwitcher) { s.SwitchDelay = -time.Second }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := valid()
			tc.mutate(s)
			assert.Panics(t, func() { s.Validate() })
		})
	}
}

func TestVersionGateSwitcher_EffectiveValue(t *testing.T) {
	// ComponentParam.Init is guarded by sync.Once, so one initialized instance
	// is shared by all subtests (they never run in parallel).
	params := &ComponentParam{}
	params.Init(NewBaseTable(SkipRemote(true)))

	t.Run("no switcher keeps original behavior", func(t *testing.T) {
		item := &params.FunctionCfg.BatchFactor // no VersionGateSwitcher
		assert.Equal(t, "5", item.GetValue())
		assert.Equal(t, 5, item.GetAsInt())
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

	t.Run("sentinel value -> gate not flipped, keeps pre-switch value", func(t *testing.T) {
		item := &params.FunctionCfg.EnableWriteBeforeMaterialization
		// The value is still the sentinel (default "auto"): every read path
		// resolves the pre-switch value until the confirmator flips it.
		assert.Equal(t, "false", item.GetValue())
		assert.False(t, item.GetAsBool())
	})

	t.Run("explicit false keeps legacy value", func(t *testing.T) {
		item := &params.FunctionCfg.EnableWriteBeforeMaterialization
		old := item.SwapTempValue("false")
		defer item.SwapTempValue(old)
		assert.Equal(t, "false", item.GetValue())
		assert.False(t, item.GetAsBool())
	})

	t.Run("explicit true force enables and bypasses the gate", func(t *testing.T) {
		item := &params.FunctionCfg.EnableWriteBeforeMaterialization
		old := item.SwapTempValue("true")
		defer item.SwapTempValue(old)
		assert.Equal(t, "true", item.GetValue())
		assert.True(t, item.GetAsBool())
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
		assert.Equal(t, "false", a.GetValue())
		assert.Equal(t, "zstd-v1", b.GetValue())
	})
}
