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

package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRLSModeString(t *testing.T) {
	tests := []struct {
		mode     RLSMode
		expected string
	}{
		{RLSModeDisabled, "disabled"},
		{RLSModePermissive, "permissive"},
		{RLSModeStrict, "strict"},
		{RLSMode(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.mode.String())
		})
	}
}

func TestDefaultRLSConfig(t *testing.T) {
	config := DefaultRLSConfig()

	assert.NotNil(t, config)
	assert.Equal(t, RLSModePermissive, config.Mode)
	assert.False(t, config.Enabled)
	assert.True(t, config.AuditEnabled)
	assert.Equal(t, 3600, config.CacheExpirationSeconds)
	assert.Equal(t, 100, config.MaxPoliciesPerCollection)
	assert.Equal(t, 50, config.MaxUserTags)
	assert.Equal(t, 4096, config.MaxExpressionLength)
}

func TestGetSetRLSConfig(t *testing.T) {
	// Save original config
	originalConfig := GetRLSConfig()
	defer SetRLSConfig(originalConfig)

	// Create and set a new config
	newConfig := &RLSConfig{
		Mode:                     RLSModeStrict,
		Enabled:                  false,
		AuditEnabled:             false,
		CacheExpirationSeconds:   1800,
		MaxPoliciesPerCollection: 50,
		MaxUserTags:              25,
		MaxExpressionLength:      2048,
	}
	SetRLSConfig(newConfig)

	retrieved := GetRLSConfig()
	assert.Equal(t, newConfig, retrieved)

	// Test setting nil config (should not change)
	SetRLSConfig(nil)
	assert.Equal(t, newConfig, GetRLSConfig())
}

func TestRLSConfigGetMode(t *testing.T) {
	config := &RLSConfig{Mode: RLSModeStrict}
	assert.Equal(t, RLSModeStrict, config.GetMode())

	config.SetMode(RLSModePermissive)
	assert.Equal(t, RLSModePermissive, config.GetMode())
}

func TestRLSConfigIsEnabled(t *testing.T) {
	Params.Save(Params.CommonCfg.AuthorizationEnabled.Key, "true")
	defer Params.Reset(Params.CommonCfg.AuthorizationEnabled.Key)

	tests := []struct {
		name     string
		enabled  bool
		mode     RLSMode
		expected bool
	}{
		{"enabled and permissive", true, RLSModePermissive, true},
		{"enabled and strict", true, RLSModeStrict, true},
		{"enabled but disabled mode", true, RLSModeDisabled, false},
		{"disabled and permissive", false, RLSModePermissive, false},
		{"disabled and strict", false, RLSModeStrict, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &RLSConfig{Enabled: tt.enabled, Mode: tt.mode}
			assert.Equal(t, tt.expected, config.IsEnabled())
		})
	}
}

func TestRLSConfigSetEnabled(t *testing.T) {
	config := &RLSConfig{}

	config.SetEnabled(true)
	assert.True(t, config.Enabled)

	config.SetEnabled(false)
	assert.False(t, config.Enabled)
}

func TestRLSConfigIsStrictMode(t *testing.T) {
	config := &RLSConfig{Mode: RLSModeStrict}
	assert.True(t, config.IsStrictMode())

	config.Mode = RLSModePermissive
	assert.False(t, config.IsStrictMode())

	config.Mode = RLSModeDisabled
	assert.False(t, config.IsStrictMode())
}

func TestRLSConfigShouldFailOnError(t *testing.T) {
	config := &RLSConfig{Mode: RLSModeStrict}
	assert.True(t, config.ShouldFailOnError())

	config.Mode = RLSModePermissive
	assert.False(t, config.ShouldFailOnError())
}

func TestRLSConfigIsAuditEnabled(t *testing.T) {
	config := &RLSConfig{AuditEnabled: true}
	assert.True(t, config.IsAuditEnabled())

	config.AuditEnabled = false
	assert.False(t, config.IsAuditEnabled())
}

func TestRLSConfigGetLimits(t *testing.T) {
	config := &RLSConfig{
		MaxPoliciesPerCollection: 100,
		MaxUserTags:              50,
		MaxExpressionLength:      4096,
	}

	assert.Equal(t, 100, config.GetMaxPoliciesPerCollection())
	assert.Equal(t, 50, config.GetMaxUserTags())
	assert.Equal(t, 4096, config.GetMaxExpressionLength())
}

func TestRLSConfigConcurrency(t *testing.T) {
	config := &RLSConfig{
		Mode:    RLSModePermissive,
		Enabled: true,
	}

	done := make(chan bool)

	// Multiple readers
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				_ = config.GetMode()
				_ = config.IsEnabled()
				_ = config.IsStrictMode()
			}
			done <- true
		}()
	}

	// Multiple writers
	for i := 0; i < 5; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				config.SetMode(RLSModeStrict)
				config.SetEnabled(false)
				config.SetMode(RLSModePermissive)
				config.SetEnabled(true)
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 15; i++ {
		<-done
	}
}
