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
	"strings"
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// RLSMode defines the RLS enforcement mode
type RLSMode int

const (
	// RLSModeDisabled means RLS is completely disabled
	RLSModeDisabled RLSMode = iota
	// RLSModePermissive means runtime RLS evaluation errors are logged but don't block operations.
	// Missing/no-matching policies are still denied when RLS is enabled.
	RLSModePermissive
	// RLSModeStrict means RLS errors block operations (fail-closed)
	RLSModeStrict
)

// String returns a string representation of the RLS mode
func (m RLSMode) String() string {
	switch m {
	case RLSModeDisabled:
		return "disabled"
	case RLSModePermissive:
		return "permissive"
	case RLSModeStrict:
		return "strict"
	default:
		return "unknown"
	}
}

// RLSConfig holds the RLS configuration
type RLSConfig struct {
	mu sync.RWMutex

	// Mode controls RLS enforcement behavior
	Mode RLSMode

	// Enabled controls whether RLS is globally enabled
	Enabled bool

	// AuditEnabled controls whether audit logging is enabled
	AuditEnabled bool

	// CacheExpirationSeconds controls how long cached policies are valid
	CacheExpirationSeconds int

	// MaxCacheEntries controls the maximum number of entries in the L2 merged expression cache
	MaxCacheEntries int

	// MaxPoliciesPerCollection limits policies per collection
	MaxPoliciesPerCollection int

	// MaxUserTags limits tags per user
	MaxUserTags int

	// MaxExpressionLength limits the length of USING/CHECK expressions
	MaxExpressionLength int

	// FailOnCacheError controls whether to fail if cache is unavailable
	FailOnCacheError bool

	// FailOnContextError controls whether to fail if user context cannot be obtained
	FailOnContextError bool
}

// DefaultRLSConfig returns the default RLS configuration
func DefaultRLSConfig() *RLSConfig {
	return &RLSConfig{
		Mode:                     RLSModePermissive,
		Enabled:                  true,
		AuditEnabled:             true,
		CacheExpirationSeconds:   3600,
		MaxCacheEntries:          10000,
		MaxPoliciesPerCollection: 100,
		MaxUserTags:              50,
		MaxExpressionLength:      4096,
		FailOnCacheError:         false,
		FailOnContextError:       false,
	}
}

// globalRLSConfig is the singleton configuration
var (
	globalRLSConfigMu sync.RWMutex
	globalRLSConfig   = DefaultRLSConfig()
)

// GetRLSConfig returns the global RLS configuration
func GetRLSConfig() *RLSConfig {
	globalRLSConfigMu.RLock()
	defer globalRLSConfigMu.RUnlock()
	return globalRLSConfig
}

// SetRLSConfig sets the global RLS configuration
func SetRLSConfig(config *RLSConfig) {
	if config != nil {
		globalRLSConfigMu.Lock()
		defer globalRLSConfigMu.Unlock()
		globalRLSConfig = config
	}
}

// GetMode returns the current RLS mode
func (c *RLSConfig) GetMode() RLSMode {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Mode
}

// SetMode sets the RLS mode
func (c *RLSConfig) SetMode(mode RLSMode) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Mode = mode
}

// IsEnabled returns whether RLS is enabled
func (c *RLSConfig) IsEnabled() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Enabled && c.Mode != RLSModeDisabled
}

// SetEnabled sets whether RLS is enabled
func (c *RLSConfig) SetEnabled(enabled bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Enabled = enabled
}

// IsStrictMode returns whether RLS is in strict (fail-closed) mode
func (c *RLSConfig) IsStrictMode() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Mode == RLSModeStrict
}

// ShouldFailOnError returns whether to fail the operation on RLS errors
func (c *RLSConfig) ShouldFailOnError() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Mode == RLSModeStrict
}

// ShouldFailOnCacheError returns whether to fail if cache is unavailable
func (c *RLSConfig) ShouldFailOnCacheError() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.FailOnCacheError && c.Mode == RLSModeStrict
}

// ShouldFailOnContextError returns whether to fail if context cannot be obtained
func (c *RLSConfig) ShouldFailOnContextError() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.FailOnContextError && c.Mode == RLSModeStrict
}

// IsAuditEnabled returns whether audit logging is enabled
func (c *RLSConfig) IsAuditEnabled() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.AuditEnabled
}

// SetAuditEnabled sets whether audit logging is enabled
func (c *RLSConfig) SetAuditEnabled(enabled bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.AuditEnabled = enabled
	GetRLSAuditLogger().SetEnabled(enabled)
}

// GetMaxPoliciesPerCollection returns the max policies limit
func (c *RLSConfig) GetMaxPoliciesPerCollection() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.MaxPoliciesPerCollection
}

// GetMaxUserTags returns the max user tags limit
func (c *RLSConfig) GetMaxUserTags() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.MaxUserTags
}

// GetMaxExpressionLength returns the max expression length limit
func (c *RLSConfig) GetMaxExpressionLength() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.MaxExpressionLength
}

// GetMaxCacheEntries returns the max cache entries limit
func (c *RLSConfig) GetMaxCacheEntries() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.MaxCacheEntries <= 0 {
		return 10000 // Default fallback
	}
	return c.MaxCacheEntries
}

// GetCacheExpirationDuration returns the cache expiration as time.Duration
func (c *RLSConfig) GetCacheExpirationDuration() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.CacheExpirationSeconds <= 0 {
		return time.Hour // Default fallback
	}
	return time.Duration(c.CacheExpirationSeconds) * time.Second
}

// parseRLSMode parses a string into RLSMode
func parseRLSMode(s string) RLSMode {
	switch strings.ToLower(s) {
	case "strict":
		return RLSModeStrict
	case "permissive":
		return RLSModePermissive
	case "disabled":
		return RLSModeDisabled
	default:
		return RLSModePermissive
	}
}

// InitRLSConfigFromParams initializes the global RLS configuration from Milvus params
func InitRLSConfigFromParams() {
	params := paramtable.Get()
	config := &RLSConfig{
		Enabled:                  params.ProxyCfg.RLSEnabled.GetAsBool(),
		Mode:                     parseRLSMode(params.ProxyCfg.RLSMode.GetValue()),
		AuditEnabled:             params.ProxyCfg.RLSAuditEnabled.GetAsBool(),
		CacheExpirationSeconds:   params.ProxyCfg.RLSCacheExpirationSec.GetAsInt(),
		MaxCacheEntries:          params.ProxyCfg.RLSMaxCacheEntries.GetAsInt(),
		MaxPoliciesPerCollection: params.ProxyCfg.RLSMaxPoliciesPerCollection.GetAsInt(),
		MaxUserTags:              params.ProxyCfg.RLSMaxUserTags.GetAsInt(),
		MaxExpressionLength:      params.ProxyCfg.RLSMaxExpressionLength.GetAsInt(),
		FailOnCacheError:         params.ProxyCfg.RLSFailOnCacheError.GetAsBool(),
		FailOnContextError:       params.ProxyCfg.RLSFailOnContextError.GetAsBool(),
	}
	SetRLSConfig(config)
}

// RefreshRLSConfigFromParams refreshes the RLS configuration from current params
// This can be called when configuration changes at runtime
func RefreshRLSConfigFromParams() {
	InitRLSConfigFromParams()
}
