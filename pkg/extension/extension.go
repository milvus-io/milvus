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

// Package extension is what a distribution that compiles its own behavior into
// the milvus binary installs at boot: a request hook, and an engine the
// coordinator runs while it is active. Everything else such a distribution
// needs is either the hook's own reach (every proxy RPC passes Mock, Before and
// After), a coordinator RPC, or a configuration item.
//
// A stock binary installs nothing: InstalledHook and InstalledCoordinatorEngine
// answer nil, and milvus behaves as it always did.
package extension

import (
	"sync/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
)

var (
	installedHook   atomic.Pointer[hookBox]
	installedEngine atomic.Pointer[engineBox]
)

type (
	hookBox   struct{ hook hook.Hook }
	engineBox struct{ engine CoordinatorEngine }
)

// SetHook installs a compiled-in request hook. hookutil prefers it over
// proxy.soPath and refuses a deployment that configures both. Call it before
// milvus starts; a nil hook leaves the stock behavior in place.
func SetHook(h hook.Hook) {
	installedHook.Store(&hookBox{hook: h})
}

// InstalledHook returns the installed request hook, or nil.
func InstalledHook() hook.Hook {
	if b := installedHook.Load(); b != nil {
		return b.hook
	}
	return nil
}

// SetCoordinatorEngine installs the engine the coordinator starts when it
// becomes active and stops on shutdown. Call it before milvus starts; nil
// installs nothing.
func SetCoordinatorEngine(e CoordinatorEngine) {
	installedEngine.Store(&engineBox{engine: e})
}

// InstalledCoordinatorEngine returns the installed engine, or nil.
func InstalledCoordinatorEngine() CoordinatorEngine {
	if b := installedEngine.Load(); b != nil {
		return b.engine
	}
	return nil
}
