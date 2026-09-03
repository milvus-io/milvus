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

package grpcmixcoord

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

// The coordinator engine is control-plane machinery a deployment hosts in the
// coordinator process. With none installed the two functions below do nothing.

func coordinatorEngine() extension.CoordinatorEngine {
	return extension.InstalledCoordinatorEngine()
}

// activeNotifier is what the coordinator implements to run work once this
// replica is ACTIVE; a standby never fires it.
type activeNotifier interface {
	OnActive(fn func())
}

// engineLifecycleMu orders an activation-time Start against a Stop, so a stop
// during activation neither races nor leaves the engine running.
var engineLifecycleMu sync.Mutex

// startCoordinatorEngine starts the installed engine over the coordinator
// client once this replica is ACTIVE. A start failure on activation is fatal:
// a coordinator serving without its engine would accept work nothing accounts
// for.
func startCoordinatorEngine(ctx context.Context, coord types.MixCoordComponent, client types.MixCoordClient) error {
	engine := coordinatorEngine()
	if engine == nil {
		return nil
	}
	start := func() error {
		if err := engine.Start(ctx, client); err != nil {
			return err
		}
		mlog.Info(ctx, "coordinator engine started")
		return nil
	}
	notifier, ok := coord.(activeNotifier)
	if !ok {
		return start()
	}
	notifier.OnActive(func() {
		engineLifecycleMu.Lock()
		defer engineLifecycleMu.Unlock()
		if err := start(); err != nil {
			mlog.Panic(ctx, "coordinator engine failed to start on activation", mlog.Err(err))
		}
	})
	return nil
}

// stopCoordinatorEngine stops the installed engine; its error is logged, not
// returned, because the coordinator shutdown must not hang on it.
func stopCoordinatorEngine(ctx context.Context) {
	engine := coordinatorEngine()
	if engine == nil {
		return
	}
	engineLifecycleMu.Lock()
	defer engineLifecycleMu.Unlock()
	if err := engine.Stop(); err != nil {
		mlog.Warn(ctx, "coordinator engine stop failed", mlog.Err(err))
	}
}
