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

package querycoordv2

import (
	"time"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	loadConfigWatcherInterval           = time.Minute
	loadConfigWatcherBackoffInitial     = 10 * time.Millisecond
	loadConfigWatcherBackoffMaxInterval = 10 * time.Minute
)

// NewLoadConfigWatcher creates a new load config watcher.
func NewLoadConfigWatcher(s *Server) *LoadConfigWatcher {
	w := &LoadConfigWatcher{
		triggerCh: make(chan struct{}, 1),
		notifier:  syncutil.NewAsyncTaskNotifier[struct{}](),
		s:         s,
	}
	w.SetLogger(mlog.With(mlog.FieldModule(typeutil.QueryCoordRole), mlog.FieldComponent("load_config_watcher")))
	go w.background()
	return w
}

// LoadConfigWatcher is a watcher for load config changes.
type LoadConfigWatcher struct {
	mlog.Binder
	triggerCh chan struct{}
	notifier  *syncutil.AsyncTaskNotifier[struct{}]
	s         *Server

	handlers map[string]config.EventHandler
}

// Trigger triggers a load config change.
func (w *LoadConfigWatcher) Trigger() {
	select {
	case <-w.notifier.Context().Done():
	case w.triggerCh <- struct{}{}:
	default:
	}
}

// background is the background task for load config watcher.
func (w *LoadConfigWatcher) background() {
	defer func() {
		w.notifier.Finish(struct{}{})
		w.Logger().Info(w.notifier.Context(), "load config watcher stopped")
	}()
	w.Logger().Info(w.notifier.Context(), "load config watcher started")

	balanceTimer := typeutil.NewBackoffTimer(typeutil.BackoffTimerConfig{
		Default: loadConfigWatcherInterval,
		Backoff: typeutil.BackoffConfig{
			InitialInterval: loadConfigWatcherBackoffInitial,
			Multiplier:      2,
			MaxInterval:     loadConfigWatcherBackoffMaxInterval,
		},
	})

	for {
		nextTimer, _ := balanceTimer.NextTimer()
		select {
		case <-w.notifier.Context().Done():
			return
		case <-w.triggerCh:
			w.Logger().Info(w.notifier.Context(), "load config watcher triggered")
		case <-nextTimer:
		}
		if err := w.applyLoadConfigChanges(); err != nil {
			balanceTimer.EnableBackoff()
			continue
		}
		balanceTimer.DisableBackoff()
	}
}

// applyLoadConfigChanges applies the load config changes.
func (w *LoadConfigWatcher) applyLoadConfigChanges() error {
	// Reconcile each current configuration, including after recovery and retries.
	// A previously applied cluster setting does not prove every collection matches.
	var result error
	for id := range w.s.qviewsRuntime.loadConfigStore.Snapshot().ConfigsMap() {
		if err := w.notifier.Context().Err(); err != nil {
			return err
		}
		if err := w.s.updateCollectionLoadConfig(w.notifier.Context(), id, 0, nil, true); err != nil {
			result = merr.Combine(result, err)
		}
	}
	if result != nil {
		w.Logger().Warn(w.notifier.Context(), "failed to apply cluster load configuration", mlog.Err(result))
	}
	return result
}

// Close closes the load config watcher.
func (w *LoadConfigWatcher) Close() {
	for key, handler := range w.handlers {
		paramtable.Get().Unwatch(key, handler)
	}
	w.notifier.Cancel()
	w.notifier.BlockUntilFinish()
}
