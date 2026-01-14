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

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// NewLoadConfigWatcher creates a new load config watcher.
func NewLoadConfigWatcher(s *Server) *LoadConfigWatcher {
	w := &LoadConfigWatcher{
		triggerCh: make(chan struct{}, 10),
		notifier:  syncutil.NewAsyncTaskNotifier[struct{}](),
		s:         s,
	}
	w.SetLogger(log.With(log.FieldModule(typeutil.QueryCoordRole), log.FieldComponent("load_config_watcher")))
	go w.background()
	return w
}

// LoadConfigWatcher is a watcher for load config changes.
type LoadConfigWatcher struct {
	log.Binder
	triggerCh chan struct{}
	notifier  *syncutil.AsyncTaskNotifier[struct{}]
	s         *Server

	previousReplicaNum int32
	previousRGs        []string
}

// Trigger triggers a load config change.
func (w *LoadConfigWatcher) Trigger() {
	select {
	case <-w.notifier.Context().Done():
	case w.triggerCh <- struct{}{}:
	}
}

// background is the background task for load config watcher.
func (w *LoadConfigWatcher) background() {
	defer func() {
		w.notifier.Finish(struct{}{})
		w.Logger().Info("load config watcher stopped")
	}()
	w.Logger().Info("load config watcher started")

	balanceTimer := typeutil.NewBackoffTimer(typeutil.BackoffTimerConfig{
		Default: time.Minute,
		Backoff: typeutil.BackoffConfig{
			InitialInterval: 10 * time.Millisecond,
			Multiplier:      2,
			MaxInterval:     10 * time.Minute,
		},
	})

	for {
		nextTimer, _ := balanceTimer.NextTimer()
		select {
		case <-w.notifier.Context().Done():
			return
		case <-w.triggerCh:
			w.Logger().Info("load config watcher triggered")
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
	newReplicaNum := paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.GetAsInt32()
	newRGs := paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.GetAsStrings()

	if newReplicaNum == 0 && len(newRGs) == 0 {
		// default cluster level load config, nothing to do for it.
		return nil
	}

	if newReplicaNum <= 0 || len(newRGs) == 0 {
		w.Logger().Info("illegal cluster level load config, skip it", zap.Int32("replica_num", newReplicaNum), zap.Strings("resource_groups", newRGs))
		return nil
	}

	if len(newRGs) != 1 && len(newRGs) != int(newReplicaNum) {
		w.Logger().Info("illegal cluster level load config, skip it", zap.Int32("replica_num", newReplicaNum), zap.Strings("resource_groups", newRGs))
		return nil
	}

	left, right := lo.Difference(w.previousRGs, newRGs)
	rgChanged := len(left) > 0 || len(right) > 0
	if w.previousReplicaNum == newReplicaNum && !rgChanged {
		w.Logger().Info("no need to update load config, skip it", zap.Int32("replica_num", newReplicaNum), zap.Strings("resource_groups", newRGs))
		return nil
	}

	// try to check load config changes after restart, and try to update replicas
	collectionIDs := w.s.meta.GetAll(w.notifier.Context())
	collectionIDs = lo.Filter(collectionIDs, func(collectionID int64, _ int) bool {
		collection := w.s.meta.GetCollection(w.notifier.Context(), collectionID)
		if collection.UserSpecifiedReplicaMode {
			w.Logger().Info("collection is user specified replica mode, skip update load config", zap.Int64("collectionID", collectionID))
			return false
		}
		return true
	})

	if len(collectionIDs) == 0 {
		w.Logger().Info("no collection to update load config, skip it")
	}

	if err := w.s.updateLoadConfig(w.notifier.Context(), collectionIDs, newReplicaNum, newRGs); err != nil {
		w.Logger().Warn("failed to update load config", zap.Error(err))
		return err
	}
	w.Logger().Info("apply load config changes",
		zap.Int64s("collectionIDs", collectionIDs),
		zap.Int32("previousReplicaNum", w.previousReplicaNum),
		zap.Strings("previousResourceGroups", w.previousRGs),
		zap.Int32("replicaNum", newReplicaNum),
		zap.Strings("resourceGroups", newRGs))
	w.previousReplicaNum = newReplicaNum
	w.previousRGs = newRGs
	return nil
}

// Close closes the load config watcher.
func (w *LoadConfigWatcher) Close() {
	w.notifier.Cancel()
	w.notifier.BlockUntilFinish()
}
