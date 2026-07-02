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
	"context"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	loadConfigWatcherInterval           = time.Minute
	loadConfigReplicaPromotionInterval  = 5 * time.Second
	loadConfigWatcherBackoffInitial     = 10 * time.Millisecond
	loadConfigWatcherBackoffMaxInterval = 10 * time.Minute
)

// NewLoadConfigWatcher creates a new load config watcher.
func NewLoadConfigWatcher(s *Server) *LoadConfigWatcher {
	w := &LoadConfigWatcher{
		triggerCh: make(chan struct{}, 10),
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

	previousReplicaNum                   int32
	previousRGs                          []string
	previousForceOverrideUserReplicaMode bool
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
		w.Logger().Info(context.TODO(),

			"load config watcher stopped")
	}()
	w.Logger().Info(context.TODO(),

		"load config watcher started")

	balanceTimer := typeutil.NewBackoffTimer(typeutil.BackoffTimerConfig{
		Default: loadConfigWatcherInterval,
		Backoff: typeutil.BackoffConfig{
			InitialInterval: loadConfigWatcherBackoffInitial,
			Multiplier:      2,
			MaxInterval:     loadConfigWatcherBackoffMaxInterval,
		},
	})
	promotionTicker := time.NewTicker(loadConfigReplicaPromotionInterval)
	defer promotionTicker.Stop()

	for {
		nextTimer, _ := balanceTimer.NextTimer()
		select {
		case <-w.notifier.Context().Done():
			return
		case <-w.triggerCh:
			w.Logger().Info(context.TODO(),

				"load config watcher triggered")
		case <-nextTimer:
		case <-promotionTicker.C:
			w.s.tryPromoteReadyLoadConfigReplicas(w.notifier.Context())
			continue
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
	w.s.tryPromoteReadyLoadConfigReplicas(w.notifier.Context())

	newReplicaNum := paramtable.Get().QueryCoordCfg.ClusterLevelLoadReplicaNumber.GetAsInt32()
	newRGs := paramtable.Get().QueryCoordCfg.ClusterLevelLoadResourceGroups.GetAsStrings()
	forceOverrideUserReplicaMode := paramtable.Get().QueryCoordCfg.ClusterLevelLoadForceOverrideUserReplicaMode.GetAsBool()

	if newReplicaNum == 0 && len(newRGs) == 0 {
		// default cluster level load config, nothing to do for it.
		return nil
	}

	if newReplicaNum <= 0 || len(newRGs) == 0 {
		w.Logger().Info(context.TODO(),

			"illegal cluster level load config, skip it", mlog.Int32("replica_num", newReplicaNum), mlog.Strings("resource_groups", newRGs))
		return nil
	}

	if len(newRGs) != 1 && len(newRGs) != int(newReplicaNum) {
		w.Logger().Info(context.TODO(),

			"illegal cluster level load config, skip it", mlog.Int32("replica_num", newReplicaNum), mlog.Strings("resource_groups", newRGs))
		return nil
	}

	left, right := lo.Difference(w.previousRGs, newRGs)
	rgChanged := len(left) > 0 || len(right) > 0
	forceOverrideChanged := w.previousForceOverrideUserReplicaMode != forceOverrideUserReplicaMode
	if w.previousReplicaNum == newReplicaNum && !rgChanged && !forceOverrideChanged {
		w.Logger().Info(context.TODO(),

			"no need to update load config, skip it", mlog.Int32("replica_num", newReplicaNum), mlog.Strings("resource_groups", newRGs))
		return nil
	}

	// try to check load config changes after restart, and try to update replicas
	collectionIDs := w.s.meta.GetAll(w.notifier.Context())
	collectionIDs = lo.Filter(collectionIDs, func(collectionID int64, _ int) bool {
		collection := w.s.meta.GetCollection(w.notifier.Context(), collectionID)
		if collection.UserSpecifiedReplicaMode && !forceOverrideUserReplicaMode {
			w.Logger().Info(context.TODO(),

				"collection is user specified replica mode, skip update load config", mlog.FieldCollectionID(collectionID))
			return false
		}
		return true
	})

	if len(collectionIDs) == 0 {
		w.Logger().Info(context.TODO(),

			"no collection to update load config, skip it")
	}

	if err := w.s.updateLoadConfig(w.notifier.Context(), collectionIDs, newReplicaNum, newRGs, true); err != nil {
		w.Logger().Warn(context.TODO(),

			"failed to update load config", mlog.Err(err))
		return err
	}
	w.s.tryPromoteReadyLoadConfigReplicas(w.notifier.Context())
	w.Logger().Info(context.TODO(),

		"apply load config changes",
		mlog.Int64s("collectionIDs", collectionIDs),
		mlog.Int32("previousReplicaNum", w.previousReplicaNum),
		mlog.Strings("previousResourceGroups", w.previousRGs),
		mlog.Int32("replicaNum", newReplicaNum),
		mlog.Strings("resourceGroups", newRGs))
	w.previousReplicaNum = newReplicaNum
	w.previousRGs = newRGs
	w.previousForceOverrideUserReplicaMode = forceOverrideUserReplicaMode
	return nil
}

// Close closes the load config watcher.
func (w *LoadConfigWatcher) Close() {
	w.notifier.Cancel()
	w.notifier.BlockUntilFinish()
}
