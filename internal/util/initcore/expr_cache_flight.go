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

package initcore

/*
#cgo pkg-config: milvus_core
#include "common/init_c.h"
*/
import "C"

import (
	"context"
	"strconv"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type exprCacheFlightConfig struct {
	enabled                                             bool
	maxWaitMs, maxFlights, maxWaiters, maxWaitersPerKey int64
}

func exprCacheFlightParams(pt *paramtable.ComponentParam) []*paramtable.ParamItem {
	p := &pt.QueryNodeCfg
	return []*paramtable.ParamItem{
		&p.ExprResCacheSingleFlightEnabled,
		&p.ExprResCacheSingleFlightMaxWaitMs,
		&p.ExprResCacheSingleFlightMaxFlights,
		&p.ExprResCacheSingleFlightMaxWaiters,
		&p.ExprResCacheSingleFlightMaxWaitersPerKey,
	}
}

func readExprCacheFlightConfig(pt *paramtable.ComponentParam) (exprCacheFlightConfig, error) {
	var result exprCacheFlightConfig
	items := exprCacheFlightParams(pt)
	enabled, err := strconv.ParseBool(items[0].GetValue())
	if err != nil {
		return result, merr.WrapErrParameterInvalidMsg("%s must be a boolean", items[0].Key)
	}
	result.enabled = enabled
	values := []*int64{&result.maxWaitMs, &result.maxFlights, &result.maxWaiters, &result.maxWaitersPerKey}
	for i, dest := range values {
		value, err := strconv.ParseInt(items[i+1].GetValue(), 10, 64)
		if err != nil || value <= 0 {
			return result, merr.WrapErrParameterInvalidMsg("%s must be a positive integer", items[i+1].Key)
		}
		*dest = value
	}
	if result.maxWaitMs > 60000 {
		return result, merr.WrapErrParameterInvalidMsg("%s must not exceed 60000", items[1].Key)
	}
	return result, nil
}

func applyExprCacheFlightConfig(cfg exprCacheFlightConfig) error {
	if !bool(C.SetExprResCacheSingleFlightConfig(C.bool(cfg.enabled), C.int64_t(cfg.maxWaitMs),
		C.int64_t(cfg.maxFlights),
		C.int64_t(cfg.maxWaiters), C.int64_t(cfg.maxWaitersPerKey))) {
		return merr.WrapErrServiceInternalMsg("failed to apply expression single-flight configuration")
	}
	return nil
}

// Use normalized dispatcher keys and a post-registration sync, including
// Create/Delete events. Changing flight policy never rebuilds the cache backend.
func registerExprCacheFlightConfig(ctx context.Context, pt *paramtable.ComponentParam, apply func(exprCacheFlightConfig) error) error {
	var mu sync.Mutex
	syncConfig := func() error {
		mu.Lock()
		defer mu.Unlock()
		cfg, err := readExprCacheFlightConfig(pt)
		if err != nil {
			// Invalid settings disable coordination without failing startup or refresh.
			if disableErr := apply(exprCacheFlightConfig{false, 1000, 1024, 64, 8}); disableErr != nil {
				return merr.Wrap(disableErr, "disable expression single-flight after invalid configuration")
			}
			mlog.Warn(ctx, "Disabled expression single-flight after invalid configuration", mlog.Err(err))
			return nil
		}
		return apply(cfg)
	}
	for _, item := range exprCacheFlightParams(pt) {
		pt.Watch(item.Key, config.NewHandler(item.Key+".singleflight", func(event *config.Event) {
			if event.HasUpdated {
				if err := syncConfig(); err != nil {
					mlog.Warn(ctx, "Failed to update expression single-flight configuration", mlog.Err(err))
				}
			}
		}))
	}
	return syncConfig()
}
