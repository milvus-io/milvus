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

package index

import (
	"context"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var (
	indexBuildPool         *conc.Pool[any]
	indexBuildPoolInitOnce sync.Once
)

func initIndexBuildPool() {
	pt := paramtable.Get()
	initPoolSize := pt.DataNodeCfg.MaxIndexBuildConcurrency.GetAsInt()
	indexBuildPool = conc.NewPool[any](
		initPoolSize,
	)

	watchKey := pt.DataNodeCfg.MaxIndexBuildConcurrency.Key
	pt.Watch(watchKey, config.NewHandler(watchKey, resizeIndexBuildPool))
	log.Info("init index building pool done", zap.Int("size", initPoolSize))
}

func resizeIndexBuildPool(evt *config.Event) {
	if evt.HasUpdated {
		newSize := paramtable.Get().DataNodeCfg.MaxIndexBuildConcurrency.GetAsInt()
		log := log.Ctx(context.Background()).With(zap.Int("newSize", newSize))

		err := GetIndexBuildPool().Resize(newSize)
		if err != nil {
			log.Warn("failed to resize pool", zap.Error(err))
			return
		}
		log.Info("index building pool resize successfully")
	}
}

func GetIndexBuildPool() *conc.Pool[any] {
	indexBuildPoolInitOnce.Do(initIndexBuildPool)
	return indexBuildPool
}
