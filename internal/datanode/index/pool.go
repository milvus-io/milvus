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

// indexBuildPool wraps a concurrency pool that auto-initializes from a ParamItem
// and watches for dynamic config changes.
type indexBuildPool struct {
	name string
	once sync.Once
	pool *conc.Pool[any]
	// param returns the ParamItem that controls pool size.
	param func() *paramtable.ParamItem
}

func newIndexBuildPool(name string, param func() *paramtable.ParamItem) *indexBuildPool {
	return &indexBuildPool{name: name, param: param}
}

func (p *indexBuildPool) init() {
	item := p.param()
	size := item.GetAsInt()
	p.pool = conc.NewPool[any](size)

	pt := paramtable.Get()
	pt.Watch(item.Key, config.NewHandler(item.Key, p.resize))
	log.Info("init index building pool done", zap.String("name", p.name), zap.Int("size", size))
}

func (p *indexBuildPool) resize(evt *config.Event) {
	if evt.HasUpdated {
		newSize := p.param().GetAsInt()
		l := log.Ctx(context.Background()).With(zap.String("name", p.name), zap.Int("newSize", newSize))

		if err := p.Get().Resize(newSize); err != nil {
			l.Warn("failed to resize index build pool", zap.Error(err))
			return
		}
		l.Info("index building pool resize successfully")
	}
}

func (p *indexBuildPool) Get() *conc.Pool[any] {
	p.once.Do(p.init)
	return p.pool
}

var (
	vecPool = newIndexBuildPool("vec", func() *paramtable.ParamItem {
		return &paramtable.Get().DataNodeCfg.MaxVecIndexBuildConcurrency
	})
	standalonePool = newIndexBuildPool("standalone", func() *paramtable.ParamItem {
		return &paramtable.Get().DataNodeCfg.StandaloneIndexBuildParallelism
	})
)

func GetVecIndexBuildPool() *conc.Pool[any] {
	return vecPool.Get()
}

func GetStandaloneIndexBuildPool() *conc.Pool[any] {
	return standalonePool.Get()
}
