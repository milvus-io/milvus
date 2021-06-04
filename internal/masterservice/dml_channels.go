// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package masterservice

import (
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"go.uber.org/zap"
)

type dmlChannels struct {
	core *Core
	lock sync.RWMutex
	dml  map[string]msgstream.MsgStream
}

func newDMLChannels(c *Core) *dmlChannels {
	return &dmlChannels{
		core: c,
		lock: sync.RWMutex{},
		dml:  make(map[string]msgstream.MsgStream),
	}
}

func (d *dmlChannels) GetNumChannles() int {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return len(d.dml)
}

func (d *dmlChannels) ProduceAll(pack *msgstream.MsgPack) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	for n, ms := range d.dml {
		if err := ms.Produce(pack); err != nil {
			log.Debug("msgstream produce error", zap.String("name", n), zap.Error(err))
		}
	}
}

func (d *dmlChannels) BroadcastAll(pack *msgstream.MsgPack) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	for n, ms := range d.dml {
		if err := ms.Broadcast(pack); err != nil {
			log.Debug("msgstream broadcast error", zap.String("name", n), zap.Error(err))
		}
	}
}

func (d *dmlChannels) Produce(name string, pack *msgstream.MsgPack) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	var err error
	ms, ok := d.dml[name]
	if !ok {
		ms, err = d.core.msFactory.NewMsgStream(d.core.ctx)
		if err != nil {
			return fmt.Errorf("create mstream failed, name = %s, error=%w", name, err)
		}
		ms.AsProducer([]string{name})
		d.dml[name] = ms
	}
	return ms.Produce(pack)

}

func (d *dmlChannels) Broadcast(name string, pack *msgstream.MsgPack) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	var err error
	ms, ok := d.dml[name]
	if !ok {
		ms, err = d.core.msFactory.NewMsgStream(d.core.ctx)
		if err != nil {
			return fmt.Errorf("create msgtream failed, name = %s, error=%w", name, err)
		}
		ms.AsProducer([]string{name})
		d.dml[name] = ms
	}
	return ms.Broadcast(pack)
}

func (d *dmlChannels) AddProducerChannles(names ...string) {
	d.lock.Lock()
	defer d.lock.Unlock()

	var err error
	for _, name := range names {
		ms, ok := d.dml[name]
		if !ok {
			ms, err = d.core.msFactory.NewMsgStream(d.core.ctx)
			if err != nil {
				log.Debug("add msgstream failed", zap.String("name", name), zap.Error(err))
				continue
			}
			ms.AsProducer([]string{name})
			d.dml[name] = ms
		}
	}
}

func (d *dmlChannels) RemoveProducerChannels(names ...string) {
	d.lock.Lock()
	defer d.lock.Unlock()

	for _, name := range names {
		if ms, ok := d.dml[name]; ok {
			ms.Close()
			delete(d.dml, name)
		}
	}
}
