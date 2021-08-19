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

package rootcoord

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

// GetNumChannels get current dml channel count
func (d *dmlChannels) GetNumChannels() int {
	d.lock.RLock()
	defer d.lock.RUnlock()
	return len(d.dml)
}

// ListChannels lists all dml channel names
func (d *dmlChannels) ListChannels() []string {
	d.lock.RLock()
	defer d.lock.RUnlock()

	ret := make([]string, 0, len(d.dml))
	for n := range d.dml {
		ret = append(ret, n)
	}
	return ret

}

// Produce produces msg pack into specified channel
func (d *dmlChannels) Produce(name string, pack *msgstream.MsgPack) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	ds, ok := d.dml[name]
	if !ok {
		return fmt.Errorf("channel %s not exist", name)
	}
	if err := ds.Produce(pack); err != nil {
		return err
	}
	return nil
}

// Broadcast broadcasts msg pack into specified channel
func (d *dmlChannels) Broadcast(name string, pack *msgstream.MsgPack) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	ds, ok := d.dml[name]
	if !ok {
		return fmt.Errorf("channel %s not exist", name)
	}
	if err := ds.Broadcast(pack); err != nil {
		return err
	}
	return nil
}

// BroadcastAll invoke broadcast with provided msg pack in all channels specified
func (d *dmlChannels) BroadcastAll(channels []string, pack *msgstream.MsgPack) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	for _, ch := range channels {
		ds, ok := d.dml[ch]
		if !ok {
			return fmt.Errorf("channel %s not exist", ch)
		}
		if err := ds.Broadcast(pack); err != nil {
			return err
		}
	}
	return nil
}

// AddProducerChannels add named channels as producer
func (d *dmlChannels) AddProducerChannels(names ...string) {
	d.lock.Lock()
	defer d.lock.Unlock()

	for _, name := range names {
		log.Debug("add dml channel", zap.String("channel name", name))
		_, ok := d.dml[name]
		if !ok {
			ms, err := d.core.msFactory.NewMsgStream(d.core.ctx)
			if err != nil {
				log.Debug("add msgstream failed", zap.String("name", name), zap.Error(err))
				continue
			}
			ms.AsProducer([]string{name})
			d.dml[name] = ms
		}
	}
}

// RemoveProducerChannels removes specified channels
func (d *dmlChannels) RemoveProducerChannels(names ...string) {
	d.lock.Lock()
	defer d.lock.Unlock()

	for _, name := range names {
		if ds, ok := d.dml[name]; ok {
			ds.Close()
			delete(d.dml, name)
		}
	}
}
