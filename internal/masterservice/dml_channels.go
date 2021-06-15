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

type dmlStream struct {
	msgStream msgstream.MsgStream
	valid     bool
}

type dmlChannels struct {
	core *Core
	lock sync.RWMutex
	dml  map[string]*dmlStream
}

func newDMLChannels(c *Core) *dmlChannels {
	return &dmlChannels{
		core: c,
		lock: sync.RWMutex{},
		dml:  make(map[string]*dmlStream),
	}
}

func (d *dmlChannels) GetNumChannles() int {
	d.lock.RLock()
	defer d.lock.RUnlock()
	count := 0
	for _, ds := range d.dml {
		if ds.valid {
			count++
		}
	}
	return count
}

func (d *dmlChannels) Produce(name string, pack *msgstream.MsgPack) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	ds, ok := d.dml[name]
	if !ok {
		return fmt.Errorf("channel %s not exist", name)
	}
	if err := ds.msgStream.Produce(pack); err != nil {
		return err
	}
	if !ds.valid {
		ds.msgStream.Close()
		delete(d.dml, name)
	}
	return nil
}

func (d *dmlChannels) Broadcast(name string, pack *msgstream.MsgPack) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	ds, ok := d.dml[name]
	if !ok {
		return fmt.Errorf("channel %s not exist", name)
	}
	if err := ds.msgStream.Broadcast(pack); err != nil {
		return err
	}
	if !ds.valid {
		ds.msgStream.Close()
		delete(d.dml, name)
	}
	return nil
}

func (d *dmlChannels) BroadcastAll(channels []string, pack *msgstream.MsgPack) error {
	d.lock.Lock()
	defer d.lock.Unlock()

	for _, ch := range channels {
		ds, ok := d.dml[ch]
		if !ok {
			return fmt.Errorf("channel %s not exist", ch)
		}
		if err := ds.msgStream.Broadcast(pack); err != nil {
			return err
		}
		if !ds.valid {
			ds.msgStream.Close()
			delete(d.dml, ch)
		}
	}
	return nil
}

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
			d.dml[name] = &dmlStream{
				msgStream: ms,
				valid:     true,
			}
		}
	}
}

func (d *dmlChannels) RemoveProducerChannels(names ...string) {
	d.lock.Lock()
	defer d.lock.Unlock()

	for _, name := range names {
		log.Debug("delete dml channel", zap.String("channel name", name))
		if ds, ok := d.dml[name]; ok {
			ds.valid = false
		}
	}
}
