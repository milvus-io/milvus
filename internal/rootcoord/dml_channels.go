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

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
)

type dmlChannels struct {
	core       *Core
	namePrefix string
	capacity   int64
	refcnt     sync.Map
	idx        *atomic.Int64
	pool       sync.Map
}

func newDmlChannels(c *Core, chanNamePrefix string, chanNum int64) *dmlChannels {
	d := &dmlChannels{
		core:       c,
		namePrefix: chanNamePrefix,
		capacity:   chanNum,
		refcnt:     sync.Map{},
		idx:        atomic.NewInt64(0),
		pool:       sync.Map{},
	}

	var i int64
	for i = 0; i < chanNum; i++ {
		name := fmt.Sprintf("%s_%d", d.namePrefix, i)
		ms, err := c.msFactory.NewMsgStream(c.ctx)
		if err != nil {
			log.Error("add msgstream failed", zap.String("name", name), zap.Error(err))
			panic("add msgstream failed")
		}
		ms.AsProducer([]string{name})
		d.pool.Store(name, &ms)
	}
	log.Debug("init dml channels", zap.Int64("num", chanNum))
	return d
}

func (d *dmlChannels) GetDmlMsgStreamName() string {
	cnt := d.idx.Load()
	name := fmt.Sprintf("%s_%d", d.namePrefix, cnt)
	d.idx.Store((cnt + 1) % d.capacity)
	return name
}

// ListChannels lists all dml channel names
func (d *dmlChannels) ListChannels() []string {
	chanNames := make([]string, 0)
	d.refcnt.Range(
		func(k, v interface{}) bool {
			chanNames = append(chanNames, k.(string))
			return true
		})
	return chanNames
}

// GetNumChannels get current dml channel count
func (d *dmlChannels) GetNumChannels() int {
	return len(d.ListChannels())
}

// Broadcast broadcasts msg pack into specified channel
func (d *dmlChannels) Broadcast(chanNames []string, pack *msgstream.MsgPack) error {
	for _, chanName := range chanNames {
		// only in-use chanName exist in refcnt
		if _, ok := d.refcnt.Load(chanName); ok {
			v, _ := d.pool.Load(chanName)
			if err := (*(v.(*msgstream.MsgStream))).Broadcast(pack); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("channel %s not exist", chanName)
		}
	}
	return nil
}

// BroadcastMark broadcasts msg pack into specified channel and returns related message id
func (d *dmlChannels) BroadcastMark(chanNames []string, pack *msgstream.MsgPack) (map[string][]byte, error) {
	result := make(map[string][]byte)
	for _, chanName := range chanNames {
		// only in-use chanName exist in refcnt
		if _, ok := d.refcnt.Load(chanName); ok {
			v, _ := d.pool.Load(chanName)
			ids, err := (*(v.(*msgstream.MsgStream))).BroadcastMark(pack)
			if err != nil {
				return result, err
			}
			for chanName, idList := range ids {
				// idList should have length 1, just flat by iteration
				for _, id := range idList {
					result[chanName] = id.Serialize()
				}
			}
		} else {
			return result, fmt.Errorf("channel %s not exist", chanName)
		}
	}
	return result, nil
}

// AddProducerChannels add named channels as producer
func (d *dmlChannels) AddProducerChannels(names ...string) {
	for _, name := range names {
		if _, ok := d.pool.Load(name); ok {
			var cnt int64
			if _, ok := d.refcnt.Load(name); !ok {
				cnt = 1
			} else {
				v, _ := d.refcnt.Load(name)
				cnt = v.(int64) + 1
			}
			d.refcnt.Store(name, cnt)
			log.Debug("assign dml channel", zap.String("chanName", name), zap.Int64("refcnt", cnt))
		} else {
			log.Error("invalid channel name", zap.String("chanName", name))
			panic("invalid channel name: " + name)
		}
	}
}

// RemoveProducerChannels removes specified channels
func (d *dmlChannels) RemoveProducerChannels(names ...string) {
	for _, name := range names {
		if v, ok := d.refcnt.Load(name); ok {
			cnt := v.(int64)
			if cnt > 1 {
				d.refcnt.Store(name, cnt-1)
			} else {
				d.refcnt.Delete(name)
			}
		}
	}
}
