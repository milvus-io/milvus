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

type dmlMsgStream struct {
	ms     msgstream.MsgStream
	mutex  sync.RWMutex
	refcnt int64
}

type dmlChannels struct {
	core       *Core
	namePrefix string
	capacity   int64
	idx        *atomic.Int64
	pool       sync.Map
}

func newDmlChannels(c *Core, chanNamePrefix string, chanNum int64) *dmlChannels {
	d := &dmlChannels{
		core:       c,
		namePrefix: chanNamePrefix,
		capacity:   chanNum,
		idx:        atomic.NewInt64(0),
		pool:       sync.Map{},
	}

	for i := int64(0); i < chanNum; i++ {
		name := getDmlChannelName(d.namePrefix, i)
		ms, err := c.msFactory.NewMsgStream(c.ctx)
		if err != nil {
			log.Error("Failed to add msgstream", zap.String("name", name), zap.Error(err))
			panic("Failed to add msgstream")
		}
		d.pool.Store(name, &dmlMsgStream{
			ms:     ms,
			mutex:  sync.RWMutex{},
			refcnt: 0,
		})
	}
	log.Debug("init dml channels", zap.Int64("num", chanNum))
	return d
}

func (d *dmlChannels) GetDmlMsgStreamName() string {
	cnt := d.idx.Inc()
	return getDmlChannelName(d.namePrefix, (cnt-1)%d.capacity)
}

// ListPhysicalChannels lists all dml channel names
func (d *dmlChannels) ListPhysicalChannels() []string {
	var chanNames []string
	d.pool.Range(
		func(k, v interface{}) bool {
			dms := v.(*dmlMsgStream)
			dms.mutex.RLock()
			if dms.refcnt > 0 {
				chanNames = append(chanNames, k.(string))
			}
			dms.mutex.RUnlock()
			return true
		})
	return chanNames
}

// GetNumChannels get current dml channel count
func (d *dmlChannels) GetPhysicalChannelNum() int {
	return len(d.ListPhysicalChannels())
}

// Broadcast broadcasts msg pack into specified channel
func (d *dmlChannels) Broadcast(chanNames []string, pack *msgstream.MsgPack) error {
	for _, chanName := range chanNames {
		v, ok := d.pool.Load(chanName)
		if !ok {
			log.Error("invalid channel name", zap.String("chanName", chanName))
			panic("invalid channel name: " + chanName)
		}
		dms := v.(*dmlMsgStream)

		dms.mutex.RLock()
		if dms.refcnt > 0 {
			if err := dms.ms.Broadcast(pack); err != nil {
				log.Error("Broadcast failed", zap.String("chanName", chanName))
				return err
			}
		}
		dms.mutex.RUnlock()
	}
	return nil
}

// BroadcastMark broadcasts msg pack into specified channel and returns related message id
func (d *dmlChannels) BroadcastMark(chanNames []string, pack *msgstream.MsgPack) (map[string][]byte, error) {
	result := make(map[string][]byte)
	for _, chanName := range chanNames {
		v, ok := d.pool.Load(chanName)
		if !ok {
			log.Error("invalid channel name", zap.String("chanName", chanName))
			panic("invalid channel name: " + chanName)
		}
		dms := v.(*dmlMsgStream)

		dms.mutex.RLock()
		if dms.refcnt > 0 {
			ids, err := dms.ms.BroadcastMark(pack)
			if err != nil {
				log.Error("BroadcastMark failed", zap.String("chanName", chanName))
				return result, err
			}
			for cn, idList := range ids {
				// idList should have length 1, just flat by iteration
				for _, id := range idList {
					result[cn] = id.Serialize()
				}
			}
		}
		dms.mutex.RUnlock()
	}
	return result, nil
}

// AddProducerChannels add named channels as producer
func (d *dmlChannels) AddProducerChannels(names ...string) {
	for _, name := range names {
		v, ok := d.pool.Load(name)
		if !ok {
			log.Error("invalid channel name", zap.String("chanName", name))
			panic("invalid channel name: " + name)
		}
		dms := v.(*dmlMsgStream)

		dms.mutex.Lock()
		if dms.refcnt == 0 {
			dms.ms.AsProducer([]string{name})
		}
		dms.refcnt++
		dms.mutex.Unlock()
	}
}

// RemoveProducerChannels removes specified channels
func (d *dmlChannels) RemoveProducerChannels(names ...string) {
	for _, name := range names {
		v, ok := d.pool.Load(name)
		if !ok {
			log.Error("invalid channel name", zap.String("chanName", name))
			panic("invalid channel name: " + name)
		}
		dms := v.(*dmlMsgStream)

		dms.mutex.Lock()
		if dms.refcnt > 0 {
			dms.refcnt--
			if dms.refcnt == 0 {
				dms.ms.Close()
			}
		}
		dms.mutex.Unlock()
	}
}

func getDmlChannelName(prefix string, idx int64) string {
	return fmt.Sprintf("%s_%d", prefix, idx)
}
