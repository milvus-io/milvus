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

package rootcoord

import (
	"context"
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/metrics"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
)

type dmlMsgStream struct {
	ms     msgstream.MsgStream
	mutex  sync.RWMutex
	refcnt int64
}

type dmlChannels struct {
	ctx        context.Context
	factory    msgstream.Factory
	namePrefix string
	capacity   int64
	idx        *atomic.Int64
	pool       sync.Map
}

func newDmlChannels(ctx context.Context, factory msgstream.Factory, chanNamePrefix string, chanNum int64) *dmlChannels {
	d := &dmlChannels{
		ctx:        ctx,
		factory:    factory,
		namePrefix: chanNamePrefix,
		capacity:   chanNum,
		idx:        atomic.NewInt64(0),
		pool:       sync.Map{},
	}

	for i := int64(0); i < chanNum; i++ {
		name := genChannelName(d.namePrefix, i)
		ms, err := factory.NewMsgStream(ctx)
		if err != nil {
			log.Error("Failed to create msgstream", zap.String("channel name", name), zap.Error(err))
			panic("Failed to add msgstream")
		}
		d.pool.Store(name, &dmlMsgStream{
			ms:     ms,
			mutex:  sync.RWMutex{},
			refcnt: 0,
		})
	}
	log.Debug("init dml channels finished", zap.Int64("channel num", chanNum))
	metrics.RootCoordNumOfDMLChannel.Add(float64(chanNum))
	return d
}

func (d *dmlChannels) getChannelName() string {
	cnt := d.idx.Inc()
	return genChannelName(d.namePrefix, (cnt-1)%d.capacity)
}

func (d *dmlChannels) listChannels() []string {
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

func (d *dmlChannels) getChannelNum() int {
	return len(d.listChannels())
}

func (d *dmlChannels) broadcast(chanNames []string, pack *msgstream.MsgPack) error {
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
				dms.mutex.RUnlock()
				return err
			}
		}
		dms.mutex.RUnlock()
	}
	return nil
}

func (d *dmlChannels) broadcastMark(chanNames []string, pack *msgstream.MsgPack) (map[string][]byte, error) {
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
				dms.mutex.RUnlock()
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

func (d *dmlChannels) addChannels(names ...string) {
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
	metrics.RootCoordNumOfDMLChannel.Inc()
}

func (d *dmlChannels) removeChannels(names ...string) {
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
	metrics.RootCoordNumOfDMLChannel.Dec()
}

func genChannelName(prefix string, idx int64) string {
	return fmt.Sprintf("%s_%d", prefix, idx)
}
