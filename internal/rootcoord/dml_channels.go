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
	"container/heap"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/metrics"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type dmlMsgStream struct {
	ms    msgstream.MsgStream
	mutex sync.RWMutex

	refcnt int64 // current in use count
	used   int64 // total used counter in current run, not stored in meta so meant to be inaccurate
	idx    int64 // idx for name
	pos    int   // position in the heap slice
}

// RefCnt returns refcnt with mutex protection.
func (dms *dmlMsgStream) RefCnt() int64 {
	dms.mutex.RLock()
	defer dms.mutex.RUnlock()
	return dms.refcnt
}

// RefCnt returns refcnt with mutex protection.
func (dms *dmlMsgStream) Used() int64 {
	dms.mutex.RLock()
	defer dms.mutex.RUnlock()
	return dms.used
}

// IncRefcnt increases refcnt.
func (dms *dmlMsgStream) IncRefcnt() {
	dms.mutex.Lock()
	defer dms.mutex.Unlock()
	dms.refcnt++
}

// BookUsage increases used, acting like reservation usage.
func (dms *dmlMsgStream) BookUsage() {
	dms.mutex.Lock()
	defer dms.mutex.Unlock()
	dms.used++
}

// DecRefCnt decreases refcnt only.
func (dms *dmlMsgStream) DecRefCnt() {
	dms.mutex.Lock()
	defer dms.mutex.Unlock()
	if dms.refcnt > 0 {
		dms.refcnt--
	} else {
		log.Warn("Try to remove channel with no ref count", zap.Int64("idx", dms.idx))
	}
}

// channelsHeap implements heap.Interface to performs like an priority queue.
type channelsHeap []*dmlMsgStream

// Len is the number of elements in the collection.
func (h channelsHeap) Len() int {
	return len(h)
}

// Less reports whether the element with index i
// must sort before the element with index j.
func (h channelsHeap) Less(i int, j int) bool {
	ei, ej := h[i], h[j]
	// use less refcnt first
	rci, rcj := ei.RefCnt(), ej.RefCnt()
	if rci != rcj {
		return rci < rcj
	}

	// used not used channel first
	ui, uj := ei.Used(), ej.Used()
	if ui != uj {
		return ui < uj
	}

	// all number same, used alphabetic smaller one
	return ei.idx < ej.idx
}

// Swap swaps the elements with indexes i and j.
func (h channelsHeap) Swap(i int, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].pos, h[j].pos = i, j
}

// Push adds a new element to the heap.
func (h *channelsHeap) Push(x interface{}) {
	item := x.(*dmlMsgStream)
	*h = append(*h, item)
}

// Pop implements heap.Interface, pop the last value.
func (h *channelsHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[0 : n-1]
	return item
}

type dmlChannels struct {
	ctx        context.Context
	factory    msgstream.Factory
	namePrefix string
	capacity   int64
	// pool maintains channelName => dmlMsgStream mapping, stable
	pool sync.Map
	// mut protects channelsHeap only
	mut sync.Mutex
	// channelsHeap is the heap to pop next dms for use
	channelsHeap channelsHeap
}

func newDmlChannels(ctx context.Context, factory msgstream.Factory, chanNamePrefixDefault string, chanNumDefault int64) *dmlChannels {
	params := paramtable.Get().CommonCfg
	var (
		chanNamePrefix string
		chanNum        int64
		names          []string
	)

	// if topic created, use the existed topic
	if params.PreCreatedTopicEnabled.GetAsBool() {
		chanNamePrefix = ""
		chanNum = int64(len(params.TopicNames.GetAsStrings()))
		names = params.TopicNames.GetAsStrings()
	} else {
		chanNamePrefix = chanNamePrefixDefault
		chanNum = chanNumDefault
		names = genChannelNames(chanNamePrefix, chanNum)
	}

	d := &dmlChannels{
		ctx:          ctx,
		factory:      factory,
		namePrefix:   chanNamePrefix,
		capacity:     chanNum,
		channelsHeap: make([]*dmlMsgStream, 0, chanNum),
	}

	for i, name := range names {
		ms, err := factory.NewMsgStream(ctx)
		if err != nil {
			log.Error("Failed to add msgstream",
				zap.String("name", name),
				zap.Error(err))
			panic("Failed to add msgstream")
		}

		if params.PreCreatedTopicEnabled.GetAsBool() {
			subName := fmt.Sprintf("pre-created-topic-check-%s", name)
			ms.AsConsumer([]string{name}, subName, mqwrapper.SubscriptionPositionUnknown)
			// check topic exist and check the existed topic whether empty or not
			// kafka and rmq will err if the topic does not yet exist, pulsar will not
			// if one of the topics is not empty, panic
			err := ms.CheckTopicValid(name)
			if err != nil {
				log.Error("created topic is invaild", zap.String("name", name), zap.Error(err))
				panic("created topic is invaild")
			}
		}

		ms.AsProducer([]string{name})
		dms := &dmlMsgStream{
			ms:     ms,
			refcnt: 0,
			used:   0,
			idx:    int64(i),
			pos:    i,
		}
		d.pool.Store(name, dms)
		d.channelsHeap = append(d.channelsHeap, dms)
	}

	heap.Init(&d.channelsHeap)

	log.Info("init dml channels", zap.String("prefix", chanNamePrefix), zap.Int64("num", chanNum))

	metrics.RootCoordNumOfDMLChannel.Add(float64(chanNum))
	metrics.RootCoordNumOfMsgStream.Add(float64(chanNum))

	return d
}

func (d *dmlChannels) getChannelNames(count int) []string {
	d.mut.Lock()
	defer d.mut.Unlock()
	if count > len(d.channelsHeap) {
		return nil
	}
	// get next count items from heap
	items := make([]*dmlMsgStream, 0, count)
	result := make([]string, 0, count)
	for i := 0; i < count; i++ {
		item := heap.Pop(&d.channelsHeap).(*dmlMsgStream)
		item.BookUsage()
		items = append(items, item)
		result = append(result, getChannelName(d.namePrefix, item.idx))
	}

	for _, item := range items {
		heap.Push(&d.channelsHeap, item)
	}

	return result
}

func (d *dmlChannels) listChannels() []string {
	var chanNames []string

	d.pool.Range(
		func(k, v interface{}) bool {
			dms := v.(*dmlMsgStream)
			if dms.RefCnt() > 0 {
				chanNames = append(chanNames, getChannelName(d.namePrefix, dms.idx))
			}
			return true
		})
	return chanNames
}

func (d *dmlChannels) getChannelNum() int {
	return len(d.listChannels())
}

func (d *dmlChannels) getMsgStreamByName(chanName string) (*dmlMsgStream, error) {
	v, ok := d.pool.Load(chanName)
	if !ok {
		log.Error("invalid channelName", zap.String("chanName", chanName))
		return nil, errors.Newf("invalid channel name: %s", chanName)
	}
	return v.(*dmlMsgStream), nil
}

func (d *dmlChannels) broadcast(chanNames []string, pack *msgstream.MsgPack) error {
	for _, chanName := range chanNames {
		dms, err := d.getMsgStreamByName(chanName)
		if err != nil {
			return err
		}

		dms.mutex.RLock()
		if dms.refcnt > 0 {
			if _, err := dms.ms.Broadcast(pack); err != nil {
				log.Error("Broadcast failed", zap.Error(err), zap.String("chanName", chanName))
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
		dms, err := d.getMsgStreamByName(chanName)
		if err != nil {
			return result, err
		}

		dms.mutex.RLock()
		if dms.refcnt > 0 {
			ids, err := dms.ms.Broadcast(pack)
			if err != nil {
				log.Error("BroadcastMark failed", zap.Error(err), zap.String("chanName", chanName))
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
		dms, err := d.getMsgStreamByName(name)
		if err != nil {
			continue
		}

		d.mut.Lock()
		dms.IncRefcnt()
		heap.Fix(&d.channelsHeap, dms.pos)
		d.mut.Unlock()
	}
}

func (d *dmlChannels) removeChannels(names ...string) {
	for _, name := range names {
		dms, err := d.getMsgStreamByName(name)
		if err != nil {
			continue
		}

		d.mut.Lock()
		dms.DecRefCnt()
		heap.Fix(&d.channelsHeap, dms.pos)
		d.mut.Unlock()
	}
}

func getChannelName(prefix string, idx int64) string {
	params := paramtable.Get().CommonCfg
	if params.PreCreatedTopicEnabled.GetAsBool() {
		return params.TopicNames.GetAsStrings()[idx]
	}
	return fmt.Sprintf("%s_%d", prefix, idx)
}

func genChannelNames(prefix string, num int64) []string {
	var results []string
	for idx := int64(0); idx < num; idx++ {
		result := fmt.Sprintf("%s_%d", prefix, idx)
		results = append(results, result)
	}
	return results
}

func parseChannelNameIndex(channelName string) int {
	index := strings.LastIndex(channelName, "_")
	if index < 0 {
		log.Error("invalid channelName", zap.String("chanName", channelName))
		panic("invalid channel name: " + channelName)
	}
	index, err := strconv.Atoi(channelName[index+1:])
	if err != nil {
		log.Error("invalid channelName", zap.String("chanName", channelName), zap.Error(err))
		panic("invalid channel name: " + channelName)
	}
	return index
}

func getNeedChanNum(setNum int, chanMap map[typeutil.UniqueID][]string) int {
	// find the largest number of current channel usage
	maxChanUsed := 0
	isPreCreatedTopicEnabled := paramtable.Get().CommonCfg.PreCreatedTopicEnabled.GetAsBool()
	chanNameSet := typeutil.NewSet[string]()

	if isPreCreatedTopicEnabled {
		// can only use the topic in the list when preCreatedTopicEnabled
		topics := paramtable.Get().CommonCfg.TopicNames.GetAsStrings()

		if len(topics) == 0 {
			panic("no topic were specified when pre-created")
		}
		for _, topic := range topics {
			if len(topic) == 0 {
				panic("topic were empty")
			}
			if chanNameSet.Contain(topic) {
				log.Error("duplicate topics are pre-created", zap.String("topic", topic))
				panic("duplicate topic: " + topic)
			}
			chanNameSet.Insert(topic)
		}

		for _, chanNames := range chanMap {
			for _, chanName := range chanNames {
				if !chanNameSet.Contain(chanName) {
					log.Error("invalid channel that is not in the list when pre-created topic", zap.String("chanName", chanName))
					panic("invalid chanName: " + chanName)
				}
			}
		}
	} else {
		maxChanUsed = setNum
		for _, chanNames := range chanMap {
			for _, chanName := range chanNames {
				index := parseChannelNameIndex(chanName)
				if maxChanUsed < index+1 {
					maxChanUsed = index + 1
				}
			}
		}
	}
	return maxChanUsed
}
