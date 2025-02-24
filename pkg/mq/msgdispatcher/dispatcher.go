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

package msgdispatcher

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/mq/common"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type signal int32

const (
	start     signal = 0
	pause     signal = 1
	resume    signal = 2
	terminate signal = 3
)

var signalString = map[int32]string{
	0: "start",
	1: "pause",
	2: "resume",
	3: "terminate",
}

func (s signal) String() string {
	return signalString[int32(s)]
}

type Dispatcher struct {
	ctx    context.Context
	cancel context.CancelFunc

	done chan struct{}
	wg   sync.WaitGroup
	once sync.Once

	isMain   bool // indicates if it's a main dispatcher
	pchannel string
	curTs    atomic.Uint64

	lagNotifyChan chan struct{}
	lagTargets    *typeutil.ConcurrentMap[string, *target] // vchannel -> *target

	// vchannel -> *target, lock free since we guarantee that
	// it's modified only after dispatcher paused or terminated
	targets map[string]*target

	stream msgstream.MsgStream
}

func NewDispatcher(
	ctx context.Context,
	factory msgstream.Factory,
	isMain bool,
	pchannel string,
	position *Pos,
	subName string,
	subPos SubPos,
	lagNotifyChan chan struct{},
	lagTargets *typeutil.ConcurrentMap[string, *target],
	includeCurrentMsg bool,
) (*Dispatcher, error) {
	log := log.With(zap.String("pchannel", pchannel),
		zap.String("subName", subName), zap.Bool("isMain", isMain))
	log.Info("creating dispatcher...")

	var stream msgstream.MsgStream
	var err error
	defer func() {
		if err != nil && stream != nil {
			stream.Close()
		}
	}()

	stream, err = factory.NewTtMsgStream(ctx)
	if err != nil {
		return nil, err
	}
	if position != nil && len(position.MsgID) != 0 {
		position = typeutil.Clone(position)
		position.ChannelName = funcutil.ToPhysicalChannel(position.ChannelName)
		err = stream.AsConsumer(ctx, []string{pchannel}, subName, common.SubscriptionPositionUnknown)
		if err != nil {
			log.Error("asConsumer failed", zap.Error(err))
			return nil, err
		}

		err = stream.Seek(ctx, []*Pos{position}, includeCurrentMsg)
		if err != nil {
			log.Error("seek failed", zap.Error(err))
			return nil, err
		}
		posTime := tsoutil.PhysicalTime(position.GetTimestamp())
		log.Info("seek successfully", zap.Uint64("posTs", position.GetTimestamp()),
			zap.Time("posTime", posTime), zap.Duration("tsLag", time.Since(posTime)))
	} else {
		err = stream.AsConsumer(ctx, []string{pchannel}, subName, subPos)
		if err != nil {
			log.Error("asConsumer failed", zap.Error(err))
			return nil, err
		}
		log.Info("asConsumer successfully")
	}

	d := &Dispatcher{
		done:          make(chan struct{}, 1),
		isMain:        isMain,
		pchannel:      pchannel,
		lagNotifyChan: lagNotifyChan,
		lagTargets:    lagTargets,
		targets:       make(map[string]*target),
		stream:        stream,
	}

	metrics.NumConsumers.WithLabelValues(paramtable.GetRole(), fmt.Sprint(paramtable.GetNodeID())).Inc()
	return d, nil
}

func (d *Dispatcher) CurTs() typeutil.Timestamp {
	return d.curTs.Load()
}

func (d *Dispatcher) AddTarget(t *target) {
	log := log.With(zap.String("vchannel", t.vchannel), zap.Bool("isMain", d.isMain))
	if _, ok := d.targets[t.vchannel]; ok {
		log.Warn("target exists")
		return
	}
	d.targets[t.vchannel] = t
	log.Info("add new target")
}

func (d *Dispatcher) GetTarget(vchannel string) (*target, error) {
	if t, ok := d.targets[vchannel]; ok {
		return t, nil
	}
	return nil, fmt.Errorf("cannot find target, vchannel=%s, isMain=%t", vchannel, d.isMain)
}

func (d *Dispatcher) CloseTarget(vchannel string) {
	log := log.With(zap.String("vchannel", vchannel), zap.Bool("isMain", d.isMain))
	if t, ok := d.targets[vchannel]; ok {
		t.close()
		delete(d.targets, vchannel)
		log.Info("closed target")
	} else {
		log.Warn("target not exist")
	}
}

func (d *Dispatcher) TargetNum() int {
	return len(d.targets)
}

func (d *Dispatcher) Handle(signal signal) {
	log := log.With(zap.String("pchannel", d.pchannel),
		zap.String("signal", signal.String()), zap.Bool("isMain", d.isMain))
	log.Info("get signal")
	switch signal {
	case start:
		d.ctx, d.cancel = context.WithCancel(context.Background())
		d.wg.Add(1)
		go d.work()
	case pause:
		d.done <- struct{}{}
		d.cancel()
		d.wg.Wait()
	case resume:
		d.ctx, d.cancel = context.WithCancel(context.Background())
		d.wg.Add(1)
		go d.work()
	case terminate:
		d.done <- struct{}{}
		d.cancel()
		d.wg.Wait()
		d.once.Do(func() {
			metrics.NumConsumers.WithLabelValues(paramtable.GetRole(), fmt.Sprint(paramtable.GetNodeID())).Dec()
			d.stream.Close()
		})
	}
	log.Info("handle signal done")
}

func (d *Dispatcher) work() {
	log := log.With(zap.String("pchannel", d.pchannel), zap.Bool("isMain", d.isMain))
	log.Info("begin to work")
	defer d.wg.Done()
	for {
		select {
		case <-d.done:
			log.Info("stop working")
			return
		case pack := <-d.stream.Chan():
			if pack == nil || len(pack.EndPositions) != 1 {
				log.Error("consumed invalid msgPack")
				continue
			}
			d.curTs.Store(pack.EndPositions[0].GetTimestamp())

			targetPacks := d.groupingMsgs(pack)
			for vchannel, p := range targetPacks {
				var err error
				t := d.targets[vchannel]
				if d.isMain {
					// for main dispatcher, split target if err occurs
					err = t.send(p)
				} else {
					// for solo dispatcher, only 1 target exists, we should
					// keep retrying if err occurs, unless it paused or terminated.
					for {
						err = t.send(p)
						if err == nil || !funcutil.CheckCtxValid(d.ctx) {
							break
						}
					}
				}
				if err != nil {
					t.pos = typeutil.Clone(pack.StartPositions[0])
					// replace the pChannel with vChannel
					t.pos.ChannelName = t.vchannel
					d.lagTargets.Insert(t.vchannel, t)
					d.nonBlockingNotify()
					delete(d.targets, vchannel)
					log.Warn("lag target notified", zap.Error(err))
				}
			}
		}
	}
}

func (d *Dispatcher) groupingMsgs(pack *MsgPack) map[string]*MsgPack {
	// init packs for all targets, even though there's no msg in pack,
	// but we still need to dispatch time ticks to the targets.
	targetPacks := make(map[string]*MsgPack)
	replicateConfigs := make(map[string]*msgstream.ReplicateConfig)
	for vchannel, t := range d.targets {
		targetPacks[vchannel] = &MsgPack{
			BeginTs:        pack.BeginTs,
			EndTs:          pack.EndTs,
			Msgs:           make([]msgstream.TsMsg, 0),
			StartPositions: pack.StartPositions,
			EndPositions:   pack.EndPositions,
		}
		if t.replicateConfig != nil {
			replicateConfigs[vchannel] = t.replicateConfig
		}
	}
	// group messages by vchannel
	for _, msg := range pack.Msgs {
		var vchannel, collectionID string
		switch msg.Type() {
		case commonpb.MsgType_Insert:
			vchannel = msg.(*msgstream.InsertMsg).GetShardName()
		case commonpb.MsgType_Delete:
			vchannel = msg.(*msgstream.DeleteMsg).GetShardName()
		case commonpb.MsgType_CreateCollection:
			collectionID = strconv.FormatInt(msg.(*msgstream.CreateCollectionMsg).GetCollectionID(), 10)
		case commonpb.MsgType_DropCollection:
			collectionID = strconv.FormatInt(msg.(*msgstream.DropCollectionMsg).GetCollectionID(), 10)
		case commonpb.MsgType_CreatePartition:
			collectionID = strconv.FormatInt(msg.(*msgstream.CreatePartitionMsg).GetCollectionID(), 10)
		case commonpb.MsgType_DropPartition:
			collectionID = strconv.FormatInt(msg.(*msgstream.DropPartitionMsg).GetCollectionID(), 10)
		}
		if vchannel == "" {
			// we need to dispatch it to the vchannel of this collection
			for k := range targetPacks {
				if msg.Type() == commonpb.MsgType_Replicate {
					config := replicateConfigs[k]
					if config != nil && msgstream.MatchReplicateID(msg, config.ReplicateID) {
						targetPacks[k].Msgs = append(targetPacks[k].Msgs, msg)
					}
					continue
				}

				if !strings.Contains(k, collectionID) {
					continue
				}
				// TODO: There's data race when non-dml msg is sent to different flow graph.
				// Wrong open-trancing information is generated, Fix in future.
				targetPacks[k].Msgs = append(targetPacks[k].Msgs, msg)
			}
			continue
		}
		if _, ok := targetPacks[vchannel]; ok {
			targetPacks[vchannel].Msgs = append(targetPacks[vchannel].Msgs, msg)
		}
	}
	replicateEndChannels := make(map[string]struct{})
	for vchannel, c := range replicateConfigs {
		if len(targetPacks[vchannel].Msgs) == 0 {
			delete(targetPacks, vchannel) // no replicate msg, can't send pack
			continue
		}
		// calculate the new pack ts
		beginTs := targetPacks[vchannel].Msgs[0].BeginTs()
		endTs := targetPacks[vchannel].Msgs[0].EndTs()
		newMsgs := make([]msgstream.TsMsg, 0)
		for _, msg := range targetPacks[vchannel].Msgs {
			if msg.BeginTs() < beginTs {
				beginTs = msg.BeginTs()
			}
			if msg.EndTs() > endTs {
				endTs = msg.EndTs()
			}
			if msg.Type() == commonpb.MsgType_Replicate {
				replicateMsg := msg.(*msgstream.ReplicateMsg)
				if c.CheckFunc(replicateMsg) {
					replicateEndChannels[vchannel] = struct{}{}
				}
				continue
			}
			newMsgs = append(newMsgs, msg)
		}
		targetPacks[vchannel].Msgs = newMsgs
		d.resetMsgPackTS(targetPacks[vchannel], beginTs, endTs)
	}
	for vchannel := range replicateEndChannels {
		if t, ok := d.targets[vchannel]; ok {
			t.replicateConfig = nil
			log.Info("replicate end, set replicate config nil", zap.String("vchannel", vchannel))
		}
	}
	return targetPacks
}

func (d *Dispatcher) resetMsgPackTS(pack *MsgPack, newBeginTs, newEndTs typeutil.Timestamp) {
	pack.BeginTs = newBeginTs
	pack.EndTs = newEndTs
	startPositions := make([]*msgstream.MsgPosition, 0)
	endPositions := make([]*msgstream.MsgPosition, 0)
	for _, pos := range pack.StartPositions {
		startPosition := typeutil.Clone(pos)
		startPosition.Timestamp = newBeginTs
		startPositions = append(startPositions, startPosition)
	}
	for _, pos := range pack.EndPositions {
		endPosition := typeutil.Clone(pos)
		endPosition.Timestamp = newEndTs
		endPositions = append(endPositions, endPosition)
	}
	pack.StartPositions = startPositions
	pack.EndPositions = endPositions
}

func (d *Dispatcher) nonBlockingNotify() {
	select {
	case d.lagNotifyChan <- struct{}{}:
	default:
	}
}
