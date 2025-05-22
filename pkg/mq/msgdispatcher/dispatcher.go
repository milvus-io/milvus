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
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
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

	id int64

	pullbackEndTs        typeutil.Timestamp
	pullbackDone         bool
	pullbackDoneNotifier *syncutil.AsyncTaskNotifier[struct{}]

	done chan struct{}
	wg   sync.WaitGroup
	once sync.Once

	pchannel string
	curTs    atomic.Uint64

	targets *typeutil.ConcurrentMap[string, *target]

	stream msgstream.MsgStream
}

func NewDispatcher(
	ctx context.Context,
	factory msgstream.Factory,
	id int64,
	pchannel string,
	position *Pos,
	subPos SubPos,
	includeCurrentMsg bool,
	pullbackEndTs typeutil.Timestamp,
) (*Dispatcher, error) {
	subName := fmt.Sprintf("%s-%d-%d", pchannel, id, time.Now().UnixNano())

	log := log.Ctx(ctx).With(zap.String("pchannel", pchannel),
		zap.Int64("id", id), zap.String("subName", subName))
	log.Info("creating dispatcher...", zap.Uint64("pullbackEndTs", pullbackEndTs))

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
		log.Info("as consumer done", zap.Any("position", position))
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
		id:                   id,
		pullbackEndTs:        pullbackEndTs,
		pullbackDoneNotifier: syncutil.NewAsyncTaskNotifier[struct{}](),
		done:                 make(chan struct{}, 1),
		pchannel:             pchannel,
		targets:              typeutil.NewConcurrentMap[string, *target](),
		stream:               stream,
	}

	metrics.NumConsumers.WithLabelValues(paramtable.GetRole(), fmt.Sprint(paramtable.GetNodeID())).Inc()
	return d, nil
}

func (d *Dispatcher) ID() int64 {
	return d.id
}

func (d *Dispatcher) CurTs() typeutil.Timestamp {
	return d.curTs.Load()
}

func (d *Dispatcher) AddTarget(t *target) {
	log := log.With(zap.String("vchannel", t.vchannel), zap.Int64("id", d.ID()), zap.Uint64("ts", t.pos.GetTimestamp()))
	if _, ok := d.targets.GetOrInsert(t.vchannel, t); ok {
		log.Warn("target exists")
		return
	}
	log.Info("add new target")
}

func (d *Dispatcher) GetTarget(vchannel string) (*target, error) {
	if t, ok := d.targets.Get(vchannel); ok {
		return t, nil
	}
	return nil, fmt.Errorf("cannot find target, vchannel=%s", vchannel)
}

func (d *Dispatcher) GetTargets() []*target {
	return d.targets.Values()
}

func (d *Dispatcher) HasTarget(vchannel string) bool {
	return d.targets.Contain(vchannel)
}

func (d *Dispatcher) RemoveTarget(vchannel string) {
	log := log.With(zap.String("vchannel", vchannel), zap.Int64("id", d.ID()))
	if _, ok := d.targets.GetAndRemove(vchannel); ok {
		log.Info("target removed")
	} else {
		log.Warn("target not exist")
	}
}

func (d *Dispatcher) TargetNum() int {
	return d.targets.Len()
}

func (d *Dispatcher) BlockUtilPullbackDone() {
	select {
	case <-d.ctx.Done():
	case <-d.pullbackDoneNotifier.FinishChan():
	}
}

func (d *Dispatcher) Handle(signal signal) {
	log := log.With(zap.String("pchannel", d.pchannel), zap.Int64("id", d.ID()),
		zap.String("signal", signal.String()))
	log.Debug("get signal")
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
	log := log.With(zap.String("pchannel", d.pchannel), zap.Int64("id", d.ID()))
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

			targetPacks := d.groupAndParseMsgs(pack, d.stream.GetUnmarshalDispatcher())
			for vchannel, p := range targetPacks {
				var err error
				t, _ := d.targets.Get(vchannel)
				isReplicateChannel := strings.Contains(vchannel, paramtable.Get().CommonCfg.ReplicateMsgChannel.GetValue())
				// The dispatcher seeks from the oldest target,
				// so for each target, msg before the target position must be filtered out.
				if p.EndTs <= t.pos.GetTimestamp() && !isReplicateChannel {
					log.Info("skip msg",
						zap.String("vchannel", vchannel),
						zap.Int("msgCount", len(p.Msgs)),
						zap.Uint64("packBeginTs", p.BeginTs),
						zap.Uint64("packEndTs", p.EndTs),
						zap.Uint64("posTs", t.pos.GetTimestamp()),
					)
					for _, msg := range p.Msgs {
						log.Debug("skip msg info",
							zap.String("vchannel", vchannel),
							zap.String("msgType", msg.Type().String()),
							zap.Int64("msgID", msg.ID()),
							zap.Uint64("msgBeginTs", msg.BeginTs()),
							zap.Uint64("msgEndTs", msg.EndTs()),
							zap.Uint64("packBeginTs", p.BeginTs),
							zap.Uint64("packEndTs", p.EndTs),
							zap.Uint64("posTs", t.pos.GetTimestamp()),
						)
					}
					continue
				}
				if d.targets.Len() > 1 {
					// for dispatcher with multiple targets, split target if err occurs
					err = t.send(p)
				} else {
					// for dispatcher with only one target,
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
					d.targets.GetAndRemove(vchannel)
					log.Warn("lag target", zap.Error(err))
				}
			}

			if !d.pullbackDone && pack.EndPositions[0].GetTimestamp() >= d.pullbackEndTs {
				d.pullbackDoneNotifier.Finish(struct{}{})
				log.Info("dispatcher pullback done",
					zap.Uint64("pullbackEndTs", d.pullbackEndTs),
					zap.Time("pullbackTime", tsoutil.PhysicalTime(d.pullbackEndTs)),
				)
				d.pullbackDone = true
			}
		}
	}
}

func (d *Dispatcher) groupAndParseMsgs(pack *msgstream.ConsumeMsgPack, unmarshalDispatcher msgstream.UnmarshalDispatcher) map[string]*MsgPack {
	// init packs for all targets, even though there's no msg in pack,
	// but we still need to dispatch time ticks to the targets.
	targetPacks := make(map[string]*MsgPack)
	replicateConfigs := make(map[string]*msgstream.ReplicateConfig)
	d.targets.Range(func(vchannel string, t *target) bool {
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
		return true
	})
	// group messages by vchannel
	for _, msg := range pack.Msgs {
		var vchannel, collectionID string

		if msg.GetType() == commonpb.MsgType_Insert || msg.GetType() == commonpb.MsgType_Delete {
			vchannel = msg.GetVChannel()
		} else if msg.GetType() == commonpb.MsgType_CreateCollection ||
			msg.GetType() == commonpb.MsgType_DropCollection ||
			msg.GetType() == commonpb.MsgType_CreatePartition ||
			msg.GetType() == commonpb.MsgType_DropPartition {
			collectionID = msg.GetCollectionID()
		}

		if vchannel == "" {
			// we need to dispatch it to the vchannel of this collection
			targets := []string{}
			for k := range targetPacks {
				if msg.GetType() == commonpb.MsgType_Replicate {
					config := replicateConfigs[k]
					if config != nil && msg.GetReplicateID() == config.ReplicateID {
						targets = append(targets, k)
					}
					continue
				}

				if !strings.Contains(k, collectionID) {
					continue
				}
				targets = append(targets, k)
			}
			if len(targets) > 0 {
				tsMsg, err := msg.Unmarshal(unmarshalDispatcher)
				if err != nil {
					log.Warn("unmarshl message failed", zap.Error(err))
					continue
				}
				// TODO: There's data race when non-dml msg is sent to different flow graph.
				// Wrong open-trancing information is generated, Fix in future.
				for _, target := range targets {
					targetPacks[target].Msgs = append(targetPacks[target].Msgs, tsMsg)
				}
			}
			continue
		}
		if _, ok := targetPacks[vchannel]; ok {
			tsMsg, err := msg.Unmarshal(unmarshalDispatcher)
			if err != nil {
				log.Warn("unmarshl message failed", zap.Error(err))
				continue
			}
			targetPacks[vchannel].Msgs = append(targetPacks[vchannel].Msgs, tsMsg)
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
		if t, ok := d.targets.Get(vchannel); ok {
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
