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

package pipeline

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

var (
	thresholdUpdateIntervalMs        = atomic.NewInt64(60 * 1000)
	thresholdUpdateIntervalWatchOnce = &sync.Once{}
)

type LastestMVCCTimeTickGetter interface {
	GetLatestRequiredMVCCTimeTick() uint64
}

// newEmptyTimeTickSlowdowner creates a new consumingSlowdowner instance.
func newEmptyTimeTickSlowdowner(lastestMVCCTimeTickGetter LastestMVCCTimeTickGetter, vChannel string) *emptyTimeTickSlowdowner {
	thresholdUpdateIntervalWatchOnce.Do(updateThresholdWithConfiguration)

	nodeID := paramtable.GetStringNodeID()
	pchannel := funcutil.ToPhysicalChannel(vChannel)
	emptyTimeTickFilteredCounter := metrics.WALDelegatorEmptyTimeTickFilteredTotal.WithLabelValues(nodeID, pchannel)
	tsafeTimeTickUnfilteredCounter := metrics.WALDelegatorTsafeTimeTickUnfilteredTotal.WithLabelValues(nodeID, pchannel)
	return &emptyTimeTickSlowdowner{
		lastestMVCCTimeTickGetter: lastestMVCCTimeTickGetter,

		lastestMVCCTimeTick:         0,
		lastestMVCCTimeTickNotified: false,
		lastConsumedTimeTick:        0,

		emptyTimeTickFilteredCounter:   emptyTimeTickFilteredCounter,
		tsafeTimeTickUnfilteredCounter: tsafeTimeTickUnfilteredCounter,
	}
}

func updateThresholdWithConfiguration() {
	params := paramtable.Get()
	interval := params.StreamingCfg.DelegatorEmptyTimeTickMaxFilterInterval.GetAsDurationByParse()
	log.Info("delegator empty time tick max filter interval initialized", zap.Duration("interval", interval))
	thresholdUpdateIntervalMs.Store(interval.Milliseconds())
	params.Watch(params.StreamingCfg.DelegatorEmptyTimeTickMaxFilterInterval.Key, config.NewHandler(
		params.StreamingCfg.DelegatorEmptyTimeTickMaxFilterInterval.Key,
		func(_ *config.Event) {
			previousInterval := thresholdUpdateIntervalMs.Load()
			newInterval := params.StreamingCfg.DelegatorEmptyTimeTickMaxFilterInterval.GetAsDurationByParse()
			log.Info("delegator empty time tick max filter interval updated",
				zap.Duration("previousInterval", time.Duration(previousInterval)),
				zap.Duration("interval", newInterval))
			thresholdUpdateIntervalMs.Store(newInterval.Milliseconds())
		},
	))
}

type emptyTimeTickSlowdowner struct {
	lastestMVCCTimeTickGetter LastestMVCCTimeTickGetter

	lastestMVCCTimeTick         uint64
	lastestMVCCTimeTickNotified bool
	lastConsumedTimeTick        uint64
	thresholdMs                 int64
	lastTimeThresholdUpdated    time.Duration

	emptyTimeTickFilteredCounter   prometheus.Counter
	tsafeTimeTickUnfilteredCounter prometheus.Counter
}

// Filter filters the message by the consuming slowdowner.
// if true, the message should be filtered out.
// if false, the message should be processed.
func (sd *emptyTimeTickSlowdowner) Filter(msg *msgstream.MsgPack) (filtered bool) {
	defer func() {
		if !filtered {
			sd.lastConsumedTimeTick = msg.EndTs
			return
		}
		sd.emptyTimeTickFilteredCounter.Inc()
	}()

	if len(msg.Msgs) != 0 {
		return false
	}

	timetick := msg.EndTs

	// handle the case that if there's a pending
	sd.updateLastestMVCCTimeTick()
	if timetick < sd.lastestMVCCTimeTick {
		// catch up the latest time tick to make the tsafe check pass.
		// every time tick should be handled,
		// otherwise the search/query operation which tsafe is less than the latest mvcc time tick will be blocked.
		sd.tsafeTimeTickUnfilteredCounter.Inc()
		return false
	}

	// if the timetick is greater than the lastestMVCCTimeTick, it means all tsafe checks can be passed by it.
	// so we mark the notified flag to true, stop the mvcc check, then the threshold check will be activated.
	if !sd.lastestMVCCTimeTickNotified && timetick >= sd.lastestMVCCTimeTick {
		sd.lastestMVCCTimeTickNotified = true
		// This is the first time tick satisfying the tsafe check, so we should NOT filter it.
		sd.tsafeTimeTickUnfilteredCounter.Inc()
		return false
	}

	// For monitoring, we should sync the time tick at least once every threshold.
	return tsoutil.CalculateDuration(timetick, sd.lastConsumedTimeTick) < thresholdUpdateIntervalMs.Load()
}

func (sd *emptyTimeTickSlowdowner) updateLastestMVCCTimeTick() {
	if newIncoming := sd.lastestMVCCTimeTickGetter.GetLatestRequiredMVCCTimeTick(); newIncoming > sd.lastestMVCCTimeTick {
		sd.lastestMVCCTimeTick = newIncoming
		sd.lastestMVCCTimeTickNotified = false
	}
}
