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

package proxy

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// ticker can update ts only when the minTs are greater than the ts of ticker, we can use maxTs to update current later
type getPChanStatisticsFuncType func() (map[pChan]*pChanStatistics, error)

// channelsTimeTicker manages the timestamp statistics
type channelsTimeTicker interface {
	// start starts the channels time ticker.
	start() error
	// close closes the channels time ticker.
	close() error
	// getLastTick returns the last write timestamp of specific pchan.
	getLastTick(pchan pChan) (Timestamp, error)
	// getMinTsStatistics returns the last write timestamp of all pchans.
	getMinTsStatistics() (map[pChan]Timestamp, Timestamp, error)
	// getMinTick returns the minimum last write timestamp between all pchans.
	getMinTick() Timestamp
}

// make sure channelsTimeTickerImpl implements channelsTimeTicker.
var _ channelsTimeTicker = (*channelsTimeTickerImpl)(nil)

// channelsTimeTickerImpl implements channelsTimeTicker.
type channelsTimeTickerImpl struct {
	interval          time.Duration       // interval to synchronize
	minTsStatistics   map[pChan]Timestamp // pchan -> min Timestamp
	statisticsMtx     sync.RWMutex
	getStatisticsFunc getPChanStatisticsFuncType
	tso               tsoAllocator
	currents          map[pChan]Timestamp
	wg                sync.WaitGroup
	ctx               context.Context
	cancel            context.CancelFunc
	defaultTimestamp  Timestamp
	minTimestamp      Timestamp
}

func (ticker *channelsTimeTickerImpl) getMinTsStatistics() (map[pChan]Timestamp, Timestamp, error) {
	ticker.statisticsMtx.RLock()
	defer ticker.statisticsMtx.RUnlock()

	ret := make(map[pChan]Timestamp)
	for k, v := range ticker.minTsStatistics {
		if v > 0 {
			ret[k] = v
		}
	}
	return ret, ticker.defaultTimestamp, nil
}

func (ticker *channelsTimeTickerImpl) initStatistics() {
	ticker.statisticsMtx.Lock()
	defer ticker.statisticsMtx.Unlock()

	for pchan := range ticker.minTsStatistics {
		ticker.minTsStatistics[pchan] = 0
	}
}

func (ticker *channelsTimeTickerImpl) initCurrents(current Timestamp) {
	for pchan := range ticker.currents {
		ticker.currents[pchan] = current
	}
}

func (ticker *channelsTimeTickerImpl) tick() error {
	now, err := ticker.tso.AllocOne(ticker.ctx)
	if err != nil {
		log.Warn("Proxy channelsTimeTickerImpl failed to get ts from tso", zap.Error(err))
		return err
	}

	stats, err2 := ticker.getStatisticsFunc()
	if err2 != nil {
		log.Warn("failed to get tt statistics", zap.Error(err))
		return nil
	}

	ticker.statisticsMtx.Lock()
	defer ticker.statisticsMtx.Unlock()

	ticker.defaultTimestamp = now
	minTs := now

	for pchan := range ticker.currents {
		current := ticker.currents[pchan]
		stat, ok := stats[pchan]

		if !ok {
			delete(ticker.minTsStatistics, pchan)
			delete(ticker.currents, pchan)
		} else {
			if stat.minTs > current {
				ticker.minTsStatistics[pchan] = stat.minTs - 1
				next := now + Timestamp(Params.ProxyCfg.TimeTickInterval.GetAsDuration(time.Millisecond))
				if next > stat.maxTs {
					next = stat.maxTs
				}
				ticker.currents[pchan] = next
			}
			lastMin := ticker.minTsStatistics[pchan]
			if minTs > lastMin {
				minTs = lastMin
			}
		}
	}

	for pchan, value := range stats {
		if value.minTs == typeutil.ZeroTimestamp {
			log.Warn("channelsTimeTickerImpl.tick, stats contains physical channel which min ts is zero ",
				zap.String("pchan", pchan))
			continue
		}
		_, ok := ticker.currents[pchan]
		if !ok {
			ticker.minTsStatistics[pchan] = value.minTs - 1
			ticker.currents[pchan] = now
		}
		if minTs > value.minTs-1 {
			minTs = value.minTs - 1
		}
	}
	ticker.minTimestamp = minTs

	return nil
}

func (ticker *channelsTimeTickerImpl) tickLoop() {
	defer ticker.wg.Done()

	timer := time.NewTicker(ticker.interval)
	defer timer.Stop()

	for {
		select {
		case <-ticker.ctx.Done():
			return
		case <-timer.C:
			err := ticker.tick()
			if err != nil {
				log.Warn("channelsTimeTickerImpl.tickLoop", zap.Error(err))
			}
		}
	}
}

func (ticker *channelsTimeTickerImpl) start() error {
	ticker.initStatistics()

	current, err := ticker.tso.AllocOne(ticker.ctx)
	if err != nil {
		return err
	}
	ticker.initCurrents(current)

	ticker.wg.Add(1)
	go ticker.tickLoop()

	return nil
}

func (ticker *channelsTimeTickerImpl) close() error {
	ticker.cancel()
	ticker.wg.Wait()
	return nil
}

func (ticker *channelsTimeTickerImpl) getLastTick(pchan pChan) (Timestamp, error) {
	ticker.statisticsMtx.RLock()
	defer ticker.statisticsMtx.RUnlock()

	ts, ok := ticker.minTsStatistics[pchan]
	if !ok {
		return ticker.defaultTimestamp, nil
	}

	return ts, nil
}

func (ticker *channelsTimeTickerImpl) getMinTick() Timestamp {
	ticker.statisticsMtx.RLock()
	defer ticker.statisticsMtx.RUnlock()
	// may be zero
	return ticker.minTimestamp
}

// newChannelsTimeTicker returns a channels time ticker.
func newChannelsTimeTicker(
	ctx context.Context,
	interval time.Duration,
	pchans []pChan,
	getStatisticsFunc getPChanStatisticsFuncType,
	tso tsoAllocator,
) *channelsTimeTickerImpl {
	ctx1, cancel := context.WithCancel(ctx)

	ticker := &channelsTimeTickerImpl{
		interval:          interval,
		minTsStatistics:   make(map[pChan]Timestamp),
		getStatisticsFunc: getStatisticsFunc,
		tso:               tso,
		currents:          make(map[pChan]Timestamp),
		ctx:               ctx1,
		cancel:            cancel,
	}

	for _, pchan := range pchans {
		ticker.minTsStatistics[pchan] = 0
		ticker.currents[pchan] = 0
	}

	return ticker
}
