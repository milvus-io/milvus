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
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func newGetStatisticsFunc(pchans []pChan) getPChanStatisticsFuncType {
	totalPchan := len(pchans)
	pchanNum := rand.Uint64()%(uint64(totalPchan)) + 1
	pchans2 := make([]pChan, 0, pchanNum)
	for i := 0; uint64(i) < pchanNum; i++ {
		pchans2 = append(pchans2, pchans[i])
	}

	retFunc := func() (map[pChan]*pChanStatistics, error) {
		ret := make(map[pChan]*pChanStatistics)
		for _, pchannel := range pchans2 {
			minTs := Timestamp(time.Now().UnixNano())
			ret[pchannel] = &pChanStatistics{
				minTs: minTs,
				maxTs: minTs + Timestamp(time.Millisecond*10),
			}
		}
		return ret, nil
	}
	return retFunc
}

func TestChannelsTimeTickerImpl_start(t *testing.T) {
	interval := time.Millisecond * 10
	pchanNum := rand.Uint64()%10 + 1
	pchans := make([]pChan, 0, pchanNum)
	for i := 0; uint64(i) < pchanNum; i++ {
		pchans = append(pchans, funcutil.GenRandomStr())
	}
	tso := newMockTsoAllocator()
	ctx := context.Background()

	ticker := newChannelsTimeTicker(ctx, interval, pchans, newGetStatisticsFunc(pchans), tso)
	err := ticker.start()
	assert.Equal(t, nil, err)

	defer func() {
		err := ticker.close()
		assert.Equal(t, nil, err)
	}()

	time.Sleep(100 * time.Millisecond)
}

func TestChannelsTimeTickerImpl_close(t *testing.T) {
	interval := time.Millisecond * 10
	pchanNum := rand.Uint64()%10 + 1
	pchans := make([]pChan, 0, pchanNum)
	for i := 0; uint64(i) < pchanNum; i++ {
		pchans = append(pchans, funcutil.GenRandomStr())
	}
	tso := newMockTsoAllocator()
	ctx := context.Background()

	ticker := newChannelsTimeTicker(ctx, interval, pchans, newGetStatisticsFunc(pchans), tso)
	err := ticker.start()
	assert.Equal(t, nil, err)

	defer func() {
		err := ticker.close()
		assert.Equal(t, nil, err)
	}()

	time.Sleep(100 * time.Millisecond)
}

func TestChannelsTimeTickerImpl_getLastTick(t *testing.T) {
	interval := time.Millisecond * 10
	pchanNum := rand.Uint64()%10 + 1
	pchans := make([]pChan, 0, pchanNum)
	for i := 0; uint64(i) < pchanNum; i++ {
		pchans = append(pchans, funcutil.GenRandomStr())
	}
	tso := newMockTsoAllocator()
	ctx := context.Background()

	channelTicker := newChannelsTimeTicker(ctx, interval, pchans, newGetStatisticsFunc(pchans), tso)
	err := channelTicker.start()
	assert.Equal(t, nil, err)

	var wg sync.WaitGroup
	wg.Add(1)
	b := make(chan struct{}, 1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(interval * 40)
		defer ticker.Stop()
		for {
			select {
			case <-b:
				return
			case <-ticker.C:
				for _, pchan := range pchans {
					ts, err := channelTicker.getLastTick(pchan)
					assert.Equal(t, nil, err)
					log.Debug("TestChannelsTimeTickerImpl_getLastTick",
						zap.Any("pchan", pchan),
						zap.Any("minTs", ts))
				}
			}
		}
	}()
	time.Sleep(100 * time.Millisecond)
	b <- struct{}{}
	wg.Wait()

	defer func() {
		err := channelTicker.close()
		assert.Equal(t, nil, err)
	}()

	time.Sleep(100 * time.Millisecond)
}

func TestChannelsTimeTickerImpl_getMinTsStatistics(t *testing.T) {
	interval := time.Millisecond * 10
	pchanNum := rand.Uint64()%10 + 1
	pchans := make([]pChan, 0, pchanNum)
	for i := 0; uint64(i) < pchanNum; i++ {
		pchans = append(pchans, funcutil.GenRandomStr())
	}
	tso := newMockTsoAllocator()
	ctx := context.Background()

	channelTicker := newChannelsTimeTicker(ctx, interval, pchans, newGetStatisticsFunc(pchans), tso)
	err := channelTicker.start()
	assert.Equal(t, nil, err)

	var wg sync.WaitGroup
	wg.Add(1)
	b := make(chan struct{}, 1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(interval * 40)
		defer ticker.Stop()
		for {
			select {
			case <-b:
				return
			case <-ticker.C:
				stats, _, err := channelTicker.getMinTsStatistics()
				assert.Equal(t, nil, err)
				for pchan, ts := range stats {
					log.Debug("TestChannelsTimeTickerImpl_getLastTick",
						zap.Any("pchan", pchan),
						zap.Any("minTs", ts))
				}
			}
		}
	}()
	time.Sleep(100 * time.Millisecond)
	b <- struct{}{}
	wg.Wait()

	defer func() {
		err := channelTicker.close()
		assert.Equal(t, nil, err)
	}()

	time.Sleep(100 * time.Millisecond)
}

func TestChannelsTimeTickerImpl_getMinTick(t *testing.T) {
	interval := time.Millisecond * 10
	pchanNum := rand.Uint64()%10 + 1
	pchans := make([]pChan, 0, pchanNum)
	for i := 0; uint64(i) < pchanNum; i++ {
		pchans = append(pchans, funcutil.GenRandomStr())
	}
	tso := newMockTsoAllocator()
	ctx := context.Background()

	channelTicker := newChannelsTimeTicker(ctx, interval, pchans, newGetStatisticsFunc(pchans), tso)
	err := channelTicker.start()
	assert.Equal(t, nil, err)

	var wg sync.WaitGroup
	wg.Add(1)
	b := make(chan struct{}, 1)
	ts := typeutil.ZeroTimestamp
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(interval * 40)
		defer ticker.Stop()
		for {
			select {
			case <-b:
				return
			case <-ticker.C:
				minTs := channelTicker.getMinTick()
				assert.GreaterOrEqual(t, minTs, ts)
			}
		}
	}()
	time.Sleep(100 * time.Millisecond)
	b <- struct{}{}
	wg.Wait()

	defer func() {
		err := channelTicker.close()
		assert.Equal(t, nil, err)
	}()

	time.Sleep(100 * time.Millisecond)
}
