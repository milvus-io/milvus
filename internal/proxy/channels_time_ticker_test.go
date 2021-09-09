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

package proxy

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/util/funcutil"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"

	"github.com/stretchr/testify/assert"
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

	time.Sleep(time.Second)
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

	time.Sleep(time.Second)
}

func TestChannelsTimeTickerImpl_addPChan(t *testing.T) {
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

	newPChanNum := rand.Uint64()%10 + 1
	for i := 0; uint64(i) < newPChanNum; i++ {
		err = ticker.addPChan(funcutil.GenRandomStr())
		assert.Equal(t, nil, err)
	}

	defer func() {
		err := ticker.close()
		assert.Equal(t, nil, err)
	}()

	time.Sleep(time.Second)
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

	ticker := newChannelsTimeTicker(ctx, interval, pchans, newGetStatisticsFunc(pchans), tso)
	err := ticker.start()
	assert.Equal(t, nil, err)

	var wg sync.WaitGroup
	wg.Add(1)
	b := make(chan struct{}, 1)
	go func() {
		defer wg.Done()
		timer := time.NewTicker(interval * 40)
		for {
			select {
			case <-b:
				return
			case <-timer.C:
				for _, pchan := range pchans {
					ts, err := ticker.getLastTick(pchan)
					assert.Equal(t, nil, err)
					log.Debug("TestChannelsTimeTickerImpl_getLastTick",
						zap.Any("pchan", pchan),
						zap.Any("minTs", ts))
				}
			}
		}
	}()
	time.Sleep(time.Second)
	b <- struct{}{}
	wg.Wait()

	defer func() {
		err := ticker.close()
		assert.Equal(t, nil, err)
	}()

	time.Sleep(time.Second)
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

	ticker := newChannelsTimeTicker(ctx, interval, pchans, newGetStatisticsFunc(pchans), tso)
	err := ticker.start()
	assert.Equal(t, nil, err)

	var wg sync.WaitGroup
	wg.Add(1)
	b := make(chan struct{}, 1)
	go func() {
		defer wg.Done()
		timer := time.NewTicker(interval * 40)
		for {
			select {
			case <-b:
				return
			case <-timer.C:
				stats, err := ticker.getMinTsStatistics()
				assert.Equal(t, nil, err)
				for pchan, ts := range stats {
					log.Debug("TestChannelsTimeTickerImpl_getLastTick",
						zap.Any("pchan", pchan),
						zap.Any("minTs", ts))
				}
			}
		}
	}()
	time.Sleep(time.Second)
	b <- struct{}{}
	wg.Wait()

	defer func() {
		err := ticker.close()
		assert.Equal(t, nil, err)
	}()

	time.Sleep(time.Second)
}
