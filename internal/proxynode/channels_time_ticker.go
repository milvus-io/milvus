package proxynode

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
)

type pChanStatistics struct {
	minTs   Timestamp
	maxTs   Timestamp
	invalid bool // invalid is true when there is no task in queue
}

// channelsTimeTickerCheckFunc(pchan, ts) return true only when all timestamp of tasks who use the pchan is greater than ts
type channelsTimeTickerCheckFunc func(string, Timestamp) bool

// ticker can update ts only when the minTs greater than the ts of ticker, we can use maxTs to update current later
type getPChanStatisticsFuncType func(pChan) (pChanStatistics, error)

// use interface tsoAllocator to keep channelsTimeTickerImpl testable
type tsoAllocator interface {
	//Start() error
	AllocOne() (Timestamp, error)
	//Alloc(count uint32) ([]Timestamp, error)
	//ClearCache()
}

type channelsTimeTicker interface {
	start() error
	close() error
	addPChan(pchan pChan) error
	removePChan(pchan pChan) error
	getLastTick(pchan pChan) (Timestamp, error)
	getMinTsStatistics() (map[pChan]Timestamp, error)
}

type channelsTimeTickerImpl struct {
	interval          time.Duration       // interval to synchronize
	minTsStatistics   map[pChan]Timestamp // pchan -> min Timestamp
	statisticsMtx     sync.RWMutex
	getStatisticsFunc getPChanStatisticsFuncType
	tso               tsoAllocator
	currents          map[pChan]Timestamp
	currentsMtx       sync.RWMutex
	wg                sync.WaitGroup
	ctx               context.Context
	cancel            context.CancelFunc
}

func (ticker *channelsTimeTickerImpl) getMinTsStatistics() (map[pChan]Timestamp, error) {
	ticker.statisticsMtx.RLock()
	defer ticker.statisticsMtx.RUnlock()

	return ticker.minTsStatistics, nil
}

func (ticker *channelsTimeTickerImpl) initStatistics() {
	ticker.statisticsMtx.Lock()
	defer ticker.statisticsMtx.Unlock()

	for pchan := range ticker.minTsStatistics {
		ticker.minTsStatistics[pchan] = 0
	}
}

func (ticker *channelsTimeTickerImpl) initCurrents(current Timestamp) {
	ticker.currentsMtx.Lock()
	defer ticker.currentsMtx.Unlock()

	for pchan := range ticker.currents {
		ticker.currents[pchan] = current
	}
}

// What if golang support generic? interface{} is not comparable now!
func getTs(ts1, ts2 Timestamp, comp func(ts1, ts2 Timestamp) bool) Timestamp {
	if comp(ts1, ts2) {
		return ts1
	}
	return ts2
}

func (ticker *channelsTimeTickerImpl) tick() error {
	ticker.statisticsMtx.Lock()
	defer ticker.statisticsMtx.Unlock()

	ticker.currentsMtx.Lock()
	defer ticker.currentsMtx.Unlock()

	for pchan := range ticker.currents {
		current := ticker.currents[pchan]

		stats, err := ticker.getStatisticsFunc(pchan)
		if err != nil {
			log.Warn("failed to get statistics from scheduler", zap.Error(err))
			continue
		}

		if !stats.invalid && stats.minTs > current {
			ticker.minTsStatistics[pchan] = current
			ticker.currents[pchan] = getTs(current+Timestamp(ticker.interval), stats.maxTs, func(ts1, ts2 Timestamp) bool {
				return ts1 > ts2
			})
		} else if stats.invalid {
			ticker.minTsStatistics[pchan] = current
			// ticker.currents[pchan] = current + Timestamp(ticker.interval)
			t, err := ticker.tso.AllocOne()
			if err != nil {
				log.Warn("failed to get ts from tso", zap.Error(err))
				continue
			}
			ticker.currents[pchan] = t
		}
	}

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

	current, err := ticker.tso.AllocOne()
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

func (ticker *channelsTimeTickerImpl) addPChan(pchan pChan) error {
	ticker.statisticsMtx.Lock()
	defer ticker.statisticsMtx.Unlock()

	if _, ok := ticker.minTsStatistics[pchan]; ok {
		return fmt.Errorf("pChan %v already exist", pchan)
	}

	ticker.minTsStatistics[pchan] = 0

	return nil
}

func (ticker *channelsTimeTickerImpl) removePChan(pchan pChan) error {
	ticker.statisticsMtx.Lock()
	defer ticker.statisticsMtx.Unlock()

	if _, ok := ticker.minTsStatistics[pchan]; !ok {
		return fmt.Errorf("pChan %v don't exist", pchan)
	}

	delete(ticker.minTsStatistics, pchan)

	return nil
}

func (ticker *channelsTimeTickerImpl) getLastTick(pchan pChan) (Timestamp, error) {
	ticker.statisticsMtx.RLock()
	defer ticker.statisticsMtx.RUnlock()

	ts, ok := ticker.minTsStatistics[pchan]
	if !ok {
		return 0, fmt.Errorf("pChan %v not found", pchan)
	}

	return ts, nil
}

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
