package cgo

import (
	"reflect"
	"sync"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const (
	registerIndex      = 0
	maxSelectCase      = 65535
	defaultRegisterBuf = 1
)

var (
	futureManager *activeFutureManager
	initOnce      sync.Once
)

// initCGO initializes the cgo caller and future manager.
func initCGO() {
	initOnce.Do(func() {
		nodeID := paramtable.GetStringNodeID()
		initCaller(nodeID)
		initExecutor()
		futureManager = newActiveFutureManager(nodeID)
		futureManager.Run()
	})
}

type futureManagerStat struct {
	ActiveCount int64
}

func newActiveFutureManager(nodeID string) *activeFutureManager {
	manager := &activeFutureManager{
		activeCount:   atomic.NewInt64(0),
		activeFutures: make([]basicFuture, 0),
		cases:         make([]reflect.SelectCase, 1),
		register:      make(chan basicFuture, defaultRegisterBuf),
		nodeID:        nodeID,
	}
	manager.cases[0] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(manager.register),
	}
	return manager
}

// activeFutureManager manages the active futures.
// it will transfer the cancel signal into cgo.
type activeFutureManager struct {
	activeCount   *atomic.Int64
	activeFutures []basicFuture
	cases         []reflect.SelectCase
	register      chan basicFuture
	nodeID        string
}

// Run starts the active future manager.
func (m *activeFutureManager) Run() {
	go func() {
		for {
			m.doSelect()
		}
	}()
}

// Register registers a future when it's created into the manager.
func (m *activeFutureManager) Register(c basicFuture) {
	m.register <- c
}

// Stat returns the stat of the manager, only for testing now.
func (m *activeFutureManager) Stat() futureManagerStat {
	return futureManagerStat{
		ActiveCount: m.activeCount.Load(),
	}
}

// doSelect selects the active futures and cancel the finished ones.
func (m *activeFutureManager) doSelect() {
	index, newCancelableObject, _ := reflect.Select(m.getSelectableCases())
	if index == registerIndex {
		newCancelable := newCancelableObject.Interface().(basicFuture)
		m.cases = append(m.cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(newCancelable.Context().Done()),
		})
		m.activeFutures = append(m.activeFutures, newCancelable)
	} else {
		m.cases = append(m.cases[:index], m.cases[index+1:]...)
		offset := index - 1
		// cancel the future and move it into gc manager.
		m.activeFutures[offset].cancel(m.activeFutures[offset].Context().Err())
		m.activeFutures = append(m.activeFutures[:offset], m.activeFutures[offset+1:]...)
	}
	activeTotal := len(m.activeFutures)
	m.activeCount.Store(int64(activeTotal))
	metrics.ActiveFutureTotal.WithLabelValues(
		m.nodeID,
	).Set(float64(activeTotal))
}

func (m *activeFutureManager) getSelectableCases() []reflect.SelectCase {
	if len(m.cases) <= maxSelectCase {
		return m.cases
	}
	return m.cases[0:maxSelectCase]
}
