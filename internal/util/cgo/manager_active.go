package cgo

import (
	"reflect"

	"go.uber.org/atomic"
)

const (
	registerIndex      = 0
	maxSelectCase      = 65535
	defaultRegisterBuf = 1
)

var futureManager *activeFutureManager

func init() {
	futureManager = newActiveFutureManager()
	futureManager.Run()
}

type futureManagerStat struct {
	ActiveCount int64
	GCCount     int64
}

func newActiveFutureManager() *activeFutureManager {
	manager := &activeFutureManager{
		gcManager:     newGCManager(),
		activeCount:   atomic.NewInt64(0),
		activeFutures: make([]basicFuture, 0),
		cases:         make([]reflect.SelectCase, 1),
		register:      make(chan basicFuture, defaultRegisterBuf),
	}
	manager.cases[0] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(manager.register),
	}
	return manager
}

type activeFutureManager struct {
	gcManager     *gcFutureManager
	activeCount   *atomic.Int64
	activeFutures []basicFuture
	cases         []reflect.SelectCase
	register      chan basicFuture
}

func (m *activeFutureManager) Run() {
	go func() {
		for {
			m.doSelect()
		}
	}()
	go m.gcManager.Run()
}

func (m *activeFutureManager) Register(c basicFuture) {
	m.register <- c
}

func (m *activeFutureManager) Stat() futureManagerStat {
	return futureManagerStat{
		ActiveCount: m.activeCount.Load(),
		GCCount:     m.gcManager.PendingCount(),
	}
}

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
		m.gcManager.Register(m.activeFutures[offset])
		m.activeFutures = append(m.activeFutures[:offset], m.activeFutures[offset+1:]...)
	}
	m.activeCount.Store(int64(len(m.activeFutures)))
}

func (m *activeFutureManager) getSelectableCases() []reflect.SelectCase {
	if len(m.cases) <= maxSelectCase {
		return m.cases
	}
	return m.cases[0:maxSelectCase]
}
