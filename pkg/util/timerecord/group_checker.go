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

package timerecord

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// groups maintains string to GroupChecker
var groups = typeutil.NewConcurrentMap[string, *CheckerManager]()

type Checker struct {
	name        string
	manager     *CheckerManager
	lastChecked atomic.Value
}

func NewChecker(name string, manager *CheckerManager) *Checker {
	checker := &Checker{}
	checker.name = name
	checker.manager = manager
	checker.lastChecked.Store(time.Now())
	manager.Register(name, checker)
	return checker
}

func (checker *Checker) Check() {
	checker.lastChecked.Store(time.Now())
}

func (checker *Checker) Close() {
	checker.manager.Remove(checker.name)
}

// CheckerManager checks members in same group silent for certain period of time
// print warning msg if there are item(s) that not reported
type CheckerManager struct {
	groupName string

	d        time.Duration                             // check duration
	t        *time.Ticker                              // internal ticker
	ch       chan struct{}                             // closing signal
	checkers *typeutil.ConcurrentMap[string, *Checker] // map member name => checker
	initOnce sync.Once
	stopOnce sync.Once

	fn func(list []string)
}

// init start worker goroutine
// protected by initOnce
func (gc *CheckerManager) init() {
	gc.initOnce.Do(func() {
		gc.ch = make(chan struct{})
		go gc.work()
	})
}

// work is the main procedure logic
func (gc *CheckerManager) work() {
	gc.t = time.NewTicker(gc.d)
	defer gc.t.Stop()

	for {
		select {
		case <-gc.t.C:
		case <-gc.ch:
			return
		}

		var list []string
		gc.checkers.Range(func(name string, checker *Checker) bool {
			if time.Since(checker.lastChecked.Load().(time.Time)) > gc.d {
				list = append(list, name)
			}
			return true
		})
		if len(list) > 0 && gc.fn != nil {
			gc.fn(list)
		}
	}
}

func (gc *CheckerManager) Register(name string, checker *Checker) {
	gc.checkers.Insert(name, checker)
}

// Remove deletes name from watch list
func (gc *CheckerManager) Remove(name string) {
	gc.checkers.GetAndRemove(name)
}

// Stop closes the GroupChecker
func (gc *CheckerManager) Stop() {
	gc.stopOnce.Do(func() {
		close(gc.ch)
		groups.GetAndRemove(gc.groupName)
	})
}

// GetGroupChecker returns the GroupChecker with related group name
// if no exist GroupChecker has the provided name, a new instance will be created with provided params
// otherwise the params will be ignored
func GetCheckerManger(groupName string, duration time.Duration, fn func([]string)) *CheckerManager {
	gc := &CheckerManager{
		groupName: groupName,
		d:         duration,
		fn:        fn,
		checkers:  typeutil.NewConcurrentMap[string, *Checker](),
	}
	gc, loaded := groups.GetOrInsert(groupName, gc)
	if !loaded {
		gc.init()
	}

	return gc
}
