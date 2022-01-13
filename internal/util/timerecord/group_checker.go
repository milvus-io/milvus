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
	"time"
)

// groups maintains string to GroupChecker
var groups sync.Map

// GroupChecker checks members in same group silent for certain period of time
// print warning msg if there are item(s) that not reported
type GroupChecker struct {
	groupName string

	d        time.Duration // check duration
	t        *time.Ticker  // internal ticker
	ch       chan struct{} // closing signal
	lastest  sync.Map      // map member name => lastest report time
	initOnce sync.Once
	stopOnce sync.Once

	fn func(list []string)
}

// init start worker goroutine
// protected by initOnce
func (gc *GroupChecker) init() {
	gc.initOnce.Do(func() {
		gc.ch = make(chan struct{})
		go gc.work()
	})
}

// work is the main procedure logic
func (gc *GroupChecker) work() {
	gc.t = time.NewTicker(gc.d)
	var name string
	var ts time.Time

	for {
		select {
		case <-gc.t.C:
		case <-gc.ch:
			return
		}

		var list []string
		gc.lastest.Range(func(k, v interface{}) bool {
			name = k.(string)
			ts = v.(time.Time)
			if time.Since(ts) > gc.d {
				list = append(list, name)
			}
			return true
		})
		if len(list) > 0 && gc.fn != nil {
			gc.fn(list)
		}
	}
}

// Check updates the latest timestamp for provided name
func (gc *GroupChecker) Check(name string) {
	gc.lastest.Store(name, time.Now())
}

// Remove deletes name from watch list
func (gc *GroupChecker) Remove(name string) {
	gc.lastest.Delete(name)
}

// Stop closes the GroupChecker
func (gc *GroupChecker) Stop() {
	gc.stopOnce.Do(func() {
		close(gc.ch)
		groups.Delete(gc.groupName)
	})
}

// GetGroupChecker returns the GroupChecker with related group name
// if no exist GroupChecker has the provided name, a new instance will be created with provided params
// otherwise the params will be ignored
func GetGroupChecker(groupName string, duration time.Duration, fn func([]string)) *GroupChecker {
	gc := &GroupChecker{
		groupName: groupName,
		d:         duration,
		fn:        fn,
	}
	actual, loaded := groups.LoadOrStore(groupName, gc)
	if !loaded {
		gc.init()
	}

	gc = actual.(*GroupChecker)
	return gc
}
