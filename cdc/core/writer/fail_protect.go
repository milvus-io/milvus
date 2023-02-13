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

package writer

import (
	"fmt"
	"sync/atomic"
	"time"
)

var FastFail = NewErrorProtect(1, time.Minute)

// ErrorProtect avoid to occur a lot of errors in the short time
type ErrorProtect struct {
	per     int32
	unit    time.Duration
	current int32
	ticker  *time.Ticker
	c       chan struct{}
}

func NewErrorProtect(per int32, unit time.Duration) *ErrorProtect {
	protect := &ErrorProtect{
		per:    per,
		unit:   unit,
		ticker: time.NewTicker(unit),
		c:      make(chan struct{}),
	}
	protect.startTicker()

	return protect
}

func (e *ErrorProtect) startTicker() {
	go func() {
		if e.per <= 1 {
			return
		}
		for {
			select {
			case <-e.ticker.C:
				atomic.StoreInt32(&e.current, 0)
			case <-e.c:
				return
			}
		}
	}()
}

func (e *ErrorProtect) close() {
	select {
	case <-e.c:
	default:
		e.ticker.Stop()
		close(e.c)
	}
}

func (e *ErrorProtect) Inc() {
	atomic.AddInt32(&e.current, 1)
	if e.current > e.per {
		e.close()
	}
}

func (e *ErrorProtect) Chan() <-chan struct{} {
	return e.c
}

func (e *ErrorProtect) Info() string {
	return fmt.Sprintf("current: %d, per: %d, unit: %s", e.current, e.per, e.unit.String())
}
