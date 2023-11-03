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

package datanode

import (
	"sync"

	"go.uber.org/atomic"
)

type flushTaskCounter struct {
	inner sync.Map // channel -> counter (*atomic.Int32)
}

func (c *flushTaskCounter) getOrZero(channel string) int32 {
	counter, exist := c.inner.Load(channel)
	if !exist {
		return 0
	}
	return counter.(*atomic.Int32).Load()
}

func (c *flushTaskCounter) increaseImpl(channel string, delta int32) {
	counter, _ := c.inner.LoadOrStore(channel, atomic.NewInt32(0))
	counter.(*atomic.Int32).Add(delta)
}

func (c *flushTaskCounter) increase(channel string) {
	c.increaseImpl(channel, 1)
}

func (c *flushTaskCounter) decrease(channel string) {
	c.increaseImpl(channel, -1)
}

func (c *flushTaskCounter) close() {
	allChannels := make([]string, 0)
	c.inner.Range(func(channel any, _ any) bool {
		allChannels = append(allChannels, channel.(string))
		return false
	})
	for _, channel := range allChannels {
		c.inner.Delete(channel)
	}
}

func newFlushTaskCounter() *flushTaskCounter {
	return &flushTaskCounter{}
}

var (
	globalFlushTaskCounter *flushTaskCounter
	flushTaskCounterOnce   sync.Once
)

func getOrCreateFlushTaskCounter() *flushTaskCounter {
	flushTaskCounterOnce.Do(func() {
		globalFlushTaskCounter = newFlushTaskCounter()
	})
	return globalFlushTaskCounter
}
