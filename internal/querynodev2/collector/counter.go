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

package collector

import (
	"sync"
)

type counter struct {
	sync.Mutex
	values map[string]int64
}

func (c *counter) Inc(label string, value int64) {
	c.Lock()
	defer c.Unlock()

	v, ok := c.values[label]
	if !ok {
		c.values[label] = value
	} else {
		v += value
		c.values[label] = v
	}
}

func (c *counter) Dec(label string, value int64) {
	c.Lock()
	defer c.Unlock()

	v, ok := c.values[label]
	if !ok {
		c.values[label] = -value
	} else {
		v -= value
		c.values[label] = v
	}
}

func (c *counter) Set(label string, value int64) {
	c.Lock()
	defer c.Unlock()
	c.values[label] = value
}

func (c *counter) Get(label string) int64 {
	c.Lock()
	defer c.Unlock()

	v, ok := c.values[label]
	if !ok {
		return 0
	}
	return v
}

func (c *counter) Remove(label string) bool {
	c.Lock()
	defer c.Unlock()

	_, ok := c.values[label]
	if ok {
		delete(c.values, label)
	}
	return ok
}

func newCounter() *counter {
	return &counter{
		values: make(map[string]int64),
	}
}
