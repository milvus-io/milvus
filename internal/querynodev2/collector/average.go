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

	"github.com/milvus-io/milvus/pkg/util/merr"
)

type averageData struct {
	total float64
	count int32
}

func (v *averageData) Add(value float64) {
	v.total += value
	v.count++
}

func (v *averageData) Value() float64 {
	if v.count == 0 {
		return 0
	}
	return v.total / float64(v.count)
}

type averageCollector struct {
	sync.Mutex
	averages map[string]*averageData
}

func (c *averageCollector) Register(label string) {
	c.Lock()
	defer c.Unlock()
	if _, ok := c.averages[label]; !ok {
		c.averages[label] = &averageData{}
	}
}

func (c *averageCollector) Add(label string, value float64) {
	c.Lock()
	defer c.Unlock()

	if average, ok := c.averages[label]; ok {
		average.Add(value)
	}
}

func (c *averageCollector) Reset(label string) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.averages[label]; ok {
		c.averages[label] = &averageData{}
	}
}

func (c *averageCollector) Average(label string) (float64, error) {
	c.Lock()
	defer c.Unlock()

	average, ok := c.averages[label]
	if !ok {
		return 0, merr.WrapErrMetricNotFound(label)
	}

	return average.Value(), nil
}

func newAverageCollector() *averageCollector {
	return &averageCollector{
		averages: make(map[string]*averageData),
	}
}
