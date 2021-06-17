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

package tso

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/stretchr/testify/assert"
)

var gTestTsoAllocator *GlobalTSOAllocator

func TestGlobalTSOAllocator_All(t *testing.T) {
	endpoints := os.Getenv("ETCD_ENDPOINTS")
	if endpoints == "" {
		endpoints = "localhost:2379"
	}
	etcdEndpoints := strings.Split(endpoints, ",")
	gTestTsoAllocator = NewGlobalTSOAllocator("timestamp", tsoutil.NewTSOKVBase(etcdEndpoints, "/test/root/kv", "tsoTest"))
	t.Run("Initialize", func(t *testing.T) {
		err := gTestTsoAllocator.Initialize()
		assert.Nil(t, err)
	})

	t.Run("GenerateTSO", func(t *testing.T) {
		count := 1000
		perCount := uint32(100)
		startTs, err := gTestTsoAllocator.GenerateTSO(perCount)
		assert.Nil(t, err)
		lastPhysical, lastLogical := tsoutil.ParseTS(startTs)
		for i := 0; i < count; i++ {
			ts, _ := gTestTsoAllocator.GenerateTSO(perCount)
			physical, logical := tsoutil.ParseTS(ts)
			if lastPhysical.Equal(physical) {
				diff := logical - lastLogical
				assert.Equal(t, uint64(perCount), diff)
			}
			lastPhysical, lastLogical = physical, logical
		}
	})

	gTestTsoAllocator.SetLimitMaxLogic(false)
	t.Run("GenerateTSO2", func(t *testing.T) {
		count := 1000
		maxL := 1 << 18
		startTs, err := gTestTsoAllocator.GenerateTSO(uint32(maxL))
		step := 10
		perCount := uint32(step) << 18 // 10 ms
		assert.Nil(t, err)
		lastPhysical, lastLogical := tsoutil.ParseTS(startTs)
		for i := 0; i < count; i++ {
			ts, _ := gTestTsoAllocator.GenerateTSO(perCount)
			physical, logical := tsoutil.ParseTS(ts)
			assert.Equal(t, logical, lastLogical)
			assert.Equal(t, physical, lastPhysical.Add(time.Millisecond*time.Duration(step)))
			lastPhysical = physical
		}
	})

	gTestTsoAllocator.SetLimitMaxLogic(true)
	t.Run("SetTSO", func(t *testing.T) {
		curTime := time.Now()
		nextTime := curTime.Add(2 * time.Second)
		physical := nextTime.UnixNano() / int64(time.Millisecond)
		logical := int64(0)
		err := gTestTsoAllocator.SetTSO(tsoutil.ComposeTS(physical, logical))
		assert.Nil(t, err)
	})

	t.Run("UpdateTSO", func(t *testing.T) {
		err := gTestTsoAllocator.UpdateTSO()
		assert.Nil(t, err)
	})

	t.Run("Reset", func(t *testing.T) {
		gTestTsoAllocator.Reset()
	})

}
