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

package querynode

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTSafe_GetAndSet(t *testing.T) {
	tSafe := newTSafe(context.Background(), "TestTSafe-channel")
	tSafe.start()
	watcher := newTSafeWatcher()
	tSafe.registerTSafeWatcher(watcher)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		// wait work
		<-watcher.watcherChan()
		timestamp := tSafe.get()
		assert.Equal(t, timestamp, Timestamp(1000))
		wg.Done()
	}()
	tSafe.set(UniqueID(1), Timestamp(1000))
	wg.Wait()
}

func TestTSafe_Remove(t *testing.T) {
	tSafe := newTSafe(context.Background(), "TestTSafe-remove")
	tSafe.start()
	watcher := newTSafeWatcher()
	tSafe.registerTSafeWatcher(watcher)

	tSafe.set(UniqueID(1), Timestamp(1000))
	tSafe.set(UniqueID(2), Timestamp(1001))

	<-watcher.watcherChan()
	timestamp := tSafe.get()
	assert.Equal(t, timestamp, Timestamp(1000))

	tSafe.removeRecord(UniqueID(1))
	timestamp = tSafe.get()
	assert.Equal(t, timestamp, Timestamp(1001))
}

func TestTSafe_Close(t *testing.T) {
	tSafe := newTSafe(context.Background(), "TestTSafe-close")
	tSafe.start()
	watcher := newTSafeWatcher()
	tSafe.registerTSafeWatcher(watcher)

	// test set won't panic while close
	go func() {
		for i := 0; i <= 100; i++ {
			tSafe.set(UniqueID(i), Timestamp(1000))
		}
	}()

	tSafe.close()

	// wait until channel close
	for range watcher.watcherChan() {

	}

	tSafe.set(UniqueID(101), Timestamp(1000))
	tSafe.removeRecord(UniqueID(1))
	// register TSafe will fail
	err := tSafe.registerTSafeWatcher(watcher)
	assert.Error(t, err)
}
