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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTSafeReplica_valid(t *testing.T) {
	replica := newTSafeReplica()
	replica.addTSafe(defaultVChannel)

	watcher := newTSafeWatcher()
	err := replica.registerTSafeWatcher(defaultVChannel, watcher)
	assert.NoError(t, err)

	timestamp := Timestamp(1000)
	err = replica.setTSafe(defaultVChannel, defaultCollectionID, timestamp)
	assert.NoError(t, err)
	time.Sleep(20 * time.Millisecond)
	resT, err := replica.getTSafe(defaultVChannel)
	assert.NoError(t, err)
	assert.Equal(t, timestamp, resT)

	err = replica.removeTSafe(defaultVChannel)
	assert.NoError(t, err)
}

func TestTSafeReplica_invalid(t *testing.T) {
	replica := newTSafeReplica()
	replica.addTSafe(defaultVChannel)

	watcher := newTSafeWatcher()
	err := replica.registerTSafeWatcher(defaultVChannel, watcher)
	assert.NoError(t, err)

	timestamp := Timestamp(1000)
	err = replica.setTSafe(defaultVChannel, defaultCollectionID, timestamp)
	assert.NoError(t, err)
	time.Sleep(20 * time.Millisecond)
	resT, err := replica.getTSafe(defaultVChannel)
	assert.NoError(t, err)
	assert.Equal(t, timestamp, resT)

	err = replica.removeTSafe(defaultVChannel)
	assert.NoError(t, err)

	replica.addTSafe(defaultVChannel)
	replica.addTSafe(defaultVChannel)
}
