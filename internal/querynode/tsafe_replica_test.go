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
	replica.registerTSafeWatcher(defaultVChannel, watcher)

	timestamp := Timestamp(1000)
	replica.setTSafe(defaultVChannel, defaultCollectionID, timestamp)
	time.Sleep(20 * time.Millisecond)
	resT := replica.getTSafe(defaultVChannel)
	assert.Equal(t, timestamp, resT)

	replica.removeTSafe(defaultVChannel)
}

func TestTSafeReplica_invalid(t *testing.T) {
	replica := newTSafeReplica()

	watcher := newTSafeWatcher()
	replica.registerTSafeWatcher(defaultVChannel, watcher)

	timestamp := Timestamp(1000)
	replica.setTSafe(defaultVChannel, defaultCollectionID, timestamp)
	time.Sleep(20 * time.Millisecond)
	resT := replica.getTSafe(defaultVChannel)
	assert.Equal(t, Timestamp(0), resT)

	replica.removeTSafe(defaultVChannel)

	replica.addTSafe(defaultVChannel)
	replica.addTSafe(defaultVChannel)
}
