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

package querynode

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTSafeReplica(t *testing.T) {
	t.Run("test valid", func(t *testing.T) {
		replica := newTSafeReplica()
		replica.addTSafe(defaultVChannel)
		watcher := newTSafeWatcher()
		assert.NotNil(t, watcher)

		err := replica.registerTSafeWatcher(defaultVChannel, watcher)
		assert.NoError(t, err)

		timestamp := Timestamp(1000)
		err = replica.setTSafe(defaultVChannel, timestamp)
		assert.NoError(t, err)

		resT, err := replica.getTSafe(defaultVChannel)
		assert.NoError(t, err)
		assert.Equal(t, timestamp, resT)

		replica.removeTSafe(defaultVChannel)
		_, err = replica.getTSafe(defaultVChannel)
		assert.Error(t, err)
	})

	t.Run("test invalid", func(t *testing.T) {
		replica := newTSafeReplica()

		err := replica.registerTSafeWatcher(defaultVChannel, nil)
		assert.Error(t, err)

		_, err = replica.getTSafe(defaultVChannel)
		assert.Error(t, err)

		err = replica.setTSafe(defaultVChannel, Timestamp(1000))
		assert.Error(t, err)
	})
}
