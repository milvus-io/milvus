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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTSafe_GetAndSet(t *testing.T) {
	tSafe := newTSafe(context.Background(), "TestTSafe-channel")
	watcher := newTSafeWatcher()
	tSafe.registerTSafeWatcher(watcher)

	go func() {
		watcher.hasUpdate()
		timestamp := tSafe.get()
		assert.Equal(t, timestamp, Timestamp(1000))
	}()

	tSafe.set(UniqueID(1), Timestamp(1000))
}
