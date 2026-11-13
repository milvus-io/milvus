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

package delegator

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDelegatorUpdateVersion(t *testing.T) {
	sd := &shardDelegator{}
	sd.version.Store(10)
	require.False(t, sd.UpdateVersion(10))
	require.False(t, sd.UpdateVersion(9))
	require.True(t, sd.UpdateVersion(20))
	require.False(t, sd.UpdateVersion(15))
	require.Equal(t, int64(20), sd.Version())
}

func TestDelegatorUpdateVersionConcurrent(t *testing.T) {
	sd := &shardDelegator{}
	const maxVersion = 256
	start := make(chan struct{})
	done := make(chan struct{})
	var writers sync.WaitGroup
	for version := int64(1); version <= maxVersion; version++ {
		writers.Add(1)
		go func(version int64) {
			defer writers.Done()
			<-start
			sd.UpdateVersion(version)
			sd.UpdateVersion(version - 1)
		}(version)
	}
	var readers sync.WaitGroup
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			<-start
			previous := int64(0)
			for {
				current := sd.Version()
				if current < previous {
					t.Errorf("version regressed from %d to %d", previous, current)
					return
				}
				previous = current
				select {
				case <-done:
					return
				default:
				}
			}
		}()
	}
	close(start)
	writers.Wait()
	close(done)
	readers.Wait()
	require.Equal(t, int64(maxVersion), sd.Version())
}
