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

package index

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/datacoord/meta"
)

func TestNotifySegmentIndexBuild(t *testing.T) {
	meta.DrainBuildIndexChForTest()
	defer meta.DrainBuildIndexChForTest()

	meta.NotifySegmentIndexBuild(10, 20, 10)

	meta.AssertBuildIndexEvents(t, 10, 20, 10)
	meta.AssertNoBuildIndexEvent(t)
}

func TestNotifySegmentIndexBuildWaitsForConsumer(t *testing.T) {
	meta.SetBuildIndexConsumerForTest(t, make(chan struct{}))
	meta.FillBuildIndexChForTest(t)
	ch := meta.GetBuildIndexChSingleton()
	time.AfterFunc(20*time.Millisecond, func() { <-ch })

	meta.NotifySegmentIndexBuild(100)

	segIDs := meta.DrainBuildIndexChForTest()
	assert.Equal(t, UniqueID(100), segIDs[len(segIDs)-1], "the segment must wait for room, not be dropped")
}

func TestNotifySegmentIndexBuildDropsWithoutConsumer(t *testing.T) {
	stopped := make(chan struct{})
	close(stopped)
	for _, done := range []<-chan struct{}{nil, stopped} {
		meta.SetBuildIndexConsumerForTest(t, done)
		meta.FillBuildIndexChForTest(t)

		meta.NotifySegmentIndexBuild(100)

		assert.NotContains(t, meta.DrainBuildIndexChForTest(), UniqueID(100))
	}
}
