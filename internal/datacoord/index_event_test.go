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

package datacoord

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func drainBuildIndexChForTest() {
	ch := getBuildIndexChSingleton()
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func drainBuildIndexOverflowChForTest() {
	ch := getBuildIndexOverflowChSingleton()
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func drainStatsTaskChForTest() {
	ch := getStatsTaskChSingleton()
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func assertBuildIndexEvents(t *testing.T, expected ...UniqueID) {
	t.Helper()
	got := make([]UniqueID, 0, len(expected))
	for range expected {
		select {
		case segID := <-getBuildIndexChSingleton():
			got = append(got, segID)
		case <-time.After(100 * time.Millisecond):
			require.Failf(t, "missing build index event", "got %v, expected %v", got, expected)
		}
	}
	assert.ElementsMatch(t, expected, got)
}

func assertNoBuildIndexEvent(t *testing.T) {
	t.Helper()
	select {
	case segID := <-getBuildIndexChSingleton():
		require.Failf(t, "unexpected build index event", "segmentID=%d", segID)
	default:
	}
}

func assertNoStatsTaskEvent(t *testing.T) {
	t.Helper()
	select {
	case segID := <-getStatsTaskChSingleton():
		require.Failf(t, "unexpected stats task event", "segmentID=%d", segID)
	default:
	}
}

func TestNotifySegmentIndexBuild(t *testing.T) {
	drainBuildIndexChForTest()
	defer drainBuildIndexChForTest()
	drainBuildIndexOverflowChForTest()
	defer drainBuildIndexOverflowChForTest()

	notifySegmentIndexBuild(10, 20, 10)

	assertBuildIndexEvents(t, 10, 20, 10)
	assertNoBuildIndexEvent(t)
}

func TestNotifySegmentIndexBuildOverflow(t *testing.T) {
	drainBuildIndexChForTest()
	defer drainBuildIndexChForTest()
	drainBuildIndexOverflowChForTest()
	defer drainBuildIndexOverflowChForTest()

	ch := getBuildIndexChSingleton()
	for idx := 0; idx < cap(ch); idx++ {
		ch <- UniqueID(idx)
	}

	notifySegmentIndexBuild(100)

	select {
	case <-getBuildIndexOverflowChSingleton():
	case <-time.After(100 * time.Millisecond):
		require.Fail(t, "missing build index overflow event")
	}
}
