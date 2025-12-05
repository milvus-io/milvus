// Copyright (C) 2019-2025 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package hardware

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetJemallocStats(t *testing.T) {
	stats := GetJemallocStats()

	// The test should always succeed, whether jemalloc is available or not
	if stats.Available {
		// If jemalloc is available, verify the stats make sense
		assert.GreaterOrEqual(t, stats.Resident, stats.Allocated,
			"Resident memory should be >= allocated memory")
		assert.Equal(t, stats.Cached, stats.Resident-stats.Allocated,
			"Cached should equal resident - allocated")

		t.Logf("Jemalloc stats:")
		t.Logf("  Allocated: %d bytes (%.2f MB)", stats.Allocated, float64(stats.Allocated)/1024/1024)
		t.Logf("  Resident:  %d bytes (%.2f MB)", stats.Resident, float64(stats.Resident)/1024/1024)
		t.Logf("  Cached:    %d bytes (%.2f MB)", stats.Cached, float64(stats.Cached)/1024/1024)
	} else {
		// If jemalloc is not available, all values should be zero
		assert.Equal(t, uint64(0), stats.Allocated, "Allocated should be 0 when jemalloc is unavailable")
		assert.Equal(t, uint64(0), stats.Resident, "Resident should be 0 when jemalloc is unavailable")
		assert.Equal(t, uint64(0), stats.Cached, "Cached should be 0 when jemalloc is unavailable")

		t.Log("Jemalloc is not available on this system")
	}
}
