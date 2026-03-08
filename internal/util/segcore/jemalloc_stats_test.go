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

package segcore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetJemallocStats(t *testing.T) {
	stats := GetJemallocStats()

	// The test should always succeed, whether jemalloc is available or not
	if stats.Success {
		// If jemalloc is available, verify the stats make sense
		assert.GreaterOrEqual(t, stats.Active, stats.Allocated,
			"Active memory should be >= allocated memory (includes fragmentation)")
		assert.GreaterOrEqual(t, stats.Resident, stats.Active,
			"Resident memory should be >= active memory")
		assert.GreaterOrEqual(t, stats.Mapped, stats.Resident,
			"Mapped memory should be >= resident memory")

		// Verify derived metrics
		expectedFragmentation := uint64(0)
		if stats.Active > stats.Allocated {
			expectedFragmentation = stats.Active - stats.Allocated
		}
		assert.Equal(t, expectedFragmentation, stats.Fragmentation,
			"Fragmentation should equal active - allocated")

		expectedOverhead := uint64(0)
		if stats.Resident > stats.Active {
			expectedOverhead = stats.Resident - stats.Active
		}
		assert.Equal(t, expectedOverhead, stats.Overhead,
			"Overhead should equal resident - active")

		t.Logf("Jemalloc stats (all 8 metrics):")
		t.Logf("  Allocated:      %d bytes (%.2f MB)", stats.Allocated, float64(stats.Allocated)/1024/1024)
		t.Logf("  Active:         %d bytes (%.2f MB)", stats.Active, float64(stats.Active)/1024/1024)
		t.Logf("  Metadata:       %d bytes (%.2f MB)", stats.Metadata, float64(stats.Metadata)/1024/1024)
		t.Logf("  Resident:       %d bytes (%.2f MB)", stats.Resident, float64(stats.Resident)/1024/1024)
		t.Logf("  Mapped:         %d bytes (%.2f MB)", stats.Mapped, float64(stats.Mapped)/1024/1024)
		t.Logf("  Retained:       %d bytes (%.2f MB)", stats.Retained, float64(stats.Retained)/1024/1024)
		t.Logf("  Fragmentation:  %d bytes (%.2f MB)", stats.Fragmentation, float64(stats.Fragmentation)/1024/1024)
		t.Logf("  Overhead:       %d bytes (%.2f MB)", stats.Overhead, float64(stats.Overhead)/1024/1024)
	} else {
		// If jemalloc is not available, all values should be zero
		assert.Equal(t, uint64(0), stats.Allocated, "Allocated should be 0 when jemalloc is unavailable")
		assert.Equal(t, uint64(0), stats.Active, "Active should be 0 when jemalloc is unavailable")
		assert.Equal(t, uint64(0), stats.Metadata, "Metadata should be 0 when jemalloc is unavailable")
		assert.Equal(t, uint64(0), stats.Resident, "Resident should be 0 when jemalloc is unavailable")
		assert.Equal(t, uint64(0), stats.Mapped, "Mapped should be 0 when jemalloc is unavailable")
		assert.Equal(t, uint64(0), stats.Retained, "Retained should be 0 when jemalloc is unavailable")
		assert.Equal(t, uint64(0), stats.Fragmentation, "Fragmentation should be 0 when jemalloc is unavailable")
		assert.Equal(t, uint64(0), stats.Overhead, "Overhead should be 0 when jemalloc is unavailable")

		t.Log("Jemalloc is not available on this system (e.g., macOS)")
	}
}
