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

package metrics

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func resetFeatureReportForTest(t *testing.T) {
	t.Helper()

	oldInterval := featureReportInterval
	FeatureReportTotal.Reset()
	resetFeatureReporters()

	t.Cleanup(func() {
		FeatureReportTotal.Reset()
		resetFeatureReporters()
		featureReportInterval = oldInterval
	})
}

func resetFeatureReporters() {
	for _, reporter := range []*FeatureReporter{
		FeatureHybridSearch,
		FeaturePartitionKey,
		FeatureDynamicField,
		FeatureBM25Function,
		FeatureResourceGroup,
		FeatureBulkImport,
	} {
		reporter.nextAllowedNanos.Store(0)
	}
}

func TestFeatureReporterThrottle(t *testing.T) {
	resetFeatureReportForTest(t)

	now := time.Unix(100, 0)

	require.True(t, FeatureHybridSearch.recordAt(now))
	assert.False(t, FeatureHybridSearch.recordAt(now.Add(time.Minute)))
	assert.True(t, FeatureHybridSearch.recordAt(now.Add(time.Hour)))

	assert.Equal(t, 2.0, testutil.ToFloat64(
		FeatureReportTotal.WithLabelValues(FeatureHybridSearch.Name(), featureReportSourceGo),
	))
}

func TestFeatureReporterNilReporter(t *testing.T) {
	resetFeatureReportForTest(t)

	var reporter *FeatureReporter
	assert.False(t, reporter.Record())
	assert.Equal(t, 0, testutil.CollectAndCount(FeatureReportTotal))
}

func TestFeatureReporterConcurrentCalls(t *testing.T) {
	resetFeatureReportForTest(t)

	now := time.Unix(100, 0)
	const goroutines = 32

	var reported atomic.Int64
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			if FeatureBulkImport.recordAt(now) {
				reported.Add(1)
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, int64(1), reported.Load())
	assert.Equal(t, 1.0, testutil.ToFloat64(
		FeatureReportTotal.WithLabelValues(FeatureBulkImport.Name(), featureReportSourceGo),
	))
}
