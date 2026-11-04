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

package rootcoord

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestGaugeSeriesDeletesWhatARoundNoLongerSets(t *testing.T) {
	gauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "test_gauge_series"}, []string{"db_name", "collection_name"})
	series := newGaugeSeries(gauge)

	round := series.newRound()
	round.set(1, "db", "kept")
	round.set(2, "db", "dropped")
	round.complete()
	assert.Equal(t, 2, testutil.CollectAndCount(gauge))

	// The dropped collection is no longer reported.
	round = series.newRound()
	round.set(3, "db", "kept")
	round.complete()
	assert.Equal(t, 1, testutil.CollectAndCount(gauge))
	assert.Equal(t, float64(3), testutil.ToFloat64(gauge.WithLabelValues("db", "kept")))

	// A round that does not complete deletes nothing, and what it set is
	// deleted by the next round that completes without setting it.
	round = series.newRound()
	round.set(4, "db", "renamed")
	assert.Equal(t, 2, testutil.CollectAndCount(gauge))

	round = series.newRound()
	round.set(5, "db", "kept")
	round.complete()
	assert.Equal(t, 1, testutil.CollectAndCount(gauge))
	assert.Equal(t, float64(5), testutil.ToFloat64(gauge.WithLabelValues("db", "kept")))
}

func TestGaugeSeriesSharingAGaugeDeleteOnlyTheirOwnSeries(t *testing.T) {
	gauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "test_gauge_series_shared"}, []string{"collection_name", "status"})
	loaded, total := newGaugeSeries(gauge), newGaugeSeries(gauge)

	round := loaded.newRound()
	round.set(1, "coll", "loaded")
	round.complete()
	round = total.newRound()
	round.set(2, "coll", "total")
	round.complete()

	// Every collection released: no loaded series, the total one stays.
	loaded.newRound().complete()
	assert.Equal(t, 1, testutil.CollectAndCount(gauge))
	assert.Equal(t, float64(2), testutil.ToFloat64(gauge.WithLabelValues("coll", "total")))
}
