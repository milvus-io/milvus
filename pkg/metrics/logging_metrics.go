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

	"github.com/prometheus/client_golang/prometheus"
)

const (
	loggingMetricSubsystem = "logging"
)

var (
	LoggingMetricsRegisterOnce sync.Once

	LoggingPendingWriteLength = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: milvusNamespace,
		Subsystem: loggingMetricSubsystem,
		Name:      "pending_write_length",
		Help:      "The length of pending writes in the logging buffer",
	})

	LoggingPendingWriteBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: milvusNamespace,
		Subsystem: loggingMetricSubsystem,
		Name:      "pending_write_bytes",
		Help:      "The total bytes of pending writes in the logging buffer",
	})

	LoggingDroppedWrites = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: milvusNamespace,
		Subsystem: loggingMetricSubsystem,
		Name:      "dropped_writes",
		Help:      "The number of dropped writes due to buffer full or write timeout",
	})

	LoggingIOFailure = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: milvusNamespace,
		Subsystem: loggingMetricSubsystem,
		Name:      "io_failures",
		Help:      "The number of IO failures due to underlying write syncer is blocked or write timeout",
	})
)

// RegisterLoggingMetrics registers logging metrics
func RegisterLoggingMetrics(registry *prometheus.Registry) {
	LoggingMetricsRegisterOnce.Do(func() {
		registry.MustRegister(LoggingPendingWriteLength)
		registry.MustRegister(LoggingPendingWriteBytes)
		registry.MustRegister(LoggingDroppedWrites)
		registry.MustRegister(LoggingIOFailure)
	})
}
