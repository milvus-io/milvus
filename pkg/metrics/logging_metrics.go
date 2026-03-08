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

	LoggingPendingWriteTotal = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: milvusNamespace,
		Subsystem: loggingMetricSubsystem,
		Name:      "pending_write_total",
		Help:      "The length of pending writes in the logging buffer",
	})

	LoggingTruncatedWriteTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: milvusNamespace,
		Subsystem: loggingMetricSubsystem,
		Name:      "truncated_write_total",
		Help:      "The number of truncated writes due to exceeding the max bytes per log",
	})

	LoggingTruncatedWriteBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: milvusNamespace,
		Subsystem: loggingMetricSubsystem,
		Name:      "truncated_write_bytes",
		Help:      "The total bytes of truncated writes due to exceeding the max bytes per log",
	})

	LoggingDroppedWriteTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: milvusNamespace,
		Subsystem: loggingMetricSubsystem,
		Name:      "dropped_write_total",
		Help:      "The number of dropped writes due to buffer full or write timeout",
	})

	LoggingIOFailureTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: milvusNamespace,
		Subsystem: loggingMetricSubsystem,
		Name:      "io_failure_total",
		Help:      "The number of IO failures due to underlying write syncer is blocked or write timeout",
	})

	LoggingWriteTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: milvusNamespace,
		Subsystem: loggingMetricSubsystem,
		Name:      "write_total",
		Help:      "The total number of writes",
	})

	LoggingWriteBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: milvusNamespace,
		Subsystem: loggingMetricSubsystem,
		Name:      "write_bytes",
		Help:      "The total bytes of written logs",
	})

	LoggingCGOWriteTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: milvusNamespace,
		Subsystem: loggingMetricSubsystem,
		Name:      "cgo_write_total",
		Help:      "The total number of CGO writes",
	})

	LoggingCGOWriteBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: milvusNamespace,
		Subsystem: loggingMetricSubsystem,
		Name:      "cgo_write_bytes",
		Help:      "The total bytes of CGO write logs, the bytes is calculated before encoding, only considers the length of the message, so the actual bytes may be greater than the value",
	})
)

// RegisterLoggingMetrics registers logging metrics
func RegisterLoggingMetrics(registry *prometheus.Registry) {
	LoggingMetricsRegisterOnce.Do(func() {
		registry.MustRegister(LoggingPendingWriteTotal)
		registry.MustRegister(LoggingTruncatedWriteTotal)
		registry.MustRegister(LoggingTruncatedWriteBytes)
		registry.MustRegister(LoggingDroppedWriteTotal)
		registry.MustRegister(LoggingIOFailureTotal)
		registry.MustRegister(LoggingWriteTotal)
		registry.MustRegister(LoggingWriteBytes)
		registry.MustRegister(LoggingCGOWriteTotal)
		registry.MustRegister(LoggingCGOWriteBytes)
	})
}
