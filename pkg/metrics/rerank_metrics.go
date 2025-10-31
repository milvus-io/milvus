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
	"github.com/prometheus/client_golang/prometheus"
)

// Labels for reranker metrics
const (
	rerankerNameLabelName = "reranker_name"
	errorTypeLabelName    = "error_type"
)

var (
	// RerankLatency records the time spent evaluating rerankers (ms)
	RerankLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: milvusNamespace,
			Subsystem: "rerank",
			Name:      "latency",
			Help:      "latency of reranker execution in milliseconds",
			Buckets:   buckets,
		}, []string{roleNameLabelName, nodeIDLabelName, collectionName, rerankerNameLabelName},
	)

	// RerankResultCount records the number of results reranked
	RerankResultCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: "rerank",
			Name:      "result_count",
			Help:      "number of results processed by reranker",
		}, []string{roleNameLabelName, nodeIDLabelName, collectionName, rerankerNameLabelName},
	)

	// RerankErrors records errors during reranking
	RerankErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: "rerank",
			Name:      "errors_total",
			Help:      "count of errors during reranking",
		}, []string{roleNameLabelName, nodeIDLabelName, collectionName, rerankerNameLabelName, errorTypeLabelName},
	)
)

// RegisterRerank registers reranker metrics to the given registry
func RegisterRerank(registry *prometheus.Registry) {
	// Make registration idempotent across multiple callers (proxy, querynode, etc.).
	// If the collector has already been registered to this registry, reuse the existing one
	// to avoid panic caused by duplicate registrations.
	if err := registry.Register(RerankLatency); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			if existing, ok := are.ExistingCollector.(*prometheus.HistogramVec); ok {
				RerankLatency = existing
			}
		} else {
			panic(err)
		}
	}

	if err := registry.Register(RerankResultCount); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			if existing, ok := are.ExistingCollector.(*prometheus.CounterVec); ok {
				RerankResultCount = existing
			}
		} else {
			panic(err)
		}
	}

	if err := registry.Register(RerankErrors); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			if existing, ok := are.ExistingCollector.(*prometheus.CounterVec); ok {
				RerankErrors = existing
			}
		} else {
			panic(err)
		}
	}
}
