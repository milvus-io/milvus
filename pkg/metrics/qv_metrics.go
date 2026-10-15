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
	qvSubsystem = "qv"

	QVComponentLabel = "component"
	QVStateLabel     = "state"
	QVFromStateLabel = "from_state"
	QVToStateLabel   = "to_state"
	QVTriggerLabel   = "trigger"

	QVRankLabel             = "rank"
	QVCollectionIDLabel     = "collection_id"
	QVReplicaIDLabel        = "replica_id"
	QVVChannelLabel         = "vchannel"
	QVQueryViewVersionLabel = "query_view_version"
	QVDataVersionLabel      = "data_version"
)

type QVViewStateMaxAgeMetric struct {
	Component        string
	State            string
	Rank             string
	CollectionID     string
	ReplicaID        string
	VChannel         string
	QueryViewVersion string
	DataVersion      string
	AgeSeconds       float64
}

type qvViewStateMaxAgeCollector struct {
	mu       sync.RWMutex
	provider func() []QVViewStateMaxAgeMetric
	desc     *prometheus.Desc
}

func newQVViewStateMaxAgeCollector() *qvViewStateMaxAgeCollector {
	return &qvViewStateMaxAgeCollector{
		desc: prometheus.NewDesc(
			prometheus.BuildFQName(milvusNamespace, qvSubsystem, "view_state_max_age_seconds"),
			"top QueryViews by active state age in seconds",
			[]string{
				QVComponentLabel,
				QVStateLabel,
				QVRankLabel,
				QVCollectionIDLabel,
				QVReplicaIDLabel,
				QVVChannelLabel,
				QVQueryViewVersionLabel,
				QVDataVersionLabel,
			},
			nil,
		),
	}
}

func (c *qvViewStateMaxAgeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

func (c *qvViewStateMaxAgeCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	provider := c.provider
	c.mu.RUnlock()
	if provider == nil {
		return
	}
	for _, metric := range provider() {
		ch <- prometheus.MustNewConstMetric(
			c.desc,
			prometheus.GaugeValue,
			metric.AgeSeconds,
			metric.Component,
			metric.State,
			metric.Rank,
			metric.CollectionID,
			metric.ReplicaID,
			metric.VChannel,
			metric.QueryViewVersion,
			metric.DataVersion,
		)
	}
}

func (c *qvViewStateMaxAgeCollector) SetProvider(provider func() []QVViewStateMaxAgeMetric) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.provider = provider
}

var (
	QVViewStates = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: qvSubsystem,
			Name:      "view_states",
			Help:      "current number of QueryViews by state",
		}, []string{
			QVComponentLabel,
			QVStateLabel,
		})

	QVViewTransitionTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: milvusNamespace,
			Subsystem: qvSubsystem,
			Name:      "view_transition_total",
			Help:      "total number of QueryView state transitions",
		}, []string{
			QVComponentLabel,
			QVFromStateLabel,
			QVToStateLabel,
			QVTriggerLabel,
		})

	QVShardLoadStates = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: qvSubsystem,
			Name:      "shard_load_states",
			Help:      "current number of Coord-visible QueryView shards by load lifecycle state",
		}, []string{
			QVStateLabel,
		})

	QVViewStateMaxAgeSeconds = newQVViewStateMaxAgeCollector()
)

func SetQVViewStateMaxAgeProvider(provider func() []QVViewStateMaxAgeMetric) {
	QVViewStateMaxAgeSeconds.SetProvider(provider)
}

func RegisterQV(registry *prometheus.Registry) {
	registry.MustRegister(QVViewStates)
	registry.MustRegister(QVViewTransitionTotal)
	registry.MustRegister(QVShardLoadStates)
	registry.MustRegister(QVViewStateMaxAgeSeconds)
}
