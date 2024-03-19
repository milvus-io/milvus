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

package collector

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/ratelimitutil"
)

var Average *averageCollector

var Rate *ratelimitutil.RateCollector

var Counter *counter

func RateMetrics() []string {
	return []string{
		metricsinfo.NQPerSecond,
		metricsinfo.SearchThroughput,
		metricsinfo.InsertConsumeThroughput,
		metricsinfo.DeleteConsumeThroughput,
	}
}

func AverageMetrics() []string {
	return []string{
		metricsinfo.QueryQueueMetric,
		metricsinfo.SearchQueueMetric,
	}
}

func ConstructLabel(subs ...string) string {
	label := ""
	for id, sub := range subs {
		label += sub
		if id != len(subs)-1 {
			label += "-"
		}
	}
	return label
}

func init() {
	var err error
	Rate, err = ratelimitutil.NewRateCollector(ratelimitutil.DefaultWindow, ratelimitutil.DefaultGranularity, false)
	if err != nil {
		log.Fatal("failed to initialize querynode rate collector", zap.Error(err))
	}
	Average = newAverageCollector()
	Counter = newCounter()

	// init rate Metric
	for _, label := range RateMetrics() {
		Rate.Register(label)
	}
	// init average metric

	for _, label := range AverageMetrics() {
		Average.Register(label)
	}
}
