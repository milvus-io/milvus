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

package util

import (
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/ratelimitutil"
)

// rateCol is global RateCollector in DataNode.
var (
	rateCol  *RateCollector
	initOnce sync.Once
)

// RateCollector helps to collect and calculate values (like rate, timeTick and etc...).
type RateCollector struct {
	*ratelimitutil.RateCollector

	flowGraphTtMu sync.Mutex
}

func initGlobalRateCollector() {
	initOnce.Do(func() {
		var err error
		rateCol, err = newRateCollector()
		if err != nil {
			log.Warn("DataNode server init rateCollector failed", zap.Error(err))
			panic(err)
		}
		rateCol.Register(metricsinfo.InsertConsumeThroughput)
		rateCol.Register(metricsinfo.DeleteConsumeThroughput)
	})
}

func DeregisterRateCollector(label string) {
	rateCol.Deregister(label)
}

func RegisterRateCollector(label string) {
	rateCol.Register(label)
}

func GetRateCollector() *RateCollector {
	initGlobalRateCollector()
	return rateCol
}

// newRateCollector returns a new RateCollector.
func newRateCollector() (*RateCollector, error) {
	rc, err := ratelimitutil.NewRateCollector(ratelimitutil.DefaultWindow, ratelimitutil.DefaultGranularity, false)
	if err != nil {
		return nil, err
	}
	return &RateCollector{
		RateCollector: rc,
	}, nil
}
