/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func NewMilvusRegistry() *MilvusRegistry {
	r := &MilvusRegistry{
		GoRegistry: prometheus.NewRegistry(),
		CRegistry:  NewCRegistry(),
	}

	r.GoRegistry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	r.GoRegistry.MustRegister(prometheus.NewGoCollector())

	return r
}

// re-write the implementation of Gather()
type MilvusRegistry struct {
	GoRegistry *prometheus.Registry
	CRegistry  *CRegistry
}

// Gather implements Gatherer.
func (r *MilvusRegistry) Gather() ([]*dto.MetricFamily, error) {
	var res []*dto.MetricFamily
	resGo, err := r.GoRegistry.Gather()
	if err != nil {
		return res, err
	}
	resC, err := r.CRegistry.Gather()
	if err != nil {
		// if gather c metrics fail, ignore the error and return go metrics
		return resGo, nil
	}
	res = append(resGo, resC...)
	return res, nil
}
