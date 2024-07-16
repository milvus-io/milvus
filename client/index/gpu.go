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

package index

import "strconv"

var _ Index = gpuBruteForceIndex{}

type gpuBruteForceIndex struct {
	baseIndex
}

func (idx gpuBruteForceIndex) Params() map[string]string {
	return map[string]string{
		// build meta
		MetricTypeKey: string(idx.metricType),
		IndexTypeKey:  string(GPUBruteForce),
	}
}

func NewGPUBruteForceIndex(metricType MetricType) Index {
	return gpuBruteForceIndex{
		baseIndex: baseIndex{
			metricType: metricType,
		},
	}
}

var _ Index = gpuIVFFlatIndex{}

type gpuIVFFlatIndex struct {
	baseIndex
	nlist int
}

func (idx gpuIVFFlatIndex) Params() map[string]string {
	return map[string]string{
		// build meta
		MetricTypeKey: string(idx.metricType),
		IndexTypeKey:  string(GPUIvfFlat),
		// build param
		ivfNlistKey: strconv.Itoa(idx.nlist),
	}
}

func NewGPUIVPFlatIndex(metricType MetricType) Index {
	return gpuIVFFlatIndex{
		baseIndex: baseIndex{
			metricType: metricType,
		},
	}
}

var _ Index = gpuIVFPQIndex{}

type gpuIVFPQIndex struct {
	baseIndex
	nlist int
	m     int
	nbits int
}

func (idx gpuIVFPQIndex) Params() map[string]string {
	return map[string]string{
		// build meta
		MetricTypeKey: string(idx.metricType),
		IndexTypeKey:  string(GPUIvfFlat),
		// build params
		ivfNlistKey: strconv.Itoa(idx.nlist),
		ivfPQMKey:   strconv.Itoa(idx.m),
		ivfPQNbits:  strconv.Itoa(idx.nbits),
	}
}

func NewGPUIVPPQIndex(metricType MetricType) Index {
	return gpuIVFPQIndex{
		baseIndex: baseIndex{
			metricType: metricType,
		},
	}
}

const (
	cagraInterGraphDegreeKey = `intermediate_graph_degree`
	cagraGraphDegreeKey      = `"graph_degree"`
)

type gpuCagra struct {
	baseIndex
	intermediateGraphDegree int
	graphDegree             int
}

func (idx gpuCagra) Params() map[string]string {
	return map[string]string{
		// build meta
		MetricTypeKey: string(idx.metricType),
		IndexTypeKey:  string(GPUIvfFlat),
		// build params
		cagraInterGraphDegreeKey: strconv.Itoa(idx.intermediateGraphDegree),
		cagraGraphDegreeKey:      strconv.Itoa(idx.graphDegree),
	}
}

func NewGPUCagraIndex(metricType MetricType,
	intermediateGraphDegree,
	graphDegree int,
) Index {
	return gpuCagra{
		baseIndex: baseIndex{
			metricType: metricType,
		},
		intermediateGraphDegree: intermediateGraphDegree,
		graphDegree:             graphDegree,
	}
}
