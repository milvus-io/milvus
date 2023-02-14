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

package indexparamcheck

import (
	"fmt"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/util/indexparams"
)

const (
	// L2 represents Euclidean distance
	L2 = "L2"

	// IP represents inner product distance
	IP = "IP"

	// HAMMING represents hamming distance
	HAMMING = "HAMMING"

	// JACCARD represents jaccard distance
	JACCARD = "JACCARD"

	// TANIMOTO represents tanimoto distance
	TANIMOTO = "TANIMOTO"

	// SUBSTRUCTURE represents substructure distance
	SUBSTRUCTURE = "SUBSTRUCTURE"

	// SUPERSTRUCTURE represents superstructure distance
	SUPERSTRUCTURE = "SUPERSTRUCTURE"

	MinNBits     = 1
	MaxNBits     = 16
	DefaultNBits = 8

	// MinNList is the lower limit of nlist that used in Index IVFxxx
	MinNList = 1
	// MaxNList is the upper limit of nlist that used in Index IVFxxx
	MaxNList = 65536

	// DefaultMinDim is the smallest dimension supported in Milvus
	DefaultMinDim = 1
	// DefaultMaxDim is the largest dimension supported in Milvus
	DefaultMaxDim = 32768

	// If Dim = 32 and raw vector data = 2G, query node need 24G disk space When loading the vectors' disk index
	// If Dim = 2, and raw vector data = 2G, query node need 240G disk space When loading the vectors' disk index
	// So DiskAnnMinDim should be greater than or equal to 32 to avoid running out of disk space
	DiskAnnMinDim = 32

	NgtMinEdgeSize = 1
	NgtMaxEdgeSize = 200

	HNSWMinEfConstruction = 8
	HNSWMaxEfConstruction = 512
	HNSWMinM              = 4
	HNSWMaxM              = 64

	MinKNNG              = 5
	MaxKNNG              = 300
	MinSearchLength      = 10
	MaxSearchLength      = 300
	MinOutDegree         = 5
	MaxOutDegree         = 300
	MinCandidatePoolSize = 50
	MaxCandidatePoolSize = 1000

	MinNTrees = 1
	// too large of n_trees takes much time, if there is real requirement, change this threshold.
	MaxNTrees = 1024

	// DIM is a constant used to represent dimension
	DIM = "dim"
	// Metric is a constant used to metric type
	Metric = "metric_type"
	// NLIST is a constant used to nlist in Index IVFxxx
	NLIST = "nlist"
	NBITS = "nbits"
	IVFM  = "m"

	KNNG         = "knng"
	SearchLength = "search_length"
	OutDegree    = "out_degree"
	CANDIDATE    = "candidate_pool_size"

	EFConstruction = "efConstruction"
	HNSWM          = "M"

	PQM    = "PQM"
	NTREES = "n_trees"

	EdgeSize                  = "edge_size"
	ForcedlyPrunedEdgeSize    = "forcedly_pruned_edge_size"
	SelectivelyPrunedEdgeSize = "selectively_pruned_edge_size"

	OutgoingEdgeSize = "outgoing_edge_size"
	IncomingEdgeSize = "incoming_edge_size"

	IndexMode = "index_mode"
	CPUMode   = "CPU"
	GPUMode   = "GPU"
)

// METRICS is a set of all metrics types supported for float vector.
var METRICS = []string{L2, IP} // const

// BinIDMapMetrics is a set of all metric types supported for binary vector.
var BinIDMapMetrics = []string{HAMMING, JACCARD, TANIMOTO, SUBSTRUCTURE, SUPERSTRUCTURE}   // const
var BinIvfMetrics = []string{HAMMING, JACCARD, TANIMOTO}                                   // const
var supportDimPerSubQuantizer = []int{32, 28, 24, 20, 16, 12, 10, 8, 6, 4, 3, 2, 1}        // const
var supportSubQuantizer = []int{96, 64, 56, 48, 40, 32, 28, 24, 20, 16, 12, 8, 4, 3, 2, 1} // const

var (
	FLATParams    = map[string]struct{}{common.IndexTypeKey: {}, common.MetricTypeKey: {}, common.DimKey: {}}
	IVFFLATParams = map[string]struct{}{common.IndexTypeKey: {}, common.MetricTypeKey: {}, common.DimKey: {}, NLIST: {}}
	IVFSQ8Params  = map[string]struct{}{common.IndexTypeKey: {}, common.MetricTypeKey: {}, common.DimKey: {}, NLIST: {},
		NBITS: {}}
	IVFPQParams = map[string]struct{}{common.IndexTypeKey: {}, common.MetricTypeKey: {}, common.DimKey: {}, NLIST: {},
		IVFM: {}, NBITS: {}}
	HNSWParams = map[string]struct{}{common.IndexTypeKey: {}, common.MetricTypeKey: {}, common.DimKey: {},
		HNSWM: {}, EFConstruction: {}}
	ANNOYParams = map[string]struct{}{common.IndexTypeKey: {}, common.MetricTypeKey: {}, common.DimKey: {},
		NTREES: {}}
	BINFLATParams    = map[string]struct{}{common.IndexTypeKey: {}, common.MetricTypeKey: {}, common.DimKey: {}}
	BINIVFFLATParams = map[string]struct{}{common.IndexTypeKey: {}, common.MetricTypeKey: {}, common.DimKey: {}, NLIST: {}}
	DISKANNParams    = map[string]struct{}{common.IndexTypeKey: {}, common.MetricTypeKey: {}, common.DimKey: {}, indexparams.MaxDegreeKey: {},
		indexparams.SearchListSizeKey: {}, indexparams.PQCodeBudgetRatioKey: {}, indexparams.NumBuildThreadRatioKey: {},
		indexparams.SearchCacheBudgetRatioKey: {}, indexparams.NumLoadThreadRatioKey: {}, indexparams.BeamWidthRatioKey: {}}
	NSGParams = map[string]struct{}{common.IndexTypeKey: {}, common.MetricTypeKey: {}, common.DimKey: {}, KNNG: {},
		SearchLength: {}, OutDegree: {}, CANDIDATE: {}}
	NGTPANNGParams = map[string]struct{}{common.IndexTypeKey: {}, common.MetricTypeKey: {}, common.DimKey: {}, EdgeSize: {},
		ForcedlyPrunedEdgeSize: {}, SelectivelyPrunedEdgeSize: {}}
	NGTONNGParams = map[string]struct{}{common.IndexTypeKey: {}, common.MetricTypeKey: {}, common.DimKey: {}, EdgeSize: {},
		OutgoingEdgeSize: {}, IncomingEdgeSize: {}}
	RHNSWPQParams = map[string]struct{}{common.IndexTypeKey: {}, common.MetricTypeKey: {}, common.DimKey: {},
		HNSWM: {}, EFConstruction: {}, PQM: {}}
	ScalarIndexParams = map[string]struct{}{common.IndexTypeKey: {}}
)

func checkUnnecessaryParams(expected map[string]struct{}, actual map[string]string) error {
	for key := range actual {
		if _, ok := expected[key]; !ok {
			return fmt.Errorf("invalid index param: %s", key)
		}
	}

	return nil
}
