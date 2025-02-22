package indexparamcheck

import (
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
)

const (
	MinNBits     = 1
	MaxNBits     = 16
	DefaultNBits = 8

	// MinNList is the lower limit of nlist that used in Index IVFxxx
	MinNList = 1
	// MaxNList is the upper limit of nlist that used in Index IVFxxx
	MaxNList = 65536

	HNSWMinEfConstruction = 1
	HNSWMaxEfConstruction = 2147483647
	HNSWMinM              = 1
	HNSWMaxM              = 2048

	// DIM is a constant used to represent dimension
	DIM = common.DimKey
	// Metric is a constant used to metric type
	Metric = common.MetricTypeKey
	// NLIST is a constant used to nlist in Index IVFxxx
	NLIST = "nlist"
	NBITS = "nbits"
	IVFM  = "m"

	EFConstruction = "efConstruction"
	HNSWM          = "M"

	RaftCacheDatasetOnDevice = "cache_dataset_on_device"

	// Cagra Train Param
	CagraInterDegree = "intermediate_graph_degree"
	CagraGraphDegree = "graph_degree"
	CagraBuildAlgo   = "build_algo"

	CargaBuildAlgoIVFPQ     = "IVF_PQ"
	CargaBuildAlgoNNDESCENT = "NN_DESCENT"

	// Sparse Index Param
	SparseDropRatioBuild = "drop_ratio_build"

	BM25K1 = "bm25_k1"
	BM25B  = "bm25_b"

	MaxBitmapCardinalityLimit = 1000
)

var (
	FloatVectorMetrics  = []string{metric.L2, metric.IP, metric.COSINE}                                        // const
	BinaryVectorMetrics = []string{metric.HAMMING, metric.JACCARD, metric.SUBSTRUCTURE, metric.SUPERSTRUCTURE} // const
)

// BinIDMapMetrics is a set of all metric types supported for binary vector.
var (
	BinIDMapMetrics           = []string{metric.HAMMING, metric.JACCARD, metric.SUBSTRUCTURE, metric.SUPERSTRUCTURE} // const
	BinIvfMetrics             = []string{metric.HAMMING, metric.JACCARD}                                             // const
	HnswMetrics               = []string{metric.L2, metric.IP, metric.COSINE, metric.HAMMING, metric.JACCARD}        // const
	RaftMetrics               = []string{metric.L2, metric.IP}
	CagraBuildAlgoTypes       = []string{CargaBuildAlgoIVFPQ, CargaBuildAlgoNNDESCENT}
	supportDimPerSubQuantizer = []int{32, 28, 24, 20, 16, 12, 10, 8, 6, 4, 3, 2, 1}              // const
	supportSubQuantizer       = []int{96, 64, 56, 48, 40, 32, 28, 24, 20, 16, 12, 8, 4, 3, 2, 1} // const
	SparseMetrics             = []string{metric.IP, metric.BM25}                                 // const
)

const (
	FloatVectorDefaultMetricType       = metric.COSINE
	SparseFloatVectorDefaultMetricType = metric.IP
	BinaryVectorDefaultMetricType      = metric.HAMMING
)
