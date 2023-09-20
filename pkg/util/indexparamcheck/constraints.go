package indexparamcheck

import (
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/metric"
)

const (
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

	HNSWMinEfConstruction = 8
	HNSWMaxEfConstruction = 512
	HNSWMinM              = 4
	HNSWMaxM              = 64

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
)

// METRICS is a set of all metrics types supported for float vector.
var METRICS = []string{metric.L2, metric.IP, metric.COSINE} // const

// BinIDMapMetrics is a set of all metric types supported for binary vector.
var (
	BinIDMapMetrics           = []string{metric.HAMMING, metric.JACCARD, metric.SUBSTRUCTURE, metric.SUPERSTRUCTURE} // const
	BinIvfMetrics             = []string{metric.HAMMING, metric.JACCARD}                                             // const
	HnswMetrics               = []string{metric.L2, metric.IP, metric.COSINE, metric.HAMMING, metric.JACCARD}        // const
	supportDimPerSubQuantizer = []int{32, 28, 24, 20, 16, 12, 10, 8, 6, 4, 3, 2, 1}                                  // const
	supportSubQuantizer       = []int{96, 64, 56, 48, 40, 32, 28, 24, 20, 16, 12, 8, 4, 3, 2, 1}                     // const
)

const (
	FloatVectorDefaultMetricType  = metric.IP
	BinaryVectorDefaultMetricType = metric.JACCARD
)
