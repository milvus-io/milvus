package indexparamcheck

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

const (
	FloatVectorDefaultMetricType  = IP
	BinaryVectorDefaultMetricType = JACCARD
)
