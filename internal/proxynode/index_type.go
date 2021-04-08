package proxynode

type IndexType = string

const (
	IndexFaissIDMap      = "FLAT"
	IndexFaissIvfFlat    = "IVF_FLAT"
	IndexFaissIvfPQ      = "IVF_PQ"
	IndexFaissIvfSQ8     = "IVF_SQ8"
	IndexFaissIvfSQ8H    = "IVF_SQ8_HYBRID"
	IndexFaissBinIDMap   = "BIN_FLAT"
	IndexFaissBinIvfFlat = "BIN_IVF_FLAT"
	IndexNSG             = "NSG"
	IndexHNSW            = "HNSW"
	IndexRHNSWFlat       = "RHNSW_FLAT"
	IndexRHNSWPQ         = "RHNSW_PQ"
	IndexRHNSWSQ         = "RHNSW_SQ"
	IndexANNOY           = "ANNOY"
	IndexNGTPANNG        = "NGT_PANNG"
	IndexNGTONNG         = "NGT_ONNG"
)
