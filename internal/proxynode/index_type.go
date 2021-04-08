package proxynode

type IndexType = string

const (
	IndexFaissIDMap      IndexType = "FLAT"
	IndexFaissIvfFlat    IndexType = "IVF_FLAT"
	IndexFaissIvfPQ      IndexType = "IVF_PQ"
	IndexFaissIvfSQ8     IndexType = "IVF_SQ8"
	IndexFaissIvfSQ8H    IndexType = "IVF_SQ8_HYBRID"
	IndexFaissBinIDMap   IndexType = "BIN_FLAT"
	IndexFaissBinIvfFlat IndexType = "BIN_IVF_FLAT"
	IndexNSG             IndexType = "NSG"
	IndexHNSW            IndexType = "HNSW"
	IndexRHNSWFlat       IndexType = "RHNSW_FLAT"
	IndexRHNSWPQ         IndexType = "RHNSW_PQ"
	IndexRHNSWSQ         IndexType = "RHNSW_SQ"
	IndexANNOY           IndexType = "ANNOY"
	IndexNGTPANNG        IndexType = "NGT_PANNG"
	IndexNGTONNG         IndexType = "NGT_ONNG"
)
