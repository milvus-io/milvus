// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package indexparamcheck

// IndexType string.
type IndexType = string

// IndexType definitions
const (
	IndexFaissIDMap      IndexType = "FLAT" // no index is built.
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
	IndexDISKANN         IndexType = "DISKANN"
)
