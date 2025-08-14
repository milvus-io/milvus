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

package metric

// MetricType string.
type MetricType = string

// MetricType definitions
const (
	// L2 represents Euclidean distance
	L2 MetricType = "L2"

	// IP represents inner product distance
	IP MetricType = "IP"

	// COSINE represents cosine distance
	COSINE MetricType = "COSINE"

	// HAMMING represents hamming distance
	HAMMING MetricType = "HAMMING"

	// JACCARD represents jaccard distance
	JACCARD MetricType = "JACCARD"

	// MHJACCARD represents jaccard distance of minhash vector
	MHJACCARD MetricType = "MHJACCARD"

	// SUBSTRUCTURE represents substructure distance
	SUBSTRUCTURE MetricType = "SUBSTRUCTURE"

	// SUPERSTRUCTURE represents superstructure distance
	SUPERSTRUCTURE MetricType = "SUPERSTRUCTURE"

	BM25 MetricType = "BM25"

	EMPTY MetricType = ""
)
