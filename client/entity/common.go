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

package entity

import "github.com/milvus-io/milvus-proto/go-api/v2/commonpb"

// MetricType metric type
type MetricType string

// Metric Constants
const (
	L2             MetricType = "L2"
	IP             MetricType = "IP"
	COSINE         MetricType = "COSINE"
	HAMMING        MetricType = "HAMMING"
	JACCARD        MetricType = "JACCARD"
	TANIMOTO       MetricType = "TANIMOTO"
	SUBSTRUCTURE   MetricType = "SUBSTRUCTURE"
	SUPERSTRUCTURE MetricType = "SUPERSTRUCTURE"
	BM25           MetricType = "BM25"
	MHJACCARD      MetricType = "MHJACCARD"
)

// CompactionState enum type for compaction state
type CompactionState commonpb.CompactionState

// CompactionState Constants
const (
	CompactionStateRunning   CompactionState = CompactionState(commonpb.CompactionState_Executing)
	CompactionStateCompleted CompactionState = CompactionState(commonpb.CompactionState_Completed)
)
