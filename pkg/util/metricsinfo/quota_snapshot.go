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

package metricsinfo

import "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"

// CollectionQuotaSnapshot is an immutable, process-local absolute value.
// Generation is a cache publication number, unrelated to SetRates versions.
type CollectionQuotaSnapshot struct {
	Generation        uint64
	Present           bool
	HasQuota          bool
	HasRows           bool
	HasL0             bool
	Bytes             int64
	Rows              int64
	L0Rows            int64
	L0DeltalogFiles   int64
	PartitionBytes    map[int64]int64
	Display           []QuotaStateStats
	DeltalogQuantiles map[string]QuotaDeltalogQuantiles
}

// QuotaDeltalogQuantiles holds the deltalog gauge values for one segment level.
type QuotaDeltalogQuantiles struct {
	FileCount, Size, DeletedRatio []float64
}

type QuotaStateStats struct {
	State commonpb.SegmentState
	Bytes int64
	Rows  int64
	Files int64
}

// QuotaSnapshot contains a full baseline or unacknowledged absolute changes.
// Readers must not mutate Collections or any nested maps/slices.
type QuotaSnapshot struct {
	// Full resets the consumer baseline after initialization or a cache rollback.
	Full               bool
	BaselineGeneration uint64
	Revision           uint64
	TotalBytes         int64
	Collections        map[int64]*CollectionQuotaSnapshot
	ScannedSegments    int
}
