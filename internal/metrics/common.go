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
package metrics

import (
	"strconv"
	"time"

	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	// metric namespace
	milvusNamespace = "milvus"

	// status labels
	LabelSuccess = "success"
	LabelFail    = "fail"
	LabelTotal   = "total"

	// observe types
	ObserveInSeconds      = 0
	ObserveInMilliSeconds = 1
	ObserveInNanoSeconds  = 2
)

var ObserveType = ObserveInMilliSeconds

func GetCollectionLabel(collectionID typeutil.UniqueID) string {
	return "collection_" + strconv.FormatInt(collectionID, 10)
}

func GetPartitionLabel(partitionID typeutil.UniqueID) string {
	return "partition_" + strconv.FormatInt(partitionID, 10)
}

func GetSegmentLabel(partitionID typeutil.UniqueID) string {
	return "segment_" + strconv.FormatInt(partitionID, 10)
}

func Observe(duration time.Duration) float64 {
	switch ObserveType {
	case ObserveInSeconds:
		return duration.Seconds()
	case ObserveInMilliSeconds:
		return float64(duration.Milliseconds())
	case ObserveInNanoSeconds:
		return float64(duration.Nanoseconds())
	}
	return 0
}
