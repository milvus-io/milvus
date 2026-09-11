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

package featureusage

// bucketSpec is a fixed set of buckets for one GroupDist quantity. Buckets
// are defined in code, not derived from data, so the report has the same
// shape on every instance. upper[i] is the inclusive upper bound of
// labels[i]; labels has one more element than upper for the open-ended top
// bucket.
type bucketSpec struct {
	name   string
	upper  []int64
	labels []string
}

func (b bucketSpec) bucket(v int64) string {
	for i, u := range b.upper {
		if v <= u {
			return b.labels[i]
		}
	}
	return b.labels[len(b.labels)-1]
}

// Names of the GroupDist quantities.
const (
	DistPartitionCount = "partition_count"
	DistShardsNum      = "shards_num"
	DistDim            = "dim"
	DistMaxLength      = "max_length"
	DistMaxCapacity    = "max_capacity"
	DistReplicaNumber  = "replica_number"
)

var (
	partitionCountBuckets = bucketSpec{
		name:   DistPartitionCount,
		upper:  []int64{1, 16, 64, 1024},
		labels: []string{"1", "2-16", "17-64", "65-1024", ">1024"},
	}
	shardsNumBuckets = bucketSpec{
		name:   DistShardsNum,
		upper:  []int64{1, 2, 8},
		labels: []string{"1", "2", "3-8", ">8"},
	}
	dimBuckets = bucketSpec{
		name:   DistDim,
		upper:  []int64{128, 512, 1024, 2048},
		labels: []string{"<=128", "129-512", "513-1024", "1025-2048", ">2048"},
	}
	maxLengthBuckets = bucketSpec{
		name:   DistMaxLength,
		upper:  []int64{256, 4096, 65535},
		labels: []string{"<=256", "257-4096", "4097-65535", ">65535"},
	}
	maxCapacityBuckets = bucketSpec{
		name:   DistMaxCapacity,
		upper:  []int64{64, 1024},
		labels: []string{"<=64", "65-1024", ">1024"},
	}
	replicaNumberBuckets = bucketSpec{
		name:   DistReplicaNumber,
		upper:  []int64{1, 2},
		labels: []string{"1", "2", "3+"},
	}
)
