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

package segments

import (
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
)

// Helper functions for aggregating stats across results

func sumInt64Field(results []*segcorepb.RetrieveResults, getter func(*segcorepb.RetrieveResults) int64) int64 {
	var sum int64
	for _, r := range results {
		if r != nil {
			sum += getter(r)
		}
	}
	return sum
}

func anyFieldTrue(results []*segcorepb.RetrieveResults, getter func(*segcorepb.RetrieveResults) bool) bool {
	for _, r := range results {
		if r != nil && getter(r) {
			return true
		}
	}
	return false
}

func sumInt64FieldInternal(results []*internalpb.RetrieveResults, getter func(*internalpb.RetrieveResults) int64) int64 {
	var sum int64
	for _, r := range results {
		if r != nil {
			sum += getter(r)
		}
	}
	return sum
}

func anyFieldTrueInternal(results []*internalpb.RetrieveResults, getter func(*internalpb.RetrieveResults) bool) bool {
	for _, r := range results {
		if r != nil && getter(r) {
			return true
		}
	}
	return false
}
