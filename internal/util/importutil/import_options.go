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

package importutil

import (
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

// Extra option keys to pass through import API
const (
	Bucket  = "bucket"   // the source files' minio bucket
	StartTs = "start_ts" // start timestamp to filter data, only data between StartTs and EndTs will be imported
	EndTs   = "end_ts"   // end timestamp to filter data, only data between StartTs and EndTs will be imported
)

// ValidateOptions the options is illegal
func ValidateOptions(options []*commonpb.KeyValuePair) error {
	optionMap := funcutil.KeyValuePair2Map(options)
	// StartTs should be int
	_, ok := optionMap[StartTs]
	if ok {
		_, err := strconv.ParseUint(optionMap[StartTs], 10, 64)
		if err != nil {
			return err
		}
	}
	// EndTs should be int
	_, ok = optionMap[EndTs]
	if ok {
		_, err := strconv.ParseUint(optionMap[EndTs], 10, 64)
		if err != nil {
			return err
		}
	}
	return nil
}
