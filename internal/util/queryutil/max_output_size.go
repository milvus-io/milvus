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

package queryutil

import "github.com/milvus-io/milvus/pkg/v3/util/merr"

// NewQueryResultSizeLimitExceededError reports the estimated Query result
// byte size and the configured maximum without conflating this guard with the
// request's row limit.
func NewQueryResultSizeLimitExceededError(estimatedOutputSize, maxOutputSize int64) error {
	return merr.WrapErrParameterInvalidMsg(
		"Query result exceeds the byte-size limit (estimated: %d bytes, maximum: %d bytes). "+
			"Reduce output fields or row limit, paginate, fetch large fields separately, or raise "+
			"quotaAndLimits.limits.maxOutputSize only after checking memory impact",
		estimatedOutputSize,
		maxOutputSize,
	)
}
