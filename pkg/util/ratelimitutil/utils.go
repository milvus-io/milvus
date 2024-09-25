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

package ratelimitutil

import "github.com/milvus-io/milvus-proto/go-api/v2/commonpb"

var QuotaErrorString = map[commonpb.ErrorCode]string{
	commonpb.ErrorCode_ForceDeny:            "access has been disabled by the administrator",
	commonpb.ErrorCode_MemoryQuotaExhausted: "memory quota exceeded, please allocate more resources",
	commonpb.ErrorCode_DiskQuotaExhausted:   "disk quota exceeded, please allocate more resources",
	commonpb.ErrorCode_TimeTickLongDelay:    "time tick long delay",
}

func GetQuotaErrorString(errCode commonpb.ErrorCode) string {
	return QuotaErrorString[errCode]
}
