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

package proxy

import (
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

// Keep this error temporarily
// this error belongs to ErrServiceMemoryLimitExceeded
// but in the error returned by querycoord,the collection id is given
// which can not be thrown out
// the error will be deleted after reaching an agreement on collection name and id in qn

// ErrInsufficientMemory returns insufficient memory error.
var ErrInsufficientMemory = errors.New("InsufficientMemoryToLoad")

// InSufficientMemoryStatus returns insufficient memory status.
func InSufficientMemoryStatus(collectionName string) *commonpb.Status {
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_InsufficientMemoryToLoad,
		Reason:    fmt.Sprintf("deny to load, insufficient memory, please allocate more resources, collectionName: %s", collectionName),
	}
}
