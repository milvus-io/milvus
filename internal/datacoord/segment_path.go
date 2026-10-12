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

package datacoord

import (
	"fmt"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/common"
)

// segmentBaseMatches checks only the segment identity, allowing legacy prefixes.
// Callers own backend-specific normalization and root containment.
func segmentBaseMatches(base string, collectionID, partitionID, segmentID int64) bool {
	suffix := fmt.Sprintf("%s/%d/%d/%d", common.SegmentInsertLogPath, collectionID, partitionID, segmentID)
	return base == suffix || strings.HasSuffix(base, "/"+suffix)
}
