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

package indexcoord

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/api/schemapb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/util"
)

// getDimension gets the dimension of data from building index request.
func getDimension(req *indexpb.CreateIndexRequest) (int64, error) {
	for _, kvPair := range req.GetTypeParams() {
		key, value := kvPair.GetKey(), kvPair.GetValue()
		if key == "dim" {
			dim, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				errMsg := "dimension is invalid"
				log.Error(errMsg)
				return 0, errors.New(errMsg)
			}
			return dim, nil
		}
	}
	errMsg := "dimension is not in type params"
	log.Error(errMsg)
	return 0, errors.New(errMsg)
}

// estimateIndexSize estimates how much memory will be occupied by IndexNode when building an index.
func estimateIndexSize(dim int64, numRows int64, dataType schemapb.DataType) (uint64, error) {
	if dataType == schemapb.DataType_FloatVector {
		return uint64(dim) * uint64(numRows) * 4, nil
	}

	if dataType == schemapb.DataType_BinaryVector {
		return uint64(dim) / 8 * uint64(numRows), nil
	}

	// TODO: optimize here.
	return 0, nil
}

func parseBuildIDFromFilePath(key string) (UniqueID, error) {
	ss := strings.Split(key, "/")
	if strings.HasSuffix(key, "/") {
		return strconv.ParseInt(ss[len(ss)-2], 10, 64)
	}
	return strconv.ParseInt(ss[len(ss)-1], 10, 64)
}

func buildHandoffKey(collID, partID, segID UniqueID) string {
	return fmt.Sprintf("%s/%d/%d/%d", util.HandoffSegmentPrefix, collID, partID, segID)
}
