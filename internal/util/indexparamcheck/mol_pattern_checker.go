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

package indexparamcheck

import (
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	minMOLPatternNBits = 64
	maxMOLPatternNBits = 4096
)

// MOLPatternChecker checks if a MOL_PATTERN index can be built.
type MOLPatternChecker struct {
	scalarIndexChecker
}

func (c *MOLPatternChecker) CheckTrain(dataType schemapb.DataType, elementType schemapb.DataType, params map[string]string) error {
	if !typeutil.IsMolType(dataType) {
		return fmt.Errorf("MOL_PATTERN index can only be built on mol field")
	}

	if nBitStr, ok := params["n_bit"]; ok {
		nBit, err := strconv.ParseInt(nBitStr, 10, 32)
		if err != nil || nBit < minMOLPatternNBits || nBit > maxMOLPatternNBits {
			return fmt.Errorf("n_bit for MOL_PATTERN index must be an integer in [%d, %d], got %s", minMOLPatternNBits, maxMOLPatternNBits, nBitStr)
		}
	}

	return c.scalarIndexChecker.CheckTrain(dataType, elementType, params)
}

func (c *MOLPatternChecker) CheckValidDataType(indexType IndexType, field *schemapb.FieldSchema) error {
	dType := field.GetDataType()
	if !typeutil.IsMolType(dType) {
		return fmt.Errorf("MOL_PATTERN index can only be built on mol field, got %s", dType.String())
	}
	return nil
}

func newMOLPatternChecker() *MOLPatternChecker {
	return &MOLPatternChecker{}
}
