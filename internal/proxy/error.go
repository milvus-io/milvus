// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package proxy

import (
	"errors"
	"fmt"

	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

// TODO(dragondriver): add more common error type

func errInvalidNumRows(numRows uint32) error {
	return fmt.Errorf("invalid num_rows: %d", numRows)
}

func errNumRowsLessThanOrEqualToZero(numRows uint32) error {
	return fmt.Errorf("num_rows(%d) should be greater than 0", numRows)
}

func errNumRowsOfFieldDataMismatchPassed(idx int, fieldNumRows, passedNumRows uint32) error {
	return fmt.Errorf("the num_rows(%d) of %dth field is not equal to passed NumRows(%d)", fieldNumRows, idx, passedNumRows)
}

var errEmptyFieldData = errors.New("empty field data")

func errFieldsLessThanNeeded(fieldsNum, needed int) error {
	return fmt.Errorf("the length(%d) of passed fields is less than needed(%d)", fieldsNum, needed)
}

func errUnsupportedDataType(dType schemapb.DataType) error {
	return fmt.Errorf("%v is not supported now", dType)
}

func errUnsupportedDType(dType string) error {
	return fmt.Errorf("%s is not supported now", dType)
}

func errInvalidDim(dim int) error {
	return fmt.Errorf("invalid dim: %d", dim)
}

func errDimLessThanOrEqualToZero(dim int) error {
	return fmt.Errorf("dim(%d) should be greater than 0", dim)
}

func errDimShouldDivide8(dim int) error {
	return fmt.Errorf("dim(%d) should divide 8", dim)
}

func msgProxyIsUnhealthy(id UniqueID) string {
	return fmt.Sprintf("proxy %d is unhealthy", id)
}

// errProxyIsUnhealthy returns an error represent proxy is unhealthy
func errProxyIsUnhealthy(id UniqueID) error {
	return errors.New(msgProxyIsUnhealthy(id))
}
