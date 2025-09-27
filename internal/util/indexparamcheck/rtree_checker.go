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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// RTREEChecker checks if a RTREE index can be built.
type RTREEChecker struct {
	scalarIndexChecker
}

func (c *RTREEChecker) CheckTrain(dataType schemapb.DataType, elementType schemapb.DataType, params map[string]string) error {
	if !typeutil.IsGeometryType(dataType) {
		return fmt.Errorf("RTREE index can only be built on geometry field")
	}

	return c.scalarIndexChecker.CheckTrain(dataType, elementType, params)
}

func (c *RTREEChecker) CheckValidDataType(indexType IndexType, field *schemapb.FieldSchema) error {
	dType := field.GetDataType()
	if !typeutil.IsGeometryType(dType) {
		return fmt.Errorf("RTREE index can only be built on geometry field, got %s", dType.String())
	}
	return nil
}

func newRTREEChecker() *RTREEChecker {
	return &RTREEChecker{}
}
