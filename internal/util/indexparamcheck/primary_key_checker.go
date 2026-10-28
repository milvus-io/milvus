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
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// primaryKeyChecker checks if a primary key index can be created.
type primaryKeyChecker struct {
	scalarIndexChecker
}

// CheckValidDataType accepts only the primary key field, without autoID,
// of type Int64 or VarChar.
func (c *primaryKeyChecker) CheckValidDataType(indexType IndexType, field *schemapb.FieldSchema) error {
	if !field.GetIsPrimaryKey() {
		return merr.WrapErrParameterInvalidMsg("%s index can only be created on the primary key field, got field %s", IndexPrimaryKey, field.GetName())
	}
	if field.GetAutoID() {
		return merr.WrapErrParameterInvalidMsg("%s index does not support an autoID primary key field %s", IndexPrimaryKey, field.GetName())
	}
	switch field.GetDataType() {
	case schemapb.DataType_Int64, schemapb.DataType_VarChar:
		return nil
	default:
		return merr.WrapErrParameterInvalidMsg("%s index is only supported on an Int64 or VarChar primary key, got %s", IndexPrimaryKey, field.GetDataType())
	}
}

// CheckTrain accepts no index param other than the index type and the mmap switch.
func (c *primaryKeyChecker) CheckTrain(dataType schemapb.DataType, elementType schemapb.DataType, params map[string]string) error {
	for key := range params {
		if key != common.IndexTypeKey && key != common.MmapEnabledKey {
			return merr.WrapErrParameterInvalidMsg("%s index does not support the index param %s", IndexPrimaryKey, key)
		}
	}
	return c.scalarIndexChecker.CheckTrain(dataType, elementType, params)
}

func newPrimaryKeyChecker() *primaryKeyChecker {
	return &primaryKeyChecker{}
}
