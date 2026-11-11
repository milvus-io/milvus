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

package tikv

import (
	"bytes"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// EmptyValueString is the on-disk representation of an empty Milvus value.
const EmptyValueString = "__milvus_reserved_empty_tikv_value_DO_NOT_USE"

var emptyValue = []byte(EmptyValueString)

// EncodeValue returns the TxnKV representation without copying non-empty values.
func EncodeValue(value []byte) ([]byte, error) {
	if len(value) == 0 {
		return emptyValue, nil
	}
	if bytes.Equal(value, emptyValue) {
		return nil, merr.WrapErrParameterInvalidMsg("value for key is reserved by EmptyValue: %s", EmptyValueString)
	}
	return value, nil
}

// DecodeValue returns the logical value seen by the metadata catalog.
func DecodeValue(value []byte) []byte {
	if bytes.Equal(value, emptyValue) || len(value) == 0 {
		return []byte{}
	}
	return value
}
