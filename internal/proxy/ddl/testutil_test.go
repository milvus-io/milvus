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

package ddl

import "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"

const (
	int64Field    = "int64"
	floatVecField = "fVec"
	dim           = 128
)

// dbName mirrors the root package-level test var of the same name.
var dbName = "default"

// testVecIndexDataTypeCheck is the injected vec-index data-type check used by
// white-box tests; it accepts every combination.
var testVecIndexDataTypeCheck = func(string, schemapb.DataType, schemapb.DataType) bool {
	return true
}
