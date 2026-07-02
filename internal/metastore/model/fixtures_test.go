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

package model

import "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"

// Shared test fixtures for the index / segment_index model tests. These mirror
// the values defined in the rootcoord model tests (collection_test.go), which
// now live in the pkg/v3/metastore/model package; they are duplicated here so
// the datacoord model tests in this package keep compiling on their own.
var (
	colID   int64 = 1
	fieldID int64 = 101
	partID  int64 = 20

	typeParams = []*commonpb.KeyValuePair{
		{
			Key:   "field110-k1",
			Value: "field110-v1",
		},
	}
)
