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

package entity

import "github.com/milvus-io/milvus-proto/go-api/v2/commonpb"

// Segment represent segment in milvus
type Segment struct {
	ID           int64
	CollectionID int64
	ParititionID int64

	NumRows int64
	State   commonpb.SegmentState
}

// Flushed indicates segment is flushed
func (s Segment) Flushed() bool {
	return s.State == commonpb.SegmentState_Flushed
}
