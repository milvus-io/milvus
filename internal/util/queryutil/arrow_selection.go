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

package queryutil

import "github.com/apache/arrow/go/v17/arrow"

// ArrowSelection is an UNMATERIALIZED reduce result: the per-segment Arrow
// records the retrieve produced, plus the rows the reduce chose from them.
//
// Carrying the selection rather than gathering it into a merged record is what
// makes the Arrow path cost one payload pass instead of two -- the merged
// record had no consumer, since the Arrow pipeline ends at the reduce and the
// response needs FieldData. See the design doc for the pass counts.
//
// Lifetime: the Records are owned by the caller of the reduce, not by this
// struct, and must stay alive until the selection has been materialized. On the
// query path QueryTask.Execute's `defer segments.ReleaseRecords(results)`
// already spans exactly that window.
//
// A nil or empty selection means the protobuf path ran.
type ArrowSelection struct {
	// Records is indexed by rowRef.resultIdx and may contain nil entries: an L0
	// segment or one that matched nothing contributes no record, and its slot
	// still has to occupy an index so resultIdx stays meaningful.
	Records []arrow.Record
	// Rows is the reduce's output order; every entry indexes into Records.
	Rows []rowRef
}

// Empty reports whether there is nothing to materialize.
func (s *ArrowSelection) Empty() bool {
	return s == nil || len(s.Rows) == 0 || len(s.Records) == 0
}

// Template returns the first non-nil record, whose schema and metadata
// (milvus.field_order, milvus.valid_data_fields) describe every record in the
// selection. Returns nil when the selection carries no record at all.
func (s *ArrowSelection) Template() arrow.Record {
	if s == nil {
		return nil
	}
	for _, r := range s.Records {
		if r != nil {
			return r
		}
	}
	return nil
}
