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

package importid

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// NeedsFileIDRanges reports whether import reserves one ID range per file for the job: the
// collection has a resolvable primary key and this is neither a backup import (the binlogs
// carry the primary keys and row ids) nor an l0 import (delete only, no rows). The range
// supplies the primary key on autoID collections and the RowID on explicit-PK ones, so the
// datanode's PK/RowID stay identical across clusters. A file with no range (such a job, or
// one created before the mechanism) falls back to the request's log id range.
func NeedsFileIDRanges(schema *schemapb.CollectionSchema, options []*commonpb.KeyValuePair) bool {
	if _, err := typeutil.GetPrimaryFieldSchema(schema); err != nil {
		return false
	}
	return !importutilv2.IsBackup(options) && !importutilv2.IsL0Import(options)
}

// FileIDRange is the reserved ID range of one import file, consumed sequentially as the file
// is read. It supplies the primary key on autoID collections and the RowID on explicit-PK
// ones, so both clusters derive identical values.
type FileIDRange struct {
	begin, end, next int64
}

// NewFileIDRange returns the ID range of the file, or nil when the file carries none (backup
// / l0, or a job created before the range mechanism). A nil range selects the datanode's log
// id range fallback.
func NewFileIDRange(file *internalpb.ImportFile) *FileIDRange {
	r := file.GetIdRange()
	if r == nil {
		return nil
	}
	return &FileIDRange{begin: r.GetBegin(), end: r.GetEnd(), next: r.GetBegin()}
}

// Take reserves n contiguous ids and returns the first one, failing loudly if the file yields
// more rows than its reserved range (the two clusters read different files, or the file
// exceeded the row count the range was sized from).
func (r *FileIDRange) Take(n int) (int64, error) {
	if r.next+int64(n) > r.end {
		return 0, merr.WrapErrImportFailed(fmt.Sprintf(
			"import file produced more rows than its reserved ID range [%d, %d): the "+
				"reservation is too small, or this file differs from the one the "+
				"primary cluster sized", r.begin, r.end))
	}
	start := r.next
	r.next += int64(n)
	return start, nil
}
