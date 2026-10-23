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

package packed

import (
	"context"
	"strconv"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// RestoreColumnGroupsFromManifest joins the logical IDs retained in Statistics
// with the ordered fields and physical formats of the exact manifest revision.
// File-name prefixes are writer-local ordinals, not logical column-group IDs.
// Columns are left unset: the caller maps field IDs using its selected schema.
func RestoreColumnGroupsFromManifest(ctx context.Context, manifestPath string, stats *datapb.Statistics, config *indexpb.StorageConfig) ([]storagecommon.ColumnGroup, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	base, version, err := UnmarshalManifestPath(manifestPath)
	if err != nil || base == "" || version <= ManifestEarliest {
		return nil, merr.WrapErrDataIntegrityMsg("invalid committed manifest path %q", manifestPath)
	}
	if config == nil {
		config = CreateStorageConfig()
	}
	groups, err := readColumnGroupsFromManifest(manifestPath, config)
	if err != nil {
		// The FFI markers have no wire code of their own. Keep their identity
		// for retry classification while giving them the storage wire code.
		if errors.Is(err, ErrLoonTransient) || errors.Is(err, ErrLoonPermanent) {
			return nil, merr.WrapErrStorage(err, "read column-group layout")
		}
		return nil, merr.Wrap(err, "read column-group layout")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	layout := make([]storagecommon.ColumnGroup, 0, len(groups))
	for _, group := range groups {
		fields := make([]int64, len(group.Columns))
		for i, column := range group.Columns {
			field, err := strconv.ParseInt(column, 10, 64)
			if err != nil || field < 0 {
				return nil, merr.WrapErrDataIntegrityMsg("invalid manifest field ID %q", column)
			}
			fields[i] = field
		}
		layout = append(layout, storagecommon.ColumnGroup{Fields: fields, Format: group.Format})
	}
	return storagecommon.RestoreColumnGroupLayout(layout, stats.GetLoadResource().GetColumnGroups())
}
