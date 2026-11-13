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

package rootcoord

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// CompactionTargetCollection resolves the current identity by stable ID using
// RootCoord's lifecycle metadata. DataCoord caches can lag renames and drops.
func (c *Core) CompactionTargetCollection(ctx context.Context, id int64) (*datapb.CompactionTargetResource, bool, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return nil, false, err
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	resource := &datapb.CompactionTargetResource{CollectionId: id}
	collection, err := c.meta.GetCollectionByID(ctx, "", id, typeutil.MaxTimestamp, true)
	if errors.Is(err, merr.ErrCollectionNotFound) {
		resource.Deleted = true
		return resource, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	switch collection.State {
	case pb.CollectionState_CollectionDropping, pb.CollectionState_CollectionDropped:
		resource.Deleted = true
	case pb.CollectionState_CollectionCreated:
		resource.CollectionName = collection.Name
	default:
		return nil, false, merr.WrapErrServiceUnavailableMsg("compaction target collection is unavailable")
	}
	dbID := collection.DBID
	if dbID == util.NonDBID {
		dbID = util.DefaultDBID
	}
	db, err := c.meta.GetDatabaseByID(ctx, dbID, typeutil.MaxTimestamp)
	if err != nil {
		if resource.Deleted && errors.Is(err, merr.ErrDatabaseNotFound) {
			return resource, false, nil
		}
		return nil, false, err
	}
	resource.DbId = dbID
	resource.DbName = db.Name
	resource.Revision = collection.UpdateTimestamp
	return resource, typeutil.IsExternalCollection(collection.ToCollectionSchemaPB()), nil
}
