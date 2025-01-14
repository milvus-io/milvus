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

	"github.com/milvus-io/milvus/pkg/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// describeDBTask describe database request task
type describeDBTask struct {
	baseTask
	Req              *rootcoordpb.DescribeDatabaseRequest
	Rsp              *rootcoordpb.DescribeDatabaseResponse
	allowUnavailable bool
}

func (t *describeDBTask) Prepare(ctx context.Context) error {
	return nil
}

// Execute task execution
func (t *describeDBTask) Execute(ctx context.Context) (err error) {
	db, err := t.core.meta.GetDatabaseByName(ctx, t.Req.GetDbName(), typeutil.MaxTimestamp)
	if err != nil {
		t.Rsp = &rootcoordpb.DescribeDatabaseResponse{
			Status: merr.Status(err),
		}
		return err
	}

	t.Rsp = &rootcoordpb.DescribeDatabaseResponse{
		Status:           merr.Success(),
		DbID:             db.ID,
		DbName:           db.Name,
		CreatedTimestamp: db.CreatedTime,
		Properties:       db.Properties,
	}
	return nil
}

func (t *describeDBTask) GetLockerKey() LockerKey {
	return NewLockerKeyChain(
		NewClusterLockerKey(false),
		NewDatabaseLockerKey(t.Req.GetDbName(), false),
	)
}
