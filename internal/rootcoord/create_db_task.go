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
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type createDatabaseTask struct {
	baseTask
	Req  *milvuspb.CreateDatabaseRequest
	dbID UniqueID
}

func (t *createDatabaseTask) Prepare(ctx context.Context) error {
	t.SetStep(typeutil.TaskStepPreExecute)
	dbs, err := t.core.meta.ListDatabases(ctx, t.GetTs())
	if err != nil {
		return err
	}

	cfgMaxDatabaseNum := Params.RootCoordCfg.MaxDatabaseNum
	if int64(len(dbs)) > cfgMaxDatabaseNum {
		return fmt.Errorf("database number (%d) exceeds max configuration (%d)", len(dbs), cfgMaxDatabaseNum)
	}

	t.dbID, err = t.core.idAllocator.AllocOne()
	if err != nil {
		return err
	}
	return nil
}

func (t *createDatabaseTask) Execute(ctx context.Context) error {
	t.SetStep(typeutil.TaskStepExecute)
	db := model.NewDatabase(t.dbID, t.Req.GetDbName(), etcdpb.DatabaseState_DatabaseCreated)
	return t.core.meta.CreateDatabase(ctx, db, t.GetTs())
}
