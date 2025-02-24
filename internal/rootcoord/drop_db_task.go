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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v2/util"
)

type dropDatabaseTask struct {
	baseTask
	Req *milvuspb.DropDatabaseRequest
}

func (t *dropDatabaseTask) Prepare(ctx context.Context) error {
	if t.Req.GetDbName() == util.DefaultDBName {
		return fmt.Errorf("can not drop default database")
	}
	return nil
}

func (t *dropDatabaseTask) Execute(ctx context.Context) error {
	dbName := t.Req.GetDbName()
	ts := t.GetTs()
	return executeDropDatabaseTaskSteps(ctx, t.core, dbName, ts)
}

func (t *dropDatabaseTask) GetLockerKey() LockerKey {
	return NewLockerKeyChain(NewClusterLockerKey(true))
}

func executeDropDatabaseTaskSteps(ctx context.Context,
	core *Core,
	dbName string,
	ts Timestamp,
) error {
	redoTask := newBaseRedoTask(core.stepExecutor)
	redoTask.AddSyncStep(&deleteDatabaseMetaStep{
		baseStep:     baseStep{core: core},
		databaseName: dbName,
		ts:           ts,
	})
	redoTask.AddSyncStep(&expireCacheStep{
		baseStep: baseStep{core: core},
		dbName:   dbName,
		ts:       ts,
		// make sure to send the "expire cache" request
		// because it won't send this request when the length of collection names array is zero
		collectionNames: []string{""},
		opts: []proxyutil.ExpireCacheOpt{
			proxyutil.SetMsgType(commonpb.MsgType_DropDatabase),
		},
	})
	return redoTask.Execute(ctx)
}
