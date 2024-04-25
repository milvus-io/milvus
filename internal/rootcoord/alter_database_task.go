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

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/log"
)

type alterDatabaseTask struct {
	baseTask
	Req *rootcoordpb.AlterDatabaseRequest
}

func (a *alterDatabaseTask) Prepare(ctx context.Context) error {
	if a.Req.GetDbName() == "" {
		return fmt.Errorf("alter database failed, database name does not exists")
	}

	return nil
}

func (a *alterDatabaseTask) Execute(ctx context.Context) error {
	// Now we only support alter properties of database
	if a.Req.GetProperties() == nil {
		return errors.New("only support alter database properties, but database properties is empty")
	}

	oldDB, err := a.core.meta.GetDatabaseByName(ctx, a.Req.GetDbName(), a.ts)
	if err != nil {
		log.Ctx(ctx).Warn("get database failed during changing database props",
			zap.String("databaseName", a.Req.GetDbName()), zap.Uint64("ts", a.ts))
		return err
	}

	newDB := oldDB.Clone()
	ret := updateProperties(oldDB.Properties, a.Req.GetProperties())
	newDB.Properties = ret

	ts := a.GetTs()
	redoTask := newBaseRedoTask(a.core.stepExecutor)
	redoTask.AddSyncStep(&AlterDatabaseStep{
		baseStep: baseStep{core: a.core},
		oldDB:    oldDB,
		newDB:    newDB,
		ts:       ts,
	})

	return redoTask.Execute(ctx)
}

func updateProperties(oldProps []*commonpb.KeyValuePair, updatedProps []*commonpb.KeyValuePair) []*commonpb.KeyValuePair {
	props := make(map[string]string)
	for _, prop := range oldProps {
		props[prop.Key] = prop.Value
	}

	for _, prop := range updatedProps {
		props[prop.Key] = prop.Value
	}

	propKV := make([]*commonpb.KeyValuePair, 0)

	for key, value := range props {
		propKV = append(propKV, &commonpb.KeyValuePair{
			Key:   key,
			Value: value,
		})
	}

	return propKV
}
