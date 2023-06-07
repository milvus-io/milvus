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
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/log"
)

type alterCollectionTask struct {
	baseTask
	Req *milvuspb.AlterCollectionRequest
}

func (a *alterCollectionTask) Prepare(ctx context.Context) error {
	if a.Req.GetCollectionName() == "" {
		return fmt.Errorf("alter collection failed, collection name does not exists")
	}

	return nil
}

func (a *alterCollectionTask) Execute(ctx context.Context) error {
	// Now we only support alter properties of collection
	if a.Req.GetProperties() == nil {
		return errors.New("only support alter collection properties, but collection properties is empty")
	}

	oldColl, err := a.core.meta.GetCollectionByName(ctx, a.Req.GetDbName(), a.Req.GetCollectionName(), a.ts)
	if err != nil {
		log.Warn("get collection failed during changing collection state",
			zap.String("collectionName", a.Req.GetCollectionName()), zap.Uint64("ts", a.ts))
		return err
	}

	newColl := oldColl.Clone()
	updateCollectionProperties(newColl, a.Req.GetProperties())

	ts := a.GetTs()
	redoTask := newBaseRedoTask(a.core.stepExecutor)
	redoTask.AddSyncStep(&AlterCollectionStep{
		baseStep: baseStep{core: a.core},
		oldColl:  oldColl,
		newColl:  newColl,
		ts:       ts,
	})

	redoTask.AddSyncStep(&expireCacheStep{
		baseStep:        baseStep{core: a.core},
		dbName:          a.Req.GetDbName(),
		collectionNames: []string{oldColl.Name},
		collectionID:    oldColl.CollectionID,
		ts:              ts,
	})

	a.Req.CollectionID = oldColl.CollectionID
	redoTask.AddSyncStep(&BroadcastAlteredCollectionStep{
		baseStep: baseStep{core: a.core},
		req:      a.Req,
		core:     a.core,
	})

	return redoTask.Execute(ctx)
}

func updateCollectionProperties(coll *model.Collection, updatedProps []*commonpb.KeyValuePair) {
	props := make(map[string]string)
	for _, prop := range coll.Properties {
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

	coll.Properties = propKV
}
