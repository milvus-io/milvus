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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type alterDynamicFieldTask struct {
	baseTask
	Req         *milvuspb.AlterCollectionRequest
	oldColl     *model.Collection
	fieldSchema *schemapb.FieldSchema
	targetValue bool
}

func (t *alterDynamicFieldTask) Prepare(ctx context.Context) error {
	if err := CheckMsgType(t.Req.GetBase().GetMsgType(), commonpb.MsgType_AlterCollection); err != nil {
		return err
	}

	oldColl, err := t.core.meta.GetCollectionByName(ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), t.ts)
	if err != nil {
		log.Ctx(ctx).Warn("get collection failed during alter dynamic schema",
			zap.String("collectionName", t.Req.GetCollectionName()), zap.Uint64("ts", t.ts))
		return err
	}
	t.oldColl = oldColl

	if len(t.Req.GetProperties()) > 1 {
		return merr.WrapErrParameterInvalidMsg("cannot alter dynamic schema with other properties")
	}

	// return nil for no-op
	if oldColl.EnableDynamicField == t.targetValue {
		return nil
	}

	// not support disabling since remove field not support yet.
	if !t.targetValue {
		return merr.WrapErrParameterInvalidMsg("dynamic schema cannot supported to be disabled")
	}

	// convert to add $meta json field, nullable, default value `{}`
	t.fieldSchema = &schemapb.FieldSchema{
		Name:      common.MetaFieldName,
		DataType:  schemapb.DataType_JSON,
		IsDynamic: true,
		Nullable:  true,
		DefaultValue: &schemapb.ValueField{
			Data: &schemapb.ValueField_BytesData{
				BytesData: []byte("{}"),
			},
		},
	}

	if err := checkFieldSchema([]*schemapb.FieldSchema{t.fieldSchema}); err != nil {
		return err
	}
	return nil
}

func (t *alterDynamicFieldTask) Execute(ctx context.Context) error {
	// return nil for no-op
	if t.oldColl.EnableDynamicField == t.targetValue {
		log.Info("dynamic schema is same as target value",
			zap.Bool("targetValue", t.targetValue),
			zap.String("collectionName", t.Req.GetCollectionName()))
		return nil
	}
	// assign field id
	t.fieldSchema.FieldID = nextFieldID(t.oldColl)

	// currently only add dynamic field support
	// TODO check target value to remove field after supported

	newField := model.UnmarshalFieldModel(t.fieldSchema)
	t.Req.CollectionID = t.oldColl.CollectionID

	ts := t.GetTs()
	return executeAddCollectionFieldTaskSteps(ctx, t.core, t.oldColl, newField, t.Req, ts)
}

func (t *alterDynamicFieldTask) GetLockerKey() LockerKey {
	collection := t.core.getCollectionIDStr(t.ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), 0)
	return NewLockerKeyChain(
		NewClusterLockerKey(false),
		NewDatabaseLockerKey(t.Req.GetDbName(), false),
		NewCollectionLockerKey(collection, true),
	)
}
