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

	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
)

type switchCollectionTask struct {
	baseTask
	Req *rootcoordpb.SwitchCollectionRequest
}

func (t *switchCollectionTask) validate() error {
	if t.core.meta.IsAlias(t.Req.GetDbName(), t.Req.GetNewName()) {
		return fmt.Errorf("cannot drop the collection via alias = %s", t.Req.GetNewName())
	}
	if t.core.meta.IsAlias(t.Req.GetDbName(), t.Req.GetOldName()) {
		return fmt.Errorf("cannot drop the collection via alias = %s", t.Req.GetOldName())
	}
	return nil
}

func (t *switchCollectionTask) Prepare(ctx context.Context) error {
	return t.validate()
}

func (t *switchCollectionTask) Execute(ctx context.Context) error {
	oldColl, err := t.core.meta.GetCollectionByName(ctx, t.Req.GetDbName(), t.Req.GetOldName(), t.GetTs())
	if err != nil {
		return err
	}
	aliases := t.core.meta.ListAliasesByID(oldColl.CollectionID)

	if err := t.core.ExpireMetaCache(ctx, t.Req.GetDbName(), append(aliases, t.Req.GetOldName(), t.Req.GetNewName()), InvalidCollectionID, t.GetTs()); err != nil {
		return err
	}
	return t.core.meta.SwitchCollectionID(ctx, t.Req.GetDbName(), t.Req.GetOldName(), t.Req.GetNewName(), t.GetTs())
	//t.core.meta.RenameCollection(ctx, t.Req.GetDbName(), t.Req.GetOldName(), ".temp", t.GetTs())
	//t.core.meta.RenameCollection(ctx, t.Req.GetDbName(), t.Req.GetNewName(), t.Req.GetOldName(), t.GetTs())
	//err := t.core.meta.RenameCollection(ctx, t.Req.GetDbName(), ".temp", t.Req.GetNewName(), t.GetTs())
	//if err == nil {
	//	for _, alias := range aliases {
	//		t.core.meta.AlterAlias(ctx, t.Req.GetDbName(), alias, t.Req.GetNewName(), t.GetTs())
	//	}
	//}
	//return err
}
