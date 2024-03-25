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

	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type lockCollectionTask struct {
	baseTask
	Req          *rootcoordpb.LockCollectionRequest
	CollectionID int64
}

func (t *lockCollectionTask) validate() error {
	if t.core.meta.IsAlias(t.Req.GetDbName(), t.Req.GetCollectionName()) {
		return fmt.Errorf("cannot drop the collection via alias = %s", t.Req.CollectionName)
	}
	return nil
}

func (t *lockCollectionTask) Prepare(ctx context.Context) error {
	return t.validate()
}

func (t *lockCollectionTask) Execute(ctx context.Context) error {
	// use max ts to check if latest collection exists.
	// we cannot handle case that
	// dropping collection with `ts1` but a collection exists in catalog with newer ts which is bigger than `ts1`.
	// fortunately, if ddls are promised to execute in sequence, then everything is OK. The `ts1` will always be latest.
	collMeta, err := t.core.meta.GetCollectionByName(ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), typeutil.MaxTimestamp)
	if errors.Is(err, merr.ErrCollectionNotFound) {
		// make dropping collection idempotent.
		log.Warn("lock non-existent collection", zap.String("collection", t.Req.GetCollectionName()))
		return nil
	}

	if err != nil {
		return err
	}

	t.CollectionID = collMeta.CollectionID
	t.core.meta.LockCollection(ctx, collMeta.CollectionID, typeutil.MaxTimestamp)

	return nil
}

type unlockCollectionTask struct {
	baseTask
	Req *rootcoordpb.UnlockCollectionRequest
}

func (t *unlockCollectionTask) validate() error {
	if t.core.meta.IsAlias(t.Req.GetDbName(), t.Req.GetCollectionName()) {
		return fmt.Errorf("cannot drop the collection via alias = %s", t.Req.CollectionName)
	}
	return nil
}

func (t *unlockCollectionTask) Prepare(ctx context.Context) error {
	return t.validate()
}

func (t *unlockCollectionTask) Execute(ctx context.Context) error {
	// use max ts to check if latest collection exists.
	// we cannot handle case that
	// dropping collection with `ts1` but a collection exists in catalog with newer ts which is bigger than `ts1`.
	// fortunately, if ddls are promised to execute in sequence, then everything is OK. The `ts1` will always be latest.
	collMeta, err := t.core.meta.GetCollectionByName(ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), typeutil.MaxTimestamp)
	if errors.Is(err, merr.ErrCollectionNotFound) {
		// make dropping collection idempotent.
		log.Warn("unlock non-existent collection", zap.String("collection", t.Req.GetCollectionName()))
		return nil
	}

	if err != nil {
		return err
	}

	t.core.meta.UnlockCollection(ctx, collMeta.CollectionID, typeutil.MaxTimestamp)

	return nil
}
