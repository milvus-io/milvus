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
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type KeyManager struct {
	ctx      context.Context
	meta     IMetaTable
	mixCoord types.MixCoord
	enabled  bool
}

func NewKeyManager(
	ctx context.Context,
	meta IMetaTable,
	mixCoord types.MixCoord,
) *KeyManager {
	return &KeyManager{
		ctx:      ctx,
		meta:     meta,
		mixCoord: mixCoord,
		enabled:  hookutil.GetCipherWithState() != nil,
	}
}

func (km *KeyManager) Init() error {
	if !km.enabled {
		log.Info("KeyManager disabled (cipher plugin not loaded)")
		return nil
	}

	hookutil.GetCipherWithState().RegisterRotationCallback(km.onKeyRotated)
	log.Info("KeyManager initialized")
	return nil
}

func (km *KeyManager) GetDatabaseEzStates() ([]int64, error) {
	if !km.enabled {
		return nil, nil
	}

	currentStates, err := hookutil.GetEzStates()
	if err != nil {
		return nil, fmt.Errorf("failed to get cipher states: %w", err)
	}

	revokedDBs := make(map[int64]struct{})
	for ezID, currentState := range currentStates {
		switch currentState {
		case hookutil.KeyStateDisabled, hookutil.KeyStatePendingDeletion:
			db, err := km.getDatabaseByEzID(ezID)
			if err != nil {
				log.Warn("KeyManager: failed to get database for ezID", zap.Int64("ezID", ezID), zap.Error(err))
				continue
			}

			revokedDBs[db.ID] = struct{}{}
		}
	}

	revokedDBIDs := lo.Keys(revokedDBs)
	if err := km.releaseLoadedCollections(revokedDBIDs); err != nil {
		log.Warn("KeyManager: failed to release collections for revoked databases", zap.Error(err))
	}

	return revokedDBIDs, nil
}

func (km *KeyManager) releaseLoadedCollections(revokedDBIDs []int64) error {
	if len(revokedDBIDs) == 0 {
		return nil
	}

	collectionIDsToCheck := make([]int64, 0)

	for _, dbID := range revokedDBIDs {
		db, err := km.meta.GetDatabaseByID(km.ctx, dbID, 0)
		if err != nil {
			log.Warn("KeyManager: failed to get database metadata", zap.Int64("dbID", dbID), zap.Error(err))
			continue
		}

		colls, err := km.meta.ListCollections(km.ctx, db.Name, 0, true)
		if err != nil {
			log.Warn("KeyManager: failed to list collections for revoked database",
				zap.Int64("dbID", dbID),
				zap.String("dbName", db.Name),
				zap.Error(err))
			continue
		}

		for _, coll := range colls {
			collectionIDsToCheck = append(collectionIDsToCheck, coll.CollectionID)
		}
	}

	if len(collectionIDsToCheck) == 0 {
		return nil
	}

	resp, err := km.mixCoord.ShowLoadCollections(km.ctx, &querypb.ShowCollectionsRequest{
		CollectionIDs: collectionIDsToCheck,
	})
	if err := merr.CheckRPCCall(resp.GetStatus(), err); err != nil {
		if errors.Is(err, merr.ErrCollectionNotLoaded) {
			return nil
		}
		return fmt.Errorf("failed to get loaded collections: %w", err)
	}

	log.Info("KeyManager: releasing loaded collection for revoked database",
		zap.Int64s("collectionIDs", resp.GetCollectionIDs()),
	)
	for _, collID := range resp.GetCollectionIDs() {
		req := &querypb.ReleaseCollectionRequest{CollectionID: collID}
		if _, err := km.mixCoord.ReleaseCollection(km.ctx, req); err != nil {
			log.Warn("KeyManager: failed to release collection", zap.Int64("collectionID", collID), zap.Error(err))
			continue
		}
	}
	return nil
}

func (km *KeyManager) getDatabaseByEzID(ezID int64) (*model.Database, error) {
	db, err := km.meta.GetDatabaseByID(km.ctx, ezID, 0)
	if err != nil {
		return km.meta.GetDatabaseByID(km.ctx, util.DefaultDBID, 0)
	}
	return db, nil
}

func (km *KeyManager) onKeyRotated(ezID int64) error {
	if !km.enabled {
		return nil
	}

	log.Info("KeyManager: handling key rotation", zap.Int64("ezID", ezID))
	db, err := km.getDatabaseByEzID(ezID)
	if err != nil {
		return err
	}

	req := &rootcoordpb.AlterDatabaseRequest{
		DbName: db.Name,
		Properties: []*commonpb.KeyValuePair{
			{
				Key: common.InternalCipherKeyRotatedKey,
			},
		},
	}

	status, err := km.mixCoord.AlterDatabase(km.ctx, req)
	if err := merr.CheckRPCCall(status, err); err != nil {
		log.Error("KeyManager: failed to broadcast key rotation",
			zap.Int64("dbID", db.ID),
			zap.Int64("ezID", ezID),
			zap.String("dbName", db.Name),
			zap.Error(err))
		return fmt.Errorf("failed to broadcast key rotation: %w", err)
	}

	log.Info("KeyManager: key rotation handled", zap.Int64("ezID", ezID))
	return nil
}
