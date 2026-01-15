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
	"strconv"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util"
)

type KeyManager struct {
	ctx     context.Context
	meta    IMetaTable
	enabled bool
}

func NewKeyManager(
	ctx context.Context,
	meta IMetaTable,
) *KeyManager {
	if hookutil.GetCipherWithState() == nil {
		log.Info("KeyManager disabled (cipher plugin not loaded)")
		return nil
	}
	log.Info("KeyManager enabled")
	return &KeyManager{
		ctx:  ctx,
		meta: meta,
	}
}

func (km *KeyManager) GetRevokedDatabases() ([]int64, error) {
	currentStates, err := hookutil.GetEzStates()
	if err != nil {
		return nil, fmt.Errorf("failed to get cipher states: %w", err)
	}

	abnormalDB := make(map[int64]struct{})
	for ezID, currentState := range currentStates {
		if currentState != hookutil.KeyStateEnabled {
			db, err := km.getDatabaseByEzID(ezID)
			if err != nil {
				log.Warn("KeyManager: failed to get database for ezID", zap.Int64("ezID", ezID), zap.Error(err))
				continue
			}

			abnormalDB[db.ID] = struct{}{}
		}
	}

	revokedDBIDs := lo.Keys(abnormalDB)
	return revokedDBIDs, nil
}

func (km *KeyManager) getDatabaseByEzID(ezID int64) (*model.Database, error) {
	// use ezID as dbID to get database
	db, err := km.meta.GetDatabaseByID(km.ctx, ezID, 0)
	if err != nil {
		// fallback to default database(dbID=1)
		db, err = km.meta.GetDatabaseByID(km.ctx, util.DefaultDBID, 0)
		if err != nil {
			return nil, err
		}
	}

	// verify the ezID matches the retrieved DB
	if db.GetProperty(common.EncryptionEzIDKey) != strconv.FormatInt(ezID, 10) {
		return nil, fmt.Errorf("db for ezID %d not found", ezID)
	}

	return db, nil
}
