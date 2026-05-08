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

package datacoord

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// updateExternalSchemaViaWAL updates collection schema's external_source and
// external_spec by delegating to RootCoord's AlterCollection API. This ensures
// RootCoord remains the authoritative schema owner and avoids stale-cache
// overwrites that can occur when DataCoord broadcasts full schema directly.
func (s *Server) updateExternalSchemaViaWAL(ctx context.Context, collectionID int64, externalSource, externalSpec string) error {
	coll := s.meta.GetClonedCollectionInfo(collectionID)
	if coll == nil {
		return merr.WrapErrCollectionNotFound(collectionID)
	}

	resp, err := s.mixCoord.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		DbName:         coll.DatabaseName,
		CollectionName: coll.Schema.GetName(),
		Properties: []*commonpb.KeyValuePair{
			{Key: common.CollectionExternalSource, Value: externalSource},
			{Key: common.CollectionExternalSpec, Value: externalSpec},
		},
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return err
	}

	log.Info("updated external schema via RootCoord after refresh",
		zap.Int64("collectionID", collectionID),
		zap.String("externalSource", externalSource),
		zap.String("externalSpec", externalspec.RedactExternalSpec(externalSpec)))
	return nil
}
