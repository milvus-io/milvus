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

package proxy

import (
	"context"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func (t *flushAllTask) Execute(ctx context.Context) error {
	dbNames := make([]string, 0)
	if t.GetDbName() != "" {
		dbNames = append(dbNames, t.GetDbName())
	} else {
		listResp, err := t.mixCoord.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{
			Base: commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_ListDatabases)),
		})
		if err != nil {
			log.Info("flush all task by streaming service failed, list databases failed", zap.Error(err))
			return err
		}
		dbNames = listResp.GetDbNames()
	}

	flushTs := t.BeginTs()

	wg := errgroup.Group{}
	// limit goroutine number to 100
	wg.SetLimit(100)
	for _, dbName := range dbNames {
		showColRsp, err := t.mixCoord.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
			Base:   commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_ShowCollections)),
			DbName: dbName,
		})
		if err := merr.CheckRPCCall(showColRsp, err); err != nil {
			log.Info("flush all task by streaming service failed, show collections failed", zap.String("dbName", dbName), zap.Error(err))
			return err
		}

		collections := showColRsp.GetCollectionNames()
		for _, collName := range collections {
			coll := collName
			wg.Go(func() error {
				collID, err := globalMetaCache.GetCollectionID(t.ctx, t.DbName, coll)
				if err != nil {
					return merr.WrapErrAsInputErrorWhen(err, merr.ErrCollectionNotFound, merr.ErrDatabaseNotFound)
				}
				vchannels, err := t.chMgr.getVChannels(collID)
				if err != nil {
					return err
				}

				// Ask the streamingnode to flush segments.
				for _, vchannel := range vchannels {
					_, err := sendManualFlushToWAL(ctx, collID, vchannel, flushTs)
					if err != nil {
						return err
					}
				}
				return nil
			})
		}
	}
	err := wg.Wait()
	if err != nil {
		return err
	}

	t.result = &datapb.FlushAllResponse{
		Status:  merr.Success(),
		FlushTs: flushTs,
	}
	return nil
}
