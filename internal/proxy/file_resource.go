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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var initProxyFileResourceManager func(storage.ChunkManager, fileresource.Mode) = fileresource.InitManager

func (node *Proxy) initFileResourceManager() error {
	mode := fileresource.ParseMode(paramtable.Get().CommonCfg.PNFileResourceMode.GetValue())
	if mode != fileresource.SyncMode {
		initProxyFileResourceManager(nil, fileresource.CloseMode)
		return nil
	}

	if node.factory == nil {
		return merr.WrapErrServiceInternalMsg("proxy file resource sync mode requires storage factory")
	}

	chunkManager, err := node.factory.NewPersistentStorageChunkManager(node.ctx)
	if err != nil {
		return err
	}

	initProxyFileResourceManager(chunkManager, mode)
	return nil
}

func (node *Proxy) SyncFileResource(ctx context.Context, req *internalpb.SyncFileResourceRequest) (*commonpb.Status, error) {
	log := mlog.With(mlog.Uint64("version", req.GetVersion()))
	log.Info(ctx, "sync file resource")

	if err := merr.CheckHealthy(node.GetStateCode()); err != nil {
		log.Warn(ctx, "failed to sync file resource, Proxy is not healthy")
		return merr.Status(err), nil
	}

	err := fileresource.Sync(req.GetVersion(), req.GetResources())
	if err != nil {
		return merr.Status(err), nil
	}
	return merr.Success(), nil
}
