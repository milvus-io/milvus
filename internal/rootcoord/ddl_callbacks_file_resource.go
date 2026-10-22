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

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (c *Core) broadcastAddFileResource(ctx context.Context, req *milvuspb.AddFileResourceRequest) error {
	if req.GetName() == "" {
		return merr.WrapErrParameterMissing("file resource name")
	}
	if req.GetPath() == "" {
		return merr.WrapErrParameterMissing("file resource path")
	}
	if exist, err := c.storage.Exist(ctx, req.GetPath()); err != nil {
		return err
	} else if !exist {
		return merr.WrapErrParameterInvalidMsg("file resource path not exist")
	}

	// Resource changes conflict with schema DDL, which reserves resource refs.
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusiveClusterResourceKey())
	if err != nil {
		return err
	}
	defer func() {
		if broadcaster != nil {
			broadcaster.Close()
		}
	}()

	var size int64
	sizeChecked := false
	for {
		resource := c.meta.FindFileResource(ctx, req.GetName())
		if resource != nil && resource.GetPath() != req.GetPath() {
			return merr.WrapErrParameterInvalidMsg("file resource %s already exists", req.GetName())
		}
		if resource == nil {
			if maxFileSize := Params.CommonCfg.FileResourceMaxFileSize.GetAsSize(); maxFileSize > 0 {
				if !sizeChecked {
					// Storage admission must not block unrelated DDL.
					broadcaster.Close()
					broadcaster = nil
					size, err = c.storage.Size(ctx, req.GetPath())
					if err != nil {
						return err
					}
					broadcaster, err = broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusiveClusterResourceKey())
					if err != nil {
						return err
					}
					sizeChecked = true
					continue
				}
				if size < 0 {
					return merr.WrapErrIoFailedMsg("file resource %s reports negative size %d", req.GetPath(), size)
				}
				if size > maxFileSize {
					return merr.WrapErrParameterTooLarge("file resource", fmt.Sprintf(
						"size %d exceeds common.fileResource.maxFileSize %d", size, maxFileSize))
				}
			}
			id, err := c.tsoAllocator.GenerateTSO(1)
			if err != nil {
				return err
			}
			resource = &internalpb.FileResourceInfo{Id: int64(id), Name: req.GetName(), Path: req.GetPath()}
		}

		msg := message.NewAddFileResourceMessageBuilderV2().
			WithHeader(&message.AddFileResourceMessageHeader{}).
			WithBody(&message.AddFileResourceMessageBody{Resource: resource}).
			WithControlChannelBroadcast().
			MustBuildBroadcast()
		_, err = broadcaster.Broadcast(ctx, msg)
		return err
	}
}

func (c *Core) broadcastRemoveFileResource(ctx context.Context, req *milvuspb.RemoveFileResourceRequest) (bool, error) {
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusiveClusterResourceKey())
	if err != nil {
		return false, err
	}
	defer broadcaster.Close()

	resource, err := c.meta.PrepareRemoveFileResource(ctx, req.GetName())
	if err != nil || resource == nil {
		return false, err
	}
	msg := message.NewRemoveFileResourceMessageBuilderV2().
		WithHeader(&message.RemoveFileResourceMessageHeader{}).
		WithBody(&message.RemoveFileResourceMessageBody{ResourceId: resource.GetId(), Name: resource.GetName()}).
		WithControlChannelBroadcast().
		MustBuildBroadcast()
	_, err = broadcaster.Broadcast(ctx, msg)
	return err == nil, err
}

func (c *DDLCallback) addFileResourceV2AckCallback(ctx context.Context, result message.BroadcastResultAddFileResourceMessageV2) error {
	resource := result.Message.MustBody().GetResource()
	if resource == nil || resource.GetId() <= 0 || resource.GetName() == "" || resource.GetPath() == "" {
		return merr.WrapErrServiceInternalMsg("add file resource message has invalid resource metadata")
	}
	if err := c.meta.AddFileResource(ctx, resource); err != nil {
		return err
	}
	if c.fileResourceObserver != nil {
		c.fileResourceObserver.Notify()
	}
	return nil
}

func (c *DDLCallback) removeFileResourceV2AckCallback(ctx context.Context, result message.BroadcastResultRemoveFileResourceMessageV2) error {
	body := result.Message.MustBody()
	if body.GetResourceId() <= 0 {
		return merr.WrapErrServiceInternalMsg("remove file resource message has invalid resource ID")
	}
	if err, _ := c.meta.RemoveFileResource(ctx, body.GetName(), body.GetResourceId()); err != nil {
		return err
	}
	// Retry the notification even if a previous attempt already removed metadata.
	if c.fileResourceObserver != nil {
		c.fileResourceObserver.Notify()
	}
	return nil
}
