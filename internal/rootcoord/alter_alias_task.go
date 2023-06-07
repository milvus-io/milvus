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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

type alterAliasTask struct {
	baseTask
	Req *milvuspb.AlterAliasRequest
}

func (t *alterAliasTask) Prepare(ctx context.Context) error {
	if err := CheckMsgType(t.Req.GetBase().GetMsgType(), commonpb.MsgType_AlterAlias); err != nil {
		return err
	}
	return nil
}

func (t *alterAliasTask) Execute(ctx context.Context) error {
	if err := t.core.ExpireMetaCache(ctx, t.Req.GetDbName(), []string{t.Req.GetAlias()}, InvalidCollectionID, t.GetTs()); err != nil {
		return err
	}
	// alter alias is atomic enough.
	return t.core.meta.AlterAlias(ctx, t.Req.GetDbName(), t.Req.GetAlias(), t.Req.GetCollectionName(), t.GetTs())
}
