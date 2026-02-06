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

package querycoordv2

import (
	"context"
	"fmt"
	"path"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const (
	resourceLimitFlagKey = "querycoord/failed_load/resource_limit"
	resourceLimitFlagTTL = 5 * time.Minute
)

func markResourceLimitFlag(ctx context.Context, cli *clientv3.Client) {
	if cli == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().ServiceParam.EtcdCfg.RequestTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	leaseResp, err := cli.Grant(ctx, int64(resourceLimitFlagTTL.Seconds()))
	if err != nil {
		log.Ctx(ctx).Warn("failed to grant resource limit flag lease", zap.Error(err))
		return
	}
	key := resourceLimitFlagPath()
	if _, err := cli.Put(ctx, key, fmt.Sprint(time.Now().Unix()), clientv3.WithLease(leaseResp.ID)); err != nil {
		log.Ctx(ctx).Warn("failed to mark resource limit flag", zap.String("key", key), zap.Error(err))
	}
}

func clearResourceLimitFlag(ctx context.Context, cli *clientv3.Client) {
	if cli == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().ServiceParam.EtcdCfg.RequestTimeout.GetAsDuration(time.Millisecond))
	defer cancel()
	key := resourceLimitFlagPath()
	if _, err := cli.Delete(ctx, key); err != nil {
		log.Ctx(ctx).Warn("failed to clear resource limit flag", zap.String("key", key), zap.Error(err))
	}
}

func resourceLimitFlagPath() string {
	return path.Join(paramtable.Get().EtcdCfg.MetaRootPath.GetValue(), resourceLimitFlagKey)
}
