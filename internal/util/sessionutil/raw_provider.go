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

package sessionutil

import (
	"context"
	"encoding/json"
	"path"

	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/merr"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type rawEntryProvider struct {
	client   *clientv3.Client
	metaPath string
	role     string
}

// NewRawEntryProvider returns a RawEntryProvider for backward-compatibility.
// works for cooorinators service entry.
// Shall be removed when all service-discovery logic is unified.
func NewRawEntryProvider(cli *clientv3.Client, metaPath string, role string) grpcclient.ServiceProvider {
	return &rawEntryProvider{
		client:   cli,
		metaPath: metaPath,
		role:     role,
	}
}

// GetServiceEntry returns current service entry information.
func (p *rawEntryProvider) GetServiceEntry(ctx context.Context) (string, int64, error) {
	key := path.Join(p.metaPath, common.DefaultServiceRoot, p.role)

	resp, err := p.client.Get(ctx, key)
	if err != nil {
		err := merr.WrapErrIoFailed(key, err.Error())
		return "", 0, err
	}

	if len(resp.Kvs) != 1 {
		err := merr.WrapErrIoKeyNotFound(key)
		return "", 0, err
	}

	kv := resp.Kvs[0]
	session := Session{}
	err = json.Unmarshal(kv.Value, &session)
	if err != nil {
		err := merr.WrapErrIoKeyNotFound(key, err.Error())
		return "", 0, err
	}

	return session.Address, session.ServerID, nil
}
