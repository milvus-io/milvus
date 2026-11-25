// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kvfactory

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestCloseEtcdClientDuringStartupShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	client := clientv3.NewCtxClient(ctx)
	original := clientCreator
	clientCreator = &etcdClientCreator{client: client, rootpath: "test/meta"}
	t.Cleanup(func() { clientCreator = original })

	cancel()
	CloseEtcdClient()
	require.Nil(t, clientCreator.client)
	require.Empty(t, clientCreator.rootpath)
}
