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

package indexnode

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/stretchr/testify/assert"
)

func TestRegister(t *testing.T) {
	var (
		factory = &mockFactory{}
		ctx     = context.TODO()
	)
	Params.Init()
	in, err := NewIndexNode(ctx, factory)
	assert.Nil(t, err)
	in.SetEtcdClient(getEtcdClient())
	assert.Nil(t, in.initSession())
	assert.Nil(t, in.Register())
	key := in.session.ServerName
	if !in.session.Exclusive {
		key = fmt.Sprintf("%s-%d", key, in.session.ServerID)
	}
	resp, err := getEtcdClient().Get(ctx, path.Join(Params.EtcdCfg.MetaRootPath, sessionutil.DefaultServiceRoot, key))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), resp.Count)
	sess := &sessionutil.Session{}
	assert.Nil(t, json.Unmarshal(resp.Kvs[0].Value, sess))
	assert.Equal(t, sess.ServerID, in.session.ServerID)
	assert.Equal(t, sess.Address, in.session.Address)
	assert.Equal(t, sess.ServerName, in.session.ServerName)

	// revoke lease
	in.session.Revoke(time.Second)

	resp, err = getEtcdClient().Get(ctx, path.Join(Params.EtcdCfg.MetaRootPath, sessionutil.DefaultServiceRoot, in.session.ServerName))
	assert.Nil(t, err)
	assert.Equal(t, resp.Count, int64(0))
}

func TestComponentState(t *testing.T) {
	var (
		factory = &mockFactory{
			chunkMgr: &mockChunkmgr{},
		}
		ctx = context.TODO()
	)
	Params.Init()
	in, err := NewIndexNode(ctx, factory)
	assert.Nil(t, err)
	in.SetEtcdClient(getEtcdClient())
	state, err := in.GetComponentStates(ctx)
	assert.Nil(t, err)
	assert.Equal(t, state.Status.ErrorCode, commonpb.ErrorCode_Success)
	assert.Equal(t, state.State.StateCode, internalpb.StateCode_Abnormal)

	assert.Nil(t, in.Init())
	state, err = in.GetComponentStates(ctx)
	assert.Nil(t, err)
	assert.Equal(t, state.Status.ErrorCode, commonpb.ErrorCode_Success)
	assert.Equal(t, state.State.StateCode, internalpb.StateCode_Initializing)

	assert.Nil(t, in.Start())
	state, err = in.GetComponentStates(ctx)
	assert.Nil(t, err)
	assert.Equal(t, state.Status.ErrorCode, commonpb.ErrorCode_Success)
	assert.Equal(t, state.State.StateCode, internalpb.StateCode_Healthy)

	assert.Nil(t, in.Stop())
	state, err = in.GetComponentStates(ctx)
	assert.Nil(t, err)
	assert.Equal(t, state.Status.ErrorCode, commonpb.ErrorCode_Success)
	assert.Equal(t, state.State.StateCode, internalpb.StateCode_Abnormal)
}

func TestGetTimeTickChannel(t *testing.T) {
	var (
		factory = &mockFactory{
			chunkMgr: &mockChunkmgr{},
		}
		ctx = context.TODO()
	)
	Params.Init()
	in, err := NewIndexNode(ctx, factory)
	assert.Nil(t, err)
	ret, err := in.GetTimeTickChannel(ctx)
	assert.Nil(t, err)
	assert.Equal(t, ret.Status.ErrorCode, commonpb.ErrorCode_Success)
}

func TestGetStatisticChannel(t *testing.T) {
	var (
		factory = &mockFactory{
			chunkMgr: &mockChunkmgr{},
		}
		ctx = context.TODO()
	)
	Params.Init()
	in, err := NewIndexNode(ctx, factory)
	assert.Nil(t, err)
	ret, err := in.GetStatisticsChannel(ctx)
	assert.Nil(t, err)
	assert.Equal(t, ret.Status.ErrorCode, commonpb.ErrorCode_Success)
}

func TestInitErr(t *testing.T) {
	// var (
	// 	factory = &mockFactory{}
	// 	ctx     = context.TODO()
	// )
	// in, err := NewIndexNode(ctx, factory)
	// assert.Nil(t, err)
	// in.SetEtcdClient(getEtcdClient())
	// assert.Error(t, in.Init())
}

func setup() {
	startEmbedEtcd()
}

func teardown() {
	stopEmbedEtcd()
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}
