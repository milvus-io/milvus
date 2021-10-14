// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package datacoord

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/types"
)

var errDisposed = errors.New("client is disposed")

// NodeInfo contains node base info
type NodeInfo struct {
	NodeID  int64
	Address string
}

// Session contains session info of a node
type Session struct {
	sync.Mutex
	info          *NodeInfo
	client        types.DataNode
	clientCreator dataNodeCreatorFunc
	isDisposed    bool
}

// NewSession create a new session
func NewSession(info *NodeInfo, creator dataNodeCreatorFunc) *Session {
	return &Session{
		info:          info,
		clientCreator: creator,
	}
}

// GetOrCreateClient gets or creates a new client for session
func (n *Session) GetOrCreateClient(ctx context.Context) (types.DataNode, error) {
	n.Lock()
	defer n.Unlock()

	if n.isDisposed {
		return nil, errDisposed
	}

	if n.client != nil {
		return n.client, nil
	}

	if n.clientCreator == nil {
		return nil, fmt.Errorf("unable to create client for %s beacause of a nil client creator", n.info.Address)
	}

	err := n.initClient(ctx)
	return n.client, err
}

func (n *Session) initClient(ctx context.Context) (err error) {
	if n.client, err = n.clientCreator(ctx, n.info.Address); err != nil {
		return
	}
	if err = n.client.Init(); err != nil {
		return
	}
	return n.client.Start()
}

// Dispose release client connection
func (n *Session) Dispose() {
	n.Lock()
	defer n.Unlock()

	if n.client != nil {
		n.client.Stop()
		n.client = nil
	}
	n.isDisposed = true
}
