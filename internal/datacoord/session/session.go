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

package session

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
)

var errDisposed = errors.New("client is disposed")

// NodeInfo contains node base info
type NodeInfo struct {
	NodeID   int64
	Address  string
	IsLegacy bool
}

// Session contains session info of a node
type Session struct {
	lock.Mutex
	info          *NodeInfo
	client        types.DataNodeClient
	clientCreator DataNodeCreatorFunc
	isDisposed    bool
}

// NewSession creates a new session
func NewSession(info *NodeInfo, creator DataNodeCreatorFunc) *Session {
	return &Session{
		info:          info,
		clientCreator: creator,
	}
}

// NodeID returns node id for session.
// If internal info is nil, return -1 instead.
func (n *Session) NodeID() int64 {
	if n.info == nil {
		return -1
	}
	return n.info.NodeID
}

// Address returns address of session internal node info.
// If internal info is nil, return empty string instead.
func (n *Session) Address() string {
	if n.info == nil {
		return ""
	}
	return n.info.Address
}

// GetOrCreateClient gets or creates a new client for session
func (n *Session) GetOrCreateClient(ctx context.Context) (types.DataNodeClient, error) {
	n.Lock()
	defer n.Unlock()

	if n.isDisposed {
		return nil, errDisposed
	}

	if n.client != nil {
		return n.client, nil
	}

	if n.clientCreator == nil {
		return nil, fmt.Errorf("unable to create client for %s because of a nil client creator", n.info.Address)
	}

	err := n.initClient(ctx)
	return n.client, err
}

func (n *Session) initClient(ctx context.Context) (err error) {
	if n.client, err = n.clientCreator(ctx, n.info.Address, n.info.NodeID); err != nil {
		return
	}
	return nil
}

// Dispose releases client connection
func (n *Session) Dispose() {
	n.Lock()
	defer n.Unlock()

	if n.client != nil {
		n.client.Close()
		n.client = nil
	}
	n.isDisposed = true
}
