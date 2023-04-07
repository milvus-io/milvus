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

package registry

import (
	"context"

	"github.com/milvus-io/milvus/internal/registry/options"
	"github.com/milvus-io/milvus/internal/types"
)

// ServiceEntry service entry definition.
type ServiceEntry interface {
	ID() int64
	Addr() string
	ComponentType() string
}

// Session is the interface for service discovery.
type Session interface {
	ServiceEntry
	Revoke(ctx context.Context) error
	Stop(ctx context.Context) error
	Init(ctx context.Context) error
	Register(ctx context.Context) error
}

// ServiceDiscovery is the interface for service discovery operations.
type ServiceDiscovery interface {
	// general get & watch, only for service entry
	GetServices(ctx context.Context, component string) ([]ServiceEntry, error)
	WatchServices(ctx context.Context, component string, opts ...options.WatchOption) ([]ServiceEntry, ServiceWatcher[ServiceEntry], error)
	// Coordinators
	GetRootCoord(ctx context.Context) (types.RootCoord, error)
	GetQueryCoord(ctx context.Context) (types.QueryCoord, error)
	GetDataCoord(ctx context.Context) (types.DataCoord, error)
	// Watch methods
	WatchDataNode(ctx context.Context, opts ...options.WatchOption) ([]types.DataNode, ServiceWatcher[types.DataNode], error)
	WatchQueryNode(ctx context.Context, opts ...options.WatchOption) ([]types.QueryNode, ServiceWatcher[types.QueryNode], error)
	WatchIndexNode(ctx context.Context, opts ...options.WatchOption) ([]types.IndexNode, ServiceWatcher[types.IndexNode], error)
	WatchProxy(ctx context.Context, opts ...options.WatchOption) ([]types.Proxy, ServiceWatcher[types.Proxy], error)
	// Register methods
	RegisterRootCoord(ctx context.Context, rootcoord types.RootCoord, addr string, opts ...options.RegisterOption) (Session, error)
	RegisterDataCoord(ctx context.Context, datacoord types.DataCoord, addr string, opts ...options.RegisterOption) (Session, error)
	RegisterQueryCoord(ctx context.Context, querycoord types.QueryCoord, addr string, opts ...options.RegisterOption) (Session, error)
	RegisterProxy(ctx context.Context, proxy types.Proxy, addr string, opts ...options.RegisterOption) (Session, error)
	RegisterDataNode(ctx context.Context, datanode types.DataNode, addr string, opts ...options.RegisterOption) (Session, error)
	RegisterIndexNode(ctx context.Context, indexnode types.IndexNode, addr string, opts ...options.RegisterOption) (Session, error)
	RegisterQueryNode(ctx context.Context, index types.QueryNode, addr string, opts ...options.RegisterOption) (Session, error)
}

// ServiceWatcher is the watch helper for ServiceDiscovery Watch methods.
type ServiceWatcher[T any] interface {
	// Watch returns the channel of all the service change.
	Watch() <-chan SessionEvent[T]
	// Stop make watcher stops and closes all the returned channel.
	Stop()
}

// SessionEventType session event type
type SessionEventType int

const (
	// SessionNoneEvent place holder for zero value
	SessionNoneEvent SessionEventType = iota
	// SessionAddEvent event type for a new Session Added
	SessionAddEvent
	// SessionDelEvent event type for a Session deleted
	SessionDelEvent
	// SessionUpdateEvent event type for a Session stopping
	SessionUpdateEvent
)

// String implements stringer.
func (t SessionEventType) String() string {
	switch t {
	case SessionAddEvent:
		return "SessionAddEvent"
	case SessionDelEvent:
		return "SessionDelEvent"
	case SessionUpdateEvent:
		return "SessionUpdateEvent"
	default:
		return ""
	}
}

// SessionEvent indicates the changes of other servers.
// if a server is up, EventType is SessAddEvent.
// if a server is down, EventType is SessDelEvent.
// Session Saves the changed server's information.
type SessionEvent[T any] struct {
	EventType SessionEventType
	Entry     ServiceEntry
	Client    T
}
