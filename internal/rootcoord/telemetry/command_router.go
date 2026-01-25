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

package telemetry

import (
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

// CommandRouter routes commands to appropriate handlers based on command type
type CommandRouter struct {
	handlers map[string]CommandHandler
	mu       sync.RWMutex
}

// NewCommandRouter creates a new command router
func NewCommandRouter() *CommandRouter {
	return &CommandRouter{
		handlers: make(map[string]CommandHandler),
	}
}

// RegisterHandler registers a handler for a specific command type
func (r *CommandRouter) RegisterHandler(cmdType string, handler CommandHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[cmdType] = handler
}

// Route routes a command to the appropriate handler based on command type
// Returns a CommandReply with success status and optional error message
// The errorMsg is non-empty if handler execution failed
func (r *CommandRouter) Route(cmd *commonpb.ClientCommand) *commonpb.CommandReply {
	r.mu.RLock()
	handler, ok := r.handlers[cmd.CommandType]
	r.mu.RUnlock()

	reply := &commonpb.CommandReply{
		CommandId: cmd.CommandId,
	}

	if !ok {
		reply.Success = false
		reply.ErrorMessage = "unknown command type: " + cmd.CommandType
		return reply
	}

	// Execute handler and capture error message
	errMsg := handler(cmd)
	if errMsg != "" {
		reply.Success = false
		reply.ErrorMessage = errMsg
	} else {
		reply.Success = true
	}

	return reply
}

// RouteAndCollectReplies routes multiple commands and returns replies
func (r *CommandRouter) RouteAndCollectReplies(commands []*commonpb.ClientCommand) []*commonpb.CommandReply {
	var replies []*commonpb.CommandReply
	for _, cmd := range commands {
		if cmd == nil {
			continue // Skip nil commands
		}
		reply := r.Route(cmd)
		if reply != nil {
			replies = append(replies, reply)
		}
	}
	return replies
}
