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

package ddl

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

var _ interceptors.Interceptor = (*ddlAppendInterceptor)(nil)

// ddlAppendInterceptor is an append interceptor.
type ddlAppendInterceptor struct {
	wal *syncutil.Future[wal.WAL]
}

// DoAppend implements AppendInterceptor.
func (d *ddlAppendInterceptor) DoAppend(ctx context.Context, msg message.MutableMessage, append interceptors.Append) (message.MessageID, error) {
	// send the create collection message.
	msgID, err := append(ctx, msg)
	if err != nil {
		return msgID, err
	}

	switch msg.MessageType() {
	case message.MessageTypeCreateCollection:
		resource.Resource().Flusher().RegisterVChannel(msg.VChannel(), d.wal.Get())
	case message.MessageTypeDropCollection:
		// TODO: unregister vchannel, cannot unregister vchannel now.
		// Wait for PR: https://github.com/milvus-io/milvus/pull/37176
	}
	return msgID, nil
}

// Close implements BasicInterceptor.
func (d *ddlAppendInterceptor) Close() {}
