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

package utility

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// AppendAndGetTimeTick calls appendOp with msg and also returns the time tick
// that this call assigned to msg, or 0 when it assigned none.
//
// It is for a caller above the timetick interceptor. That interceptor writes the
// allocated time tick into msg in place before the message is written, and no
// interceptor between the caller and it copies msg. So when the call fails:
//   - 0 means that msg was not written.
//   - A time tick X means that msg may have been written, and only at X: the
//     append below the timetick interceptor retries the same msg and never
//     allocates again.
//
// msg may carry a time tick from an earlier call, a redo for example, so only a
// changed time tick counts.
func AppendAndGetTimeTick(ctx context.Context, appendOp func(context.Context, message.MutableMessage) (message.MessageID, error), msg message.MutableMessage) (message.MessageID, uint64, error) {
	before, _ := message.AssignedTimeTick(msg)
	id, err := appendOp(ctx, msg)
	after, ok := message.AssignedTimeTick(msg)
	if !ok || after == before {
		return id, 0, err
	}
	return id, after, err
}
