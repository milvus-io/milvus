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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestAppendAndGetTimeTick(t *testing.T) {
	newMsg := func() message.MutableMessage {
		return message.NewTimeTickMessageBuilderV1().
			WithAllVChannel().
			WithHeader(&message.TimeTickMessageHeader{}).
			WithBody(&message.TimeTickMsg{}).
			MustBuildMutable()
	}
	failed := errors.New("failed")
	assign := func(tt uint64, err error) func(context.Context, message.MutableMessage) (message.MessageID, error) {
		return func(_ context.Context, msg message.MutableMessage) (message.MessageID, error) {
			if tt != 0 {
				msg.WithTimeTick(tt)
			}
			if err != nil {
				return nil, err
			}
			return walimplstest.NewTestMessageID(1), nil
		}
	}

	// Failed before the time tick was assigned.
	_, tt, err := AppendAndGetTimeTick(context.Background(), assign(0, failed), newMsg())
	assert.ErrorIs(t, err, failed)
	assert.Zero(t, tt)

	// Failed after the time tick was assigned.
	_, tt, err = AppendAndGetTimeTick(context.Background(), assign(7, failed), newMsg())
	assert.ErrorIs(t, err, failed)
	assert.Equal(t, uint64(7), tt)

	// A time tick left by an earlier call does not count.
	msg := newMsg().WithTimeTick(7)
	_, tt, err = AppendAndGetTimeTick(context.Background(), assign(0, failed), msg)
	assert.ErrorIs(t, err, failed)
	assert.Zero(t, tt)

	_, tt, err = AppendAndGetTimeTick(context.Background(), assign(9, nil), msg)
	assert.NoError(t, err)
	assert.Equal(t, uint64(9), tt)
}
