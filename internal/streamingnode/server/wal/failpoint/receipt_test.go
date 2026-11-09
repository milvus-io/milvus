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

package failpoint

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func receiptMessage(vchannel string) message.MutableMessage {
	return message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.InsertMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.InsertRequest{CollectionID: 1}).
		WithIdempotencyKey(message.NewCollectionScopedIdempotencyKey(1, "request")).
		MustBuildMutable().WithTimeTick(123)
}

func receiptConfig(t *testing.T, msg message.MutableMessage) Config {
	t.Helper()
	return Config{
		PID: os.Getpid(), Key: string(message.IdempotencyKeyOf(msg)),
		VChannel: msg.VChannel(), MessageType: msg.MessageType().String(),
		Action: "observe", Directory: t.TempDir(),
	}
}

func writeReceiptConfig(t *testing.T, config Config) string {
	t.Helper()
	data, err := json.Marshal(config)
	require.NoError(t, err)
	file := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, os.WriteFile(file, data, 0o600))
	return file
}

func TestHitMatchesRequest(t *testing.T) {
	msg := receiptMessage("v1")
	for _, tc := range []struct {
		name   string
		change func(*Config)
	}{
		{"pid", func(c *Config) { c.PID++ }},
		{"key", func(c *Config) { c.Key = "another-request" }},
		{"vchannel", func(c *Config) { c.VChannel = "v2" }},
		{"message type", func(c *Config) { c.MessageType = "Delete" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := receiptConfig(t, msg)
			tc.change(&config)
			require.False(t, Hit(t.Context(), writeReceiptConfig(t, config), BeforeIdempotencyAppend, msg, nil, nil))
			files, err := os.ReadDir(config.Directory)
			require.NoError(t, err)
			require.Empty(t, files, "unmatched requests must not claim the point or publish a receipt")
		})
	}
	require.False(t, Hit(t.Context(), filepath.Join(t.TempDir(), "missing"), BeforeIdempotencyAppend, msg, nil, nil))
}

func TestHitPublishesReceiptOncePerChannel(t *testing.T) {
	for _, action := range []string{"observe", "error"} {
		t.Run(action, func(t *testing.T) {
			msg := receiptMessage("v1")
			config := receiptConfig(t, msg)
			config.Action, config.VChannel = action, ""
			file := writeReceiptConfig(t, config)
			id := mock_message.NewMockMessageID(t)
			id.EXPECT().String().Return("message-42").Once()
			result := &messagespb.IdempotentInsertResult{
				Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{10, 20}}}},
				RowOffsets: []uint32{1, 3},
			}
			require.Equal(t, action == "error", Hit(t.Context(), file, AfterBackendAppend, msg, id, result))
			files, err := filepath.Glob(filepath.Join(config.Directory, "*.json"))
			require.NoError(t, err)
			require.Len(t, files, 1)
			data, err := os.ReadFile(files[0])
			require.NoError(t, err)
			var receipt Receipt
			require.NoError(t, json.Unmarshal(data, &receipt))
			require.Equal(t, Receipt{
				Key: config.Key, VChannel: "v1", MessageType: msg.MessageType().String(),
				PID: os.Getpid(), MessageID: "message-42", TimeTick: 123,
				IDs: []int64{10, 20}, Offsets: []uint32{1, 3},
			}, receipt)
			require.False(t, Hit(t.Context(), file, AfterBackendAppend, msg, id, result))
			again, err := os.ReadFile(files[0])
			require.NoError(t, err)
			require.Equal(t, data, again, "a retry must not overwrite the original receipt")
			require.Equal(t, action == "error", Hit(t.Context(), file, AfterBackendAppend, receiptMessage("v2"), nil, nil))
			files, err = filepath.Glob(filepath.Join(config.Directory, "*.json"))
			require.NoError(t, err)
			require.Len(t, files, 2, "another channel must be able to claim the point independently")
			temporary, err := filepath.Glob(filepath.Join(config.Directory, "*.tmp"))
			require.NoError(t, err)
			require.Empty(t, temporary)
		})
	}
}

func TestHitPauseReleaseAndCancellation(t *testing.T) {
	for _, cancelBarrier := range []bool{false, true} {
		name := "release"
		if cancelBarrier {
			name = "cancellation"
		}
		t.Run(name, func(t *testing.T) {
			msg := receiptMessage("v1")
			config := receiptConfig(t, msg)
			config.Action = "pause"
			file := writeReceiptConfig(t, config)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan bool, 1)
			go func() { done <- Hit(ctx, file, BeforeIdempotencyAppend, msg, nil, nil) }()
			require.Eventually(t, func() bool {
				files, err := filepath.Glob(filepath.Join(config.Directory, "*.json"))
				return err == nil && len(files) == 1
			}, 5*time.Second, 10*time.Millisecond)
			select {
			case <-done:
				t.Fatal("pause returned before release or cancellation")
			default:
			}
			if cancelBarrier {
				cancel()
			} else {
				require.NoError(t, os.WriteFile(filepath.Join(config.Directory, "release"), nil, 0o600))
			}
			select {
			case injectError := <-done:
				require.False(t, injectError)
			case <-time.After(5 * time.Second):
				t.Fatal("pause did not stop after release or cancellation")
			}
		})
	}
}

func TestHitRejectsInvalidConfiguration(t *testing.T) {
	msg := receiptMessage("v1")
	require.Panics(t, func() { Hit(t.Context(), 42, BeforeIdempotencyAppend, msg, nil, nil) })
	malformed := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, os.WriteFile(malformed, []byte("{"), 0o600))
	require.Panics(t, func() { Hit(t.Context(), malformed, BeforeIdempotencyAppend, msg, nil, nil) })
	for _, action := range []string{"unknown", "error"} {
		config := receiptConfig(t, msg)
		config.Action = action
		file := writeReceiptConfig(t, config)
		require.Panics(t, func() { Hit(t.Context(), file, BeforeIdempotencyAppend, msg, nil, nil) })
	}
}
