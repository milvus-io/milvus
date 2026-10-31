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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// Hit is called inside a rewritten failpoint marker in the instrumented binary.
// GO_FAILPOINTS supplies a config-file path. Receipts and releases use a private
// directory shared with the integration controller, so no HTTP listener is
// exposed. A missing config disarms the point without changing process state.
func Hit(ctx context.Context, value any, point string, msg message.MutableMessage, id message.MessageID, result *messagespb.IdempotentInsertResult) bool {
	configPath, ok := value.(string)
	if !ok {
		panic("WAL failpoint requires a config-file path")
	}
	data, err := os.ReadFile(configPath)
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	must(err)
	var config Config
	must(json.Unmarshal(data, &config))
	if config.PID != os.Getpid() || config.Key != string(message.IdempotencyKeyOf(msg)) ||
		(config.VChannel != "" && config.VChannel != msg.VChannel()) ||
		config.MessageType != msg.MessageType().String() {
		return false
	}
	if config.Action != "observe" && config.Action != "pause" && config.Action != "error" {
		panic("unknown WAL failpoint action")
	}
	if config.Action == "error" && point != AfterBackendAppend {
		panic("append errors must be injected below the backend retry loop")
	}
	hash := sha256.Sum256([]byte(msg.VChannel()))
	base := filepath.Join(config.Directory, hex.EncodeToString(hash[:]))
	claim, err := os.OpenFile(base+".claim", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		return false
	}
	must(err)
	must(claim.Close())
	receipt := Receipt{
		Key: config.Key, VChannel: msg.VChannel(), MessageType: msg.MessageType().String(),
		PID: os.Getpid(), IDs: result.GetIds().GetIntId().GetData(), Offsets: result.GetRowOffsets(),
	}
	if id != nil {
		receipt.MessageID = id.String()
		receipt.TimeTick = msg.TimeTick()
	}
	data, err = json.Marshal(receipt)
	must(err)
	must(os.WriteFile(base+".tmp", data, 0o600))
	must(os.Rename(base+".tmp", base+".json"))
	if config.Action != "pause" {
		return config.Action == "error"
	}
	// A bounded barrier cannot leave a failed test's process hung indefinitely.
	timer := time.NewTimer(2 * time.Minute)
	defer timer.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(filepath.Join(config.Directory, "release")); err == nil {
			return false
		} else if !errors.Is(err, os.ErrNotExist) {
			panic(err)
		}
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			panic("WAL failpoint barrier timed out")
		case <-ticker.C:
		}
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
