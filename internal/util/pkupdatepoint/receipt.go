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

// Package pkupdatepoint records one scoped event from a rewritten service fixture.
package pkupdatepoint

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"
)

// Hold claims one matching operation and records the physical event before
// waiting. Each arm uses a fresh directory; process exit needs no cleanup.
func Hold(ctx context.Context, value any, channel string, tick uint64, segment int64, details any) bool {
	path, ok := value.(string)
	if !ok {
		return false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	var arm struct {
		PID       int
		Channel   string
		Tick      uint64
		Segment   int64
		Directory string
	}
	if json.Unmarshal(data, &arm) != nil || arm.PID != os.Getpid() || arm.Channel != channel ||
		tick < arm.Tick || (arm.Segment != 0 && segment != arm.Segment) {
		return false
	}
	claim, err := os.OpenFile(filepath.Join(arm.Directory, "claim"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return false
	}
	if claim.Close() != nil {
		return false
	}
	event, err := json.Marshal(map[string]any{"pid": os.Getpid(), "channel": channel, "tick": tick, "segment": segment, "details": details})
	tmp := filepath.Join(arm.Directory, "receipt.tmp")
	if err != nil || os.WriteFile(tmp, event, 0o600) != nil || os.Rename(tmp, filepath.Join(arm.Directory, "receipt.json")) != nil {
		return false
	}
	deadline := time.NewTimer(90 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(20 * time.Millisecond)
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-deadline.C:
			return false
		case <-poll.C:
			_, err := os.Stat(filepath.Join(arm.Directory, "release"))
			if err == nil {
				return true
			}
			if !errors.Is(err, os.ErrNotExist) {
				return false
			}
		}
	}
}
