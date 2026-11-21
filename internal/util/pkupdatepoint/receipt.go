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
	return HoldInstance(ctx, value, channel, tick, segment, "", details)
}

// HoldInstance additionally matches an exact physical instance when armed.
func HoldInstance(ctx context.Context, value any, channel string, tick uint64, segment int64, instance string, details any) bool {
	return wait(ctx, value, channel, tick, segment, instance, details) != ""
}

// Armed reports whether this process and physical operation match the fixture.
func Armed(value any, channel string, tick uint64, segment int64, instance string) bool {
	return directory(value, channel, tick, segment, instance) != ""
}

func directory(value any, channel string, tick uint64, segment int64, instance string) string {
	path, ok := value.(string)
	if !ok {
		return ""
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	var arm struct {
		PID       int
		Channel   string
		Tick      uint64
		Segment   int64
		Instance  string
		Directory string
	}
	if json.Unmarshal(data, &arm) != nil || arm.PID != os.Getpid() || arm.Channel != channel ||
		(arm.Instance != "" && arm.Instance != instance) || tick < arm.Tick || (arm.Segment != 0 && segment != arm.Segment) {
		return ""
	}
	return arm.Directory
}

func wait(ctx context.Context, value any, channel string, tick uint64, segment int64, instance string, details any) string {
	dir := directory(value, channel, tick, segment, instance)
	if dir == "" {
		return ""
	}
	claim, err := os.OpenFile(filepath.Join(dir, "claim"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return ""
	}
	if claim.Close() != nil {
		return ""
	}
	event, err := json.Marshal(map[string]any{"pid": os.Getpid(), "channel": channel, "tick": tick, "segment": segment, "details": details})
	tmp := filepath.Join(dir, "receipt.tmp")
	if err != nil || os.WriteFile(tmp, event, 0o600) != nil || os.Rename(tmp, filepath.Join(dir, "receipt.json")) != nil {
		return ""
	}
	deadline := time.NewTimer(90 * time.Second)
	defer deadline.Stop()
	poll := time.NewTicker(20 * time.Millisecond)
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			return ""
		case <-deadline.C:
			return ""
		case <-poll.C:
			_, err := os.Stat(filepath.Join(dir, "release"))
			if err == nil {
				return dir
			}
			if !errors.Is(err, os.ErrNotExist) {
				return ""
			}
		}
	}
}

// Redeliver exercises a callback captured from the failed physical generation.
// Release is controlled by the service test after a replacement serves a read.
func Redeliver(value any, channel string, tick uint64, segment int64, details any, callback func()) {
	go func() {
		dir := wait(context.Background(), value, channel, tick, segment, "", details)
		if dir != "" {
			callback()
			_ = os.WriteFile(filepath.Join(dir, "completed"), []byte("delivered\n"), 0o600)
		}
	}()
}
