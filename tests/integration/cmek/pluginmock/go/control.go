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

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

const fixtureControlEnv = "MILVUS_CMEK_FIXTURE_CONTROL"

type keyFault struct {
	Role         string `json:"role"`
	EZID         int64  `json:"ez_id"`
	CollectionID int64  `json:"collection_id"`
	Operation    string `json:"operation"`
	Caller       string `json:"caller,omitempty"`
}

func (c *fixtureCipher) key(ezID, collectionID int64, operation string) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	var stack []string
	if os.Getenv(fixtureControlEnv) != "" {
		stack = callerFunctions()
	}
	if c.unavailable(ezID, collectionID, operation, stack) {
		c.record(ezID, collectionID, operation, "unavailable", stack)
		return nil
	}
	key := c.keys[ezID]
	if len(key) == 0 && c.role != "datanode" {
		// These roles obtain their own EZK; DataNode must receive it in a task.
		key = deriveEZKey(ezID)
	}
	outcome := "available"
	if len(key) == 0 {
		outcome = "unregistered"
	}
	c.record(ezID, collectionID, operation, outcome, stack)
	return append([]byte(nil), key...)
}

func (c *fixtureCipher) unavailable(ezID, collectionID int64, operation string, stack []string) bool {
	dir := os.Getenv(fixtureControlEnv)
	if dir == "" {
		return false
	}
	// #nosec G703 -- The test harness owns this configured directory; the file name is fixed.
	data, err := os.ReadFile(filepath.Join(dir, "fault.json"))
	if os.IsNotExist(err) {
		return false
	}
	if err != nil {
		panic(fmt.Errorf("fixture cipher: read fault control: %w", err))
	}
	var fault keyFault
	if err := json.Unmarshal(data, &fault); err != nil {
		panic(fmt.Errorf("fixture cipher: decode fault control: %w", err))
	}
	if fault.Role != c.role || fault.EZID != ezID || fault.CollectionID != collectionID || fault.Operation != operation {
		return false
	}
	if fault.Caller == "" {
		return true
	}
	// The hook interface has no task-kind argument. A fixture-only stack filter
	// distinguishes independently scheduled stages without changing the host API
	// or replacing its scheduler, RPC, writer or reader.
	for _, function := range stack {
		if strings.Contains(function, fault.Caller) {
			return true
		}
	}
	return false
}

func callerFunctions() []string {
	pcs := make([]uintptr, 32)
	n := runtime.Callers(3, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	var functions []string
	for {
		frame, more := frames.Next()
		functions = append(functions, frame.Function)
		if !more {
			return functions
		}
	}
}

// record runs under the cipher mutex. Each process owns its own append-only
// file, so a reader can associate events with the cold DataNode and task stage.
// No key, EDEK, input data or plugin parameter is recorded.
func (c *fixtureCipher) record(ezID, collectionID int64, operation, outcome string, stack []string) {
	dir := os.Getenv(fixtureControlEnv)
	if dir == "" {
		return
	}
	nodeID, _ := strconv.ParseInt(os.Getenv("MILVUS_NODE_ID_FOR_TESTING"), 10, 64)
	event := struct {
		PID          int      `json:"pid"`
		NodeID       int64    `json:"node_id"`
		Role         string   `json:"role"`
		EZID         int64    `json:"ez_id"`
		CollectionID int64    `json:"collection_id"`
		Operation    string   `json:"operation"`
		Outcome      string   `json:"outcome"`
		Stack        []string `json:"stack,omitempty"`
	}{os.Getpid(), nodeID, c.role, ezID, collectionID, operation, outcome, stack}
	data, err := json.Marshal(event)
	if err != nil {
		panic(err)
	}
	// #nosec G703 -- The test harness owns this directory; the file name contains only this process's PID.
	file, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("go-%d.jsonl", os.Getpid())), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		panic(err)
	}
	_, writeErr := file.Write(append(data, '\n'))
	closeErr := file.Close()
	if writeErr != nil {
		panic(writeErr)
	}
	if closeErr != nil {
		panic(closeErr)
	}
}
