// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build test

package segments

import (
	"runtime"
	"time"
)

// WaitForSchemaTransitionWriterForTest waits until a schema writer is queued
// behind an active insert transition reader. It is available only in test
// builds so cross-package regression tests can synchronize on the writer
// queue instead of using a timer as the success condition.
func (c *Collection) WaitForSchemaTransitionWriterForTest(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if !c.schemaTransitionMu.TryRLock() {
			return true
		}
		c.schemaTransitionMu.RUnlock()
		if time.Now().After(deadline) {
			return false
		}
		runtime.Gosched()
	}
}

// HasInsertSchemaTransitionReaderForTest reports whether an insert reader is
// holding the transition mutex. Callers must invoke it before starting a schema
// writer, so a failed TryLock unambiguously means a reader is active.
func (c *Collection) HasInsertSchemaTransitionReaderForTest() bool {
	if c.schemaTransitionMu.TryLock() {
		c.schemaTransitionMu.Unlock()
		return false
	}
	return true
}
