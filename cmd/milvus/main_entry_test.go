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

package milvus

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Main must return normally on too few arguments, taking the usage branch
// rather than panicking or exiting. That is the contract a distribution's own
// main relies on.
func TestMainWithoutSubcommandReturns(t *testing.T) {
	Main([]string{"milvus"})
}

// Main must leave the caller's argument vector as it found it: a distribution
// may keep using its own os.Args after the call.
func TestMainDoesNotModifyCallerArgs(t *testing.T) {
	args := []string{"milvus", "unknown-command"}
	want := append([]string(nil), args...)
	Main(args)
	assert.Equal(t, want, args)
}
