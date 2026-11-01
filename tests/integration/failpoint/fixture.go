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

// Package failpoint provides subprocess fixtures for MiniCluster fault tests.
package failpoint

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// Run prepares one binary before any cluster starts. CI may prebuild it with
// make build-failpoint-fixture and set MILVUS_FAILPOINT_WORK_DIR. Direct go test
// (including the Dev CLI runner) uses the same build script once per package.
func Run(m *testing.M) int {
	fixture := os.Getenv("MILVUS_FAILPOINT_WORK_DIR")
	if fixture == "" {
		root := os.Getenv("MILVUS_WORK_DIR")
		if root == "" {
			fmt.Fprintln(os.Stderr, "MILVUS_WORK_DIR must identify the built checkout")
			return 1
		}
		var err error
		fixture, err = os.MkdirTemp("", "milvus-failpoint-")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		defer os.RemoveAll(fixture)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, "bash", filepath.Join(root, "scripts/build_failpoint_fixture.sh"), fixture)
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		if err := cmd.Run(); err != nil {
			fmt.Fprintln(os.Stderr, "build failpoint fixture:", err)
			return 1
		}
	}
	if _, err := os.Stat(filepath.Join(fixture, "failpoint-fixture")); err != nil {
		fmt.Fprintln(os.Stderr, "instrumented fixture required:", err)
		return 1
	}
	if err := os.Setenv("MILVUS_WORK_DIR", fixture); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return m.Run()
}
