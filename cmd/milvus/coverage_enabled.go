//go:build servercoverage

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
	"context"

	"github.com/milvus-io/milvus/cmd/coverage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

func startServerCoverage(args []string) {
	// Only the actual server process owns the output, never its supervisor
	// or a short-lived version/stop command.
	if len(args) < 3 || args[1] != "run" {
		return
	}
	ctx := context.TODO()
	if err := coverage.Start(ctx); err != nil {
		mlog.Fatal(ctx, "initialize server coverage", mlog.Err(err))
	}
}
