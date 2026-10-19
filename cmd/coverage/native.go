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

package coverage

/*
#cgo pkg-config: milvus_core
int MilvusDumpCoverage(void);
*/
import "C"

import (
	"context"
	"os"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Start enables local coverage snapshots only when MILVUS_COVERAGE_DIR is set.
// Each server process must own a fresh output directory.
func Start(ctx context.Context) error {
	root := os.Getenv("MILVUS_COVERAGE_DIR")
	if root == "" {
		return nil
	}
	return start(ctx, root, func() error {
		if C.MilvusDumpCoverage() != 0 {
			return merr.WrapErrServiceInternalMsg("failed to flush native coverage diagnostics")
		}
		return nil
	})
}
