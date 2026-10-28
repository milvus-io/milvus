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

package datacoord

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storage"
	importcommon "github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCreateVerdictSeesMappedStorageErrorAsTerminal(t *testing.T) {
	// The DataNode serializes a chunk-manager construction failure through
	// merr.Status; DataCoord rebuilds it with merr.Error. A raw SDK error must
	// survive that round trip as a terminal typed IO error (ErrIoFailed).
	mapped := storage.ToMilvusIoError("bucket", errors.New("connection reset by peer"))
	round := merr.Error(merr.Status(mapped))
	require.True(t, importcommon.IsTerminalImportV3Err(round), "%v", round)
}
