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

package storage

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// A backup/binlog header is parsed from an in-memory buffer, so a raw read
// failure is the file's content -- not a transport fault. Reporting it as a data
// integrity error makes an immutable corrupt backup fail its import instead of
// being retried forever.
func TestNewBinlogReaderRejectsMalformedHeader(t *testing.T) {
	t.Run("truncated magic number", func(t *testing.T) {
		_, err := NewBinlogReader([]byte{1, 2, 3})
		require.Error(t, err)
		require.True(t, errors.Is(err, merr.ErrDataIntegrity), "got %v", err)
	})

	t.Run("truncated descriptor", func(t *testing.T) {
		buf := new(bytes.Buffer)
		require.NoError(t, binary.Write(buf, common.Endian, MagicNumber))
		_, err := NewBinlogReader(buf.Bytes())
		require.Error(t, err)
		require.True(t, errors.Is(err, merr.ErrDataIntegrity), "got %v", err)
	})
}
