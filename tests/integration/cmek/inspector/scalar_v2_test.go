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

package inspector

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
)

type fixtureEncryptor struct{}

func (fixtureEncryptor) Encrypt(plainText []byte) ([]byte, error) {
	return append([]byte(nil), plainText...), nil
}

func TestInspectV2PreservesLargeEZID(t *testing.T) {
	const (
		expectedEZID int64 = 468599886057571311
		collectionID int64 = 11
		partitionID  int64 = 12
		segmentID    int64 = 13
		fieldID      int64 = 14
	)

	writer := storage.NewInsertBinlogWriter(
		schemapb.DataType_Int32,
		collectionID,
		partitionID,
		segmentID,
		fieldID,
		false,
		storage.WithWriterEncryptionContext(expectedEZID, []byte("fixture-edek"), fixtureEncryptor{}),
	)
	writer.SetEventTimeStamp(1, 2)

	eventWriter, err := writer.NextInsertEventWriter()
	require.NoError(t, err)
	require.NoError(t, eventWriter.AddInt32ToPayload([]int32{1}, nil))
	eventWriter.SetEventTimestamp(1, 2)
	writer.AddExtra("original_size", "4")
	require.NoError(t, writer.Finish())

	raw, err := writer.GetBuffer()
	require.NoError(t, err)
	require.NoError(t, InspectV2(raw, expectedEZID))
}
