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
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAzureFile(t *testing.T) {
	t.Run("Read", func(t *testing.T) {
		data := []byte("Test data for Read.")
		azureFile := NewAzureFile(io.NopCloser(bytes.NewReader(data)))
		buffer := make([]byte, 4)
		n, err := azureFile.Read(buffer)
		assert.NoError(t, err)
		assert.Equal(t, 4, n)
		assert.Equal(t, "Test", string(buffer))

		buffer = make([]byte, 6)
		n, err = azureFile.Read(buffer)
		assert.NoError(t, err)
		assert.Equal(t, 6, n)
		assert.Equal(t, " data ", string(buffer))
	})

	t.Run("ReadAt", func(t *testing.T) {
		data := []byte("Test data for ReadAt.")
		azureFile := NewAzureFile(io.NopCloser(bytes.NewReader(data)))
		buffer := make([]byte, 4)
		n, err := azureFile.ReadAt(buffer, 5)
		assert.NoError(t, err)
		assert.Equal(t, 4, n)
		assert.Equal(t, "data", string(buffer))
	})

	t.Run("Seek start", func(t *testing.T) {
		data := []byte("Test data for Seek.")
		azureFile := NewAzureFile(io.NopCloser(bytes.NewReader(data)))
		offset, err := azureFile.Seek(10, io.SeekStart)
		assert.NoError(t, err)
		assert.Equal(t, int64(10), offset)
		buffer := make([]byte, 4)

		n, err := azureFile.Read(buffer)
		assert.NoError(t, err)
		assert.Equal(t, 4, n)
		assert.Equal(t, "for ", string(buffer))
	})

	t.Run("Seek current", func(t *testing.T) {
		data := []byte("Test data for Seek.")
		azureFile := NewAzureFile(io.NopCloser(bytes.NewReader(data)))

		buffer := make([]byte, 4)
		n, err := azureFile.Read(buffer)
		assert.NoError(t, err)
		assert.Equal(t, 4, n)
		assert.Equal(t, "Test", string(buffer))

		offset, err := azureFile.Seek(10, io.SeekCurrent)
		assert.NoError(t, err)
		assert.Equal(t, int64(14), offset)

		buffer = make([]byte, 4)
		n, err = azureFile.Read(buffer)
		assert.NoError(t, err)
		assert.Equal(t, 4, n)
		assert.Equal(t, "Seek", string(buffer))
	})
}
