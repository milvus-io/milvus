// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package grpcclient

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/encoding"
)

func TestGrpcEncoder(t *testing.T) {
	data := "hello zstd algorithm!"
	var buf bytes.Buffer

	compressor := encoding.GetCompressor(Zstd)
	writer, err := compressor.Compress(&buf)
	assert.NoError(t, err)
	written, err := writer.Write([]byte(data))
	assert.NoError(t, err)
	assert.Equal(t, written, len(data))
	err = writer.Close()
	assert.NoError(t, err)

	reader, err := compressor.Decompress(bytes.NewReader(buf.Bytes()))
	assert.NoError(t, err)
	result := make([]byte, len(data))
	reader.Read(result)
	assert.Equal(t, data, string(result))
}

func BenchmarkGrpcCompressionPerf(b *testing.B) {
	data := "// Licensed to the LF AI & Data foundation under one\n// or more contributor license agreements. See the NOTICE file\n// distributed with this work for additional information\n// regarding copyright ownership. The ASF licenses this file\n// to you under the Apache License, Version 2.0 (the\n// \"License\"); you may not use this file except in compliance\n// with the License. You may obtain a copy of the License at\n//\n//\thttp://www.apache.org/licenses/LICENSE-2.0\n//\n// Unless required by applicable law or agreed to in writing, software\n// distributed under the License is distributed on an \"AS IS\" BASIS,\n// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n// See the License for the specific language governing permissions and\n// limitations under the License."
	compressor := encoding.GetCompressor(Zstd)

	// Reset the timer to exclude setup time from the measurements
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < 10; j++ {
			var buf bytes.Buffer
			writer, _ := compressor.Compress(&buf)
			writer.Write([]byte(data))
			writer.Close()

			compressedData := buf.Bytes()
			reader, _ := compressor.Decompress(bytes.NewReader(compressedData))
			var result bytes.Buffer
			result.ReadFrom(reader)
		}
	}
}
