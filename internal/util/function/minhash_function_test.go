/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package function

import (
	"bytes"
	"encoding/binary"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// num_hashes = 2^59+1: (2^59+1)*32 wraps around int64 to 32, so a
// multiply-based dim check would accept it and pass the raw value to
// initializePermutations' slice allocation on first use.
func TestValidateMinHashFunctionRejectsNumHashesOverflowingDimCheck(t *testing.T) {
	collSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{Name: "text", DataType: schemapb.DataType_VarChar},
		{Name: "hash", DataType: schemapb.DataType_BinaryVector, TypeParams: []*commonpb.KeyValuePair{
			{Key: "dim", Value: "32"},
		}},
	}}
	funSchema := &schemapb.FunctionSchema{
		Name: "minhash", Type: schemapb.FunctionType_MinHash,
		InputFieldNames: []string{"text"}, OutputFieldNames: []string{"hash"},
		Params: []*commonpb.KeyValuePair{{Key: NumHashesKey, Value: "576460752303423489"}},
	}

	err := ValidateMinHashFunction(collSchema, funSchema)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.ErrorContains(t, err, "does not match expected dim")
}

// Function outputs are deliberately not retrievable through Query/Search. Keep
// signature-level assertions at the runner boundary instead of relying on that API.
func runMinHashTest(t *testing.T, texts []string, params map[string]string) []byte {
	t.Helper()
	numHashes := 16
	if value, ok := params[NumHashesKey]; ok {
		var err error
		numHashes, err = strconv.Atoi(value)
		require.NoError(t, err)
	}
	coll := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{Name: "text", FieldID: 100, DataType: schemapb.DataType_VarChar},
		{
			Name: "signature", FieldID: 101, DataType: schemapb.DataType_BinaryVector, IsFunctionOutput: true,
			TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: strconv.Itoa(numHashes * 32)}},
		},
	}}
	function := &schemapb.FunctionSchema{
		Name: "minhash", Type: schemapb.FunctionType_MinHash,
		InputFieldIds: []int64{100}, OutputFieldIds: []int64{101},
	}
	for key, value := range params {
		function.Params = append(function.Params, &commonpb.KeyValuePair{Key: key, Value: value})
	}
	runner, err := NewMinHashFunctionRunner(coll, function)
	require.NoError(t, err)
	defer runner.Close()
	output, err := runner.BatchRun(texts)
	require.NoError(t, err)
	require.Len(t, output, 1)
	field, ok := output[0].(*schemapb.FieldData)
	require.True(t, ok)
	require.Equal(t, schemapb.DataType_BinaryVector, field.GetType())
	require.EqualValues(t, numHashes*32, field.GetVectors().GetDim())
	data := field.GetVectors().GetBinaryVector()
	require.Len(t, data, len(texts)*numHashes*4)
	return data
}

func TestMinHashFunctionSignatureProperties(t *testing.T) {
	const text = "the quick brown fox jumps over the lazy dog"
	for _, tokenLevel := range []string{"char", "word"} {
		for _, hashFunction := range []string{"xxhash64", "sha1"} {
			t.Run(tokenLevel+"/"+hashFunction, func(t *testing.T) {
				params := map[string]string{SeedKey: "42", TokenLevelKey: tokenLevel, HashFuncKey: hashFunction}
				data := runMinHashTest(t, []string{text, text}, params)
				require.Equal(t, data[:64], data[64:])
				// Independent runners must produce the same signature too.
				require.Equal(t, data[:64], runMinHashTest(t, []string{text}, params))
			})
		}
	}

	t.Run("seeds", func(t *testing.T) {
		seen := make(map[string]bool)
		for _, seed := range []string{"1234", "42", "0", "999999"} {
			data := runMinHashTest(t, []string{text}, map[string]string{SeedKey: seed})
			require.False(t, seen[string(data)], "seed %s repeated a signature", seed)
			seen[string(data)] = true
		}
		require.Equal(t, runMinHashTest(t, []string{text}, nil),
			runMinHashTest(t, []string{text}, map[string]string{SeedKey: "1234"}))
	})
	t.Run("token levels", func(t *testing.T) {
		require.NotEqual(t, runMinHashTest(t, []string{text}, map[string]string{TokenLevelKey: "char"}),
			runMinHashTest(t, []string{text}, map[string]string{TokenLevelKey: "word"}))
	})
	t.Run("hash functions", func(t *testing.T) {
		require.NotEqual(t, runMinHashTest(t, []string{text}, map[string]string{HashFuncKey: "xxhash64"}),
			runMinHashTest(t, []string{text}, map[string]string{HashFuncKey: "sha1"}))
	})
	t.Run("empty and short text", func(t *testing.T) {
		data := runMinHashTest(t, []string{"", " ", "  ", "a", "ab", "abc"}, nil)
		require.Equal(t, bytes.Repeat([]byte{0xff}, 3*64), data[:3*64])
		for row := 3; row < 6; row++ {
			require.NotEqual(t, bytes.Repeat([]byte{0xff}, 64), data[row*64:(row+1)*64])
		}
	})
	t.Run("binary encoding", func(t *testing.T) {
		signature := []uint32{0x01020304, 0xffffffff, 0}
		encoded := signatureToBinaryVector(signature)
		require.Equal(t, []byte{4, 3, 2, 1, 255, 255, 255, 255, 0, 0, 0, 0}, encoded)
		for i, value := range signature {
			require.Equal(t, value, binary.LittleEndian.Uint32(encoded[i*4:]))
		}
		runMinHashTest(t, []string{text}, map[string]string{NumHashesKey: "8"})
	})
}

func TestMinHashFunctionCharSignatures(t *testing.T) {
	// Golden values from the independent Python xxHash/MT19937-64 reference
	// previously compared through raw Query output in the client tests.
	expected := [][]uint32{
		{
			420291426, 316837721, 514581153, 1241663375, 22040188, 48408789, 553090275, 952986231,
			147146527, 412698415, 183941347, 641178125, 1674576963, 342011049, 224130818, 224920081,
		},
		{
			821509702, 2131162617, 329166583, 241079645, 1586733937, 43573419, 606157031, 60037816,
			1275287656, 34392379, 309094149, 901619182, 673753560, 381323488, 565331532, 1089662188,
		},
	}
	data := runMinHashTest(t, []string{"hello world", "abcdefg"}, map[string]string{
		SeedKey: "42", TokenLevelKey: "char", ShingleSizeKey: "3", NumHashesKey: "16",
	})
	for row, signature := range expected {
		for i, value := range signature {
			require.Equal(t, value, binary.LittleEndian.Uint32(data[(row*16+i)*4:]), "row %d hash %d", row, i)
		}
	}
}
