package function

/*
#cgo pkg-config: milvus_core

#include <stdint.h>
#include <stdlib.h>
#include "segcore/minhash_c.h"
#include "segcore/tokenizer_c.h"
*/
import "C"

import (
	"encoding/binary"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/internal/util/analyzer/canalyzer"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// MinHashFunctionRunner
// Input: string (text)
// Output: []byte (binary vector - MinHash signature vector)
const (
	// outter  parameters
	NumHashesKey   = "num_hashes"
	ShingleSizeKey = "shingle_size"
	HashFuncKey    = "hash_function"
	TokenLevelKey  = "token_level" // "char" for character-level n-grams, "word" for word-level (default)
	SeedKey        = "seed"
	// internal  parameters
	defaultShingleSize = 3
	defaultSeed        = 1234
)

// HashFunction type
type HashFunction int

const (
	// todo: support more hash functions
	HashFuncSHA1 HashFunction = iota
	HashFuncXXHash64
)

type MinHashFunctionRunner struct {
	mu     sync.RWMutex
	closed bool

	tokenizer   analyzer.Analyzer // word-level tokenizer
	funSchema   *schemapb.FunctionSchema
	inputField  *schemapb.FieldSchema
	outputField *schemapb.FieldSchema

	// MinHash specific parameters
	numHashes    int          // MinHash signature vector dimension
	shingleSize  int          // N-gram, N size
	hashFunc     HashFunction // Hash function to use
	useCharToken bool         // true: character-level n-grams, false: word-level tokens + shingles

	// Universal hash family parameters: h(x) = ((a * x + b) mod p) mod m
	// Each permutation has its own (a, b) pair
	permA []uint64 // 'a' (must be odd for full period)
	permB []uint64 // 'b'
}

func NewMinHashFunctionRunner(
	collSchema *schemapb.CollectionSchema,
	funSchema *schemapb.FunctionSchema,
) (FunctionRunner, error) {
	if len(funSchema.GetOutputFieldIds()) != 1 {
		return nil, merr.WrapErrParameterInvalidMsg("minhash function should only have one output field, but now %d", len(funSchema.GetOutputFieldIds()))
	}
	if len(funSchema.GetInputFieldIds()) != 1 {
		return nil, merr.WrapErrParameterInvalidMsg("minhash function should only have one input field, but now %d", len(funSchema.GetInputFieldIds()))
	}
	var inputField, outputField *schemapb.FieldSchema
	for _, field := range collSchema.GetFields() {
		if field.GetFieldID() == funSchema.GetOutputFieldIds()[0] {
			outputField = field
		}

		if field.GetFieldID() == funSchema.GetInputFieldIds()[0] {
			inputField = field
		}
	}
	if outputField == nil {
		return nil, merr.WrapErrParameterInvalidMsg("no output field")
	}
	if inputField == nil {
		return nil, merr.WrapErrParameterInvalidMsg("no input field")
	}

	params := getAnalyzerParams(inputField)
	tokenizer, err := analyzer.NewAnalyzer(params, "")
	if err != nil {
		return nil, err
	}

	numHashes := 0
	shingleSize := defaultShingleSize
	hashFunc := HashFuncXXHash64 // Default to xxHash for better performance
	useCharToken := false        // Default to word-level Token
	seed := defaultSeed
	var permA, permB []uint64

	for _, param := range funSchema.GetParams() {
		switch strings.ToLower(param.GetKey()) {
		case NumHashesKey:
			val, err := strconv.ParseInt(param.GetValue(), 10, 64)
			if err != nil {
				return nil, merr.WrapErrParameterInvalidMsg("param num_hashes:%s is not a number", param.GetValue())
			}
			if val <= 0 {
				return nil, merr.WrapErrParameterInvalidMsg("param num_hashes:%d must be positive", val)
			}
			numHashes = int(val)
		case ShingleSizeKey:
			val, err := strconv.ParseInt(param.GetValue(), 10, 64)
			if err != nil {
				return nil, merr.WrapErrParameterInvalidMsg("param shingle_size:%s is not a number", param.GetValue())
			}
			if val <= 0 {
				return nil, merr.WrapErrParameterInvalidMsg("param shingle_size:%d must be positive", val)
			}
			shingleSize = int(val)
		case HashFuncKey:
			switch strings.ToLower(param.GetValue()) {
			case "xxhash", "xxhash64":
				hashFunc = HashFuncXXHash64
			case "sha1":
				hashFunc = HashFuncSHA1
			default:
				return nil, merr.WrapErrParameterInvalidMsg("unknown hash function: %s", param.GetValue())
			}
		case TokenLevelKey:
			switch strings.ToLower(param.GetValue()) {
			case "char", "character":
				useCharToken = true
			case "word":
				useCharToken = false
			default:
				return nil, merr.WrapErrParameterInvalidMsg("unknown token_level: %s (expected 'char' or 'word')", param.GetValue())
			}
		case SeedKey:
			val, err := strconv.ParseInt(param.GetValue(), 10, 64)
			if err != nil {
				return nil, merr.WrapErrParameterInvalidMsg("param seed:%s is not a number", param.GetValue())
			}
			seed = int(val)
		}
	}
	if numHashes <= 0 {
		// auto generate numHashes from output field dim
		var outputDim int64 = -1

		for _, param := range outputField.GetTypeParams() {
			if param.GetKey() == "dim" {
				val, err := strconv.ParseInt(param.GetValue(), 10, 64)
				if err == nil {
					outputDim = val
					break
				}
			}
		}
		if outputDim <= 0 || outputDim%32 != 0 {
			return nil, merr.WrapErrParameterInvalidMsg("minhash function output field '%s' dim not found or invalid(dim > 0, dim %% 32 == 0)", outputField.GetName())
		}
		numHashes = int(outputDim / 32)
		funSchema.Params = append(funSchema.Params, &commonpb.KeyValuePair{
			Key:   NumHashesKey,
			Value: strconv.Itoa(numHashes),
		})
	}
	// Initialize permutations
	permA, permB = initializePermutations(numHashes, int64(seed))

	runner := &MinHashFunctionRunner{
		tokenizer:    tokenizer,
		funSchema:    funSchema,
		inputField:   inputField,
		outputField:  outputField,
		numHashes:    numHashes,
		shingleSize:  shingleSize,
		hashFunc:     hashFunc,
		useCharToken: useCharToken,
		permA:        permA,
		permB:        permB,
	}

	return runner, nil
}

func ValidateMinHashFunction(collSchema *schemapb.CollectionSchema, funSchema *schemapb.FunctionSchema) error {
	var inputField, outputField *schemapb.FieldSchema

	// check input field count
	if len(funSchema.GetInputFieldNames()) != 1 {
		return merr.WrapErrParameterInvalidMsg("minhash function should only have one input field, but now %d", len(funSchema.GetInputFieldNames()))
	}
	if len(funSchema.GetOutputFieldNames()) != 1 {
		return merr.WrapErrParameterInvalidMsg("minhash function should only have one output field, but now %d", len(funSchema.GetOutputFieldNames()))
	}

	// Find fields by name (since FieldIDs may not be assigned yet during validation)
	inputFieldName := funSchema.GetInputFieldNames()[0]
	outputFieldName := funSchema.GetOutputFieldNames()[0]

	for _, field := range collSchema.GetFields() {
		if field.GetName() == inputFieldName {
			inputField = field
		}
		if field.GetName() == outputFieldName {
			outputField = field
		}
	}

	if inputField == nil {
		return merr.WrapErrParameterInvalidMsg("minhash function input field '%s' not found", inputFieldName)
	}
	if outputField == nil {
		return merr.WrapErrParameterInvalidMsg("minhash function output field '%s' not found", outputFieldName)
	}

	if inputField.GetDataType() != schemapb.DataType_VarChar && inputField.GetDataType() != schemapb.DataType_String {
		return merr.WrapErrParameterInvalidMsg("minhash function input field '%s' is not string type, is %s",
			inputFieldName, inputField.GetDataType())
	}
	// check function params
	numHashes := int(-1)
	for _, param := range funSchema.GetParams() {
		switch strings.ToLower(param.GetKey()) {
		case NumHashesKey:
			val, err := strconv.ParseInt(param.GetValue(), 10, 64)
			if err != nil {
				return merr.WrapErrParameterInvalidMsg("param num_hashes:%s is not a number", param.GetValue())
			}
			numHashes = int(val)
			if numHashes <= 0 {
				return merr.WrapErrParameterInvalidMsg("param num_hashes:%d must be positive", numHashes)
			}
		case ShingleSizeKey:
			val, err := strconv.ParseInt(param.GetValue(), 10, 64)
			if err != nil {
				return merr.WrapErrParameterInvalidMsg("param shingle_size:%s is not a number", param.GetValue())
			}
			if val <= 0 {
				return merr.WrapErrParameterInvalidMsg("param shingle_size:%d must be positive", val)
			}
		case HashFuncKey:
			switch strings.ToLower(param.GetValue()) {
			case "xxhash", "xxhash64", "sha1":
				// valid hash function
			default:
				return merr.WrapErrParameterInvalidMsg("unknown hash function: %s (expected 'xxhash64' or 'sha1')", param.GetValue())
			}
		case TokenLevelKey:
			switch strings.ToLower(param.GetValue()) {
			case "char", "character", "word":
				// valid token level
			default:
				return merr.WrapErrParameterInvalidMsg("unknown token_level: %s (expected 'char' or 'word')", param.GetValue())
			}
		case SeedKey:
			_, err := strconv.ParseInt(param.GetValue(), 10, 64)
			if err != nil {
				return merr.WrapErrParameterInvalidMsg("param seed:%s is not a number", param.GetValue())
			}
		}
	}
	// check numHashes with output field
	var outputDim int64 = -1
	if outputField.GetDataType() != schemapb.DataType_BinaryVector {
		return merr.WrapErrParameterInvalidMsg("minhash function output field '%s' is not binary vector type", outputFieldName)
	}
	for _, param := range outputField.GetTypeParams() {
		if param.GetKey() == "dim" {
			val, err := strconv.ParseInt(param.GetValue(), 10, 64)
			if err == nil {
				outputDim = val
				break
			}
		}
	}

	if numHashes > 0 {
		expectedDim := int64(numHashes * 32) // binary vector, each hash is 4 bytes (32 bits), but stored as 8 bits in binary vector
		if outputDim != expectedDim {
			return merr.WrapErrParameterInvalidMsg("minhash function output field '%s' dim %d does not match expected dim %d (numHashes %d * one minhash signature size of 32bit)", outputFieldName, outputDim, expectedDim, numHashes)
		}
	} else {
		if outputDim%32 != 0 {
			return merr.WrapErrParameterInvalidMsg("minhash function output field '%s' dim %d is not multiple of 32 (one minhash signature size)", outputFieldName, outputDim)
		}
	}
	// else no numHashes specified, skip output field validation
	return nil
}

func (m *MinHashFunctionRunner) run(data []string, dst [][]byte) error {
	// Clone the appropriate tokenizer based on mode
	var wordTokenizer analyzer.Analyzer
	var err error

	if !m.useCharToken {
		// Word-level mode: use word tokenizer
		wordTokenizer, err = m.tokenizer.Clone()
		if err != nil {
			return err
		}
		defer wordTokenizer.Destroy()
	}

	// Phase 1 & 2: Generate hashes and compute MinHash signatures
	var allSignatures [][]uint32
	// Everything happens in C++ to eliminate ALL CGO overhead:
	// - Tokenization/character processing in C++
	// - Shingle generation in C++
	// - Base hash computation in C++
	// - MinHash signature computation with rotation-based SIMD in C++

	var tokenizerPtr unsafe.Pointer
	if !m.useCharToken {
		// Word-level: get C tokenizer pointer
		tokenizerPtr = getTokenizerPtr(wordTokenizer)
	}
	// Char-level: tokenizerPtr is nil, C++ will process characters directly
	allSignatures, err = m.batchComputeMinHashFromTexts(data, tokenizerPtr)
	if err != nil {
		return err
	}

	// Phase 3: Batch convert to binary vectors
	batchSignatureToBinaryVector(allSignatures, dst)

	return nil
}

func (m *MinHashFunctionRunner) BatchRun(inputs ...any) ([]any, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, merr.WrapErrServiceInternalMsg("MinHash function closed")
	}

	if len(inputs) > 1 {
		return nil, merr.WrapErrParameterInvalidMsg("MinHash function received more than one input column")
	}

	text, ok := inputs[0].([]string)
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("MinHash function input not string list")
	}

	rowNum := len(text)
	signatures := make([][]byte, rowNum)

	concurrency := 8
	if rowNum < concurrency {
		concurrency = rowNum
	}
	wg := sync.WaitGroup{}
	errCh := make(chan error, concurrency)

	for i, j := 0, 0; i < concurrency && j < rowNum; i++ {
		start := j
		end := start + rowNum/concurrency
		if i < rowNum%concurrency {
			end += 1
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			err := m.run(text[start:end], signatures[start:end])
			if err != nil {
				errCh <- err
			}
		}()
		j = end
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return nil, err
		}
	}

	return []any{buildBinaryVectorFieldData(signatures)}, nil
}

func (v *MinHashFunctionRunner) GetSchema() *schemapb.FunctionSchema {
	return v.funSchema
}

func (m *MinHashFunctionRunner) GetOutputFields() []*schemapb.FieldSchema {
	return []*schemapb.FieldSchema{m.outputField}
}

func (v *MinHashFunctionRunner) GetInputFields() []*schemapb.FieldSchema {
	return []*schemapb.FieldSchema{v.inputField}
}

func (m *MinHashFunctionRunner) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.closed {
		if m.tokenizer != nil {
			m.tokenizer.Destroy()
		}
		m.closed = true
	}
}

func (m *MinHashFunctionRunner) batchComputeMinHashFromTexts(texts []string, tokenizerPtr unsafe.Pointer) ([][]uint32, error) {
	if len(texts) == 0 {
		return nil, nil
	}

	// Prepare text data - calculate total bytes needed
	totalBytes := 0
	for _, text := range texts {
		totalBytes += len(text)
	}

	// Allocate buffer for all texts
	cBuffer := C.malloc(C.size_t(totalBytes))
	defer C.free(cBuffer)

	// Prepare pointer and length arrays
	cTexts := make([]unsafe.Pointer, len(texts))
	textLengths := make([]int32, len(texts))

	// Copy texts into buffer
	cBufferSlice := unsafe.Slice((*byte)(cBuffer), totalBytes)
	bufferOffset := 0
	for i, text := range texts {
		textLen := len(text)
		if textLen > 0 {
			copy(cBufferSlice[bufferOffset:bufferOffset+textLen], text)
			cTexts[i] = unsafe.Pointer(&cBufferSlice[bufferOffset])
		} else {
			cTexts[i] = nil
		}
		textLengths[i] = int32(textLen)
		bufferOffset += textLen
	}

	// Allocate output buffer (flattened)
	flatSignatures := make([]uint32, len(texts)*m.numHashes)

	// Call C++ end-to-end implementation
	status := C.ComputeMinHashFromTexts(
		(**C.char)(unsafe.Pointer(&cTexts[0])),
		(*C.int32_t)(unsafe.Pointer(&textLengths[0])),
		C.int32_t(len(texts)),
		tokenizerPtr,
		C.int32_t(m.shingleSize),
		(*C.uint64_t)(unsafe.Pointer(&m.permA[0])),
		(*C.uint64_t)(unsafe.Pointer(&m.permB[0])),
		C.int32_t(m.hashFunc),
		C.int32_t(m.numHashes),
		(*C.uint32_t)(unsafe.Pointer(&flatSignatures[0])),
	)
	if status.error_code != 0 {
		errorMsg := C.GoString(status.error_msg)
		C.free(unsafe.Pointer(status.error_msg))
		return nil, merr.SegcoreError(int32(status.error_code), errorMsg)
	}

	// Convert flattened output to [][]uint32 using slicing (zero-copy view)
	signatures := make([][]uint32, len(texts))
	for i := 0; i < len(texts); i++ {
		start := i * m.numHashes
		end := start + m.numHashes
		signatures[i] = flatSignatures[start:end]
	}

	return signatures, nil
}

// helper function to get analyzer params
// getTokenizerPtr extracts the underlying C tokenizer pointer from an Analyzer
func getTokenizerPtr(a analyzer.Analyzer) unsafe.Pointer {
	if cAnalyzer, ok := a.(*canalyzer.CAnalyzer); ok {
		// Use reflection or provide a public method in CAnalyzer to get the pointer
		// For now, we'll need to add a public method to CAnalyzer
		return cAnalyzer.GetCPtr()
	}
	return nil
}

func initializePermutations(numHashes int, seed int64) ([]uint64, []uint64) {
	if numHashes <= 0 {
		return nil, nil
	}
	permA := make([]uint64, numHashes)
	permB := make([]uint64, numHashes)

	C.InitPermutations(
		C.int32_t(numHashes),
		C.uint64_t(seed),
		(*C.uint64_t)(unsafe.Pointer(&permA[0])),
		(*C.uint64_t)(unsafe.Pointer(&permB[0])),
	)
	return permA, permB
}

func signatureToBinaryVector(signature []uint32) []byte {
	byteLength := len(signature) * 4
	result := make([]byte, byteLength)
	i := 0
	for ; i+4 <= len(signature); i += 4 {
		offset := i * 4
		binary.LittleEndian.PutUint32(result[offset:offset+4], signature[i])
		binary.LittleEndian.PutUint32(result[offset+4:offset+8], signature[i+1])
		binary.LittleEndian.PutUint32(result[offset+8:offset+12], signature[i+2])
		binary.LittleEndian.PutUint32(result[offset+12:offset+16], signature[i+3])
	}
	for ; i < len(signature); i++ {
		hash := signature[i]
		offset := i * 4
		binary.LittleEndian.PutUint32(result[offset:offset+4], hash)
	}
	return result
}

// batchSignatureToBinaryVector converts multiple signatures to binary vectors in batch
// This improves cache locality and reduces function call overhead
func batchSignatureToBinaryVector(signatures [][]uint32, dst [][]byte) {
	if len(signatures) == 0 {
		return
	}

	signatureByteLen := len(signatures[0]) * 4

	for batchIdx := 0; batchIdx < len(signatures); batchIdx++ {
		signature := signatures[batchIdx]
		result := make([]byte, signatureByteLen)

		i := 0

		for ; i+4 <= len(signature); i += 4 {
			offset := i * 4
			binary.LittleEndian.PutUint32(result[offset:], signature[i])
			binary.LittleEndian.PutUint32(result[offset+4:], signature[i+1])
			binary.LittleEndian.PutUint32(result[offset+8:], signature[i+2])
			binary.LittleEndian.PutUint32(result[offset+12:], signature[i+3])
		}

		// Handle remaining elements
		for ; i < len(signature); i++ {
			offset := i * 4
			binary.LittleEndian.PutUint32(result[offset:], signature[i])
		}

		dst[batchIdx] = result
	}
}

func buildBinaryVectorFieldData(signatures [][]byte) *schemapb.FieldData {
	var dim int64
	var flatData []byte

	if len(signatures) > 0 {
		dim = int64(len(signatures[0]) * 8)
		flatData = make([]byte, 0, len(signatures)*len(signatures[0]))
		for _, sig := range signatures {
			flatData = append(flatData, sig...)
		}
	}

	return &schemapb.FieldData{
		Type: schemapb.DataType_BinaryVector,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: dim,
				Data: &schemapb.VectorField_BinaryVector{
					BinaryVector: flatData,
				},
			},
		},
	}
}
