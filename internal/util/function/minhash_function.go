package function

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/analyzer"
)

// MinHashFunctionRunner
// Input: string (text)
// Output: []byte (binary vector - MinHash signature vector)
const (
	NumHashesKey       = "num_hashes"
	ShingleSizeKey     = "shingle_size"
	HashFuncKey        = "hash_function"
	SeedKey            = "seed"
	PermutationsKey    = "permutations" // internal parameters, Serialized (a,b) parameters
	defaultNumHashs    = 128
	defaultShingleSize = 3
	defaultSeed        = 1

	// Mersenne prime for universal hashing: 2^61 - 1
	// Using a large prime to reduce collisions
	mersennePrime = (1 << 61) - 1
	maxHash32     = 0xFFFFFFFF
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

	tokenizer   analyzer.Analyzer
	funSchema   *schemapb.FunctionSchema
	inputField  *schemapb.FieldSchema
	outputField *schemapb.FieldSchema

	// MinHash specific parameters
	numHashes   int          // MinHash signature vector dimension
	shingleSize int          // N-gram, N size
	hashFunc    HashFunction // Hash function to use

	// Universal hash family parameters: h(x) = ((a * x + b) mod p) mod m
	// Each permutation has its own (a, b) pair
	permA []uint64 // 'a' coefficients (must be odd for full period)
	permB []uint64 // 'b' coefficients

	// Buffer pool for reducing allocations
	bufferPool sync.Pool
}

func NewMinHashFunctionRunner(
	collSchema *schemapb.CollectionSchema,
	funSchema *schemapb.FunctionSchema,
) (FunctionRunner, error) {
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
		return nil, errors.New("no output field")
	}

	params := getAnalyzerParams(inputField)
	tokenizer, err := analyzer.NewAnalyzer(params)
	if err != nil {
		return nil, err
	}

	numHashes := defaultNumHashs
	shingleSize := defaultShingleSize
	hashFunc := HashFuncXXHash64 // Default to xxHash for better performance
	seed := int64(defaultSeed)
	var permA, permB []uint64
	var hasPerms bool

	for _, param := range funSchema.GetParams() {
		switch strings.ToLower(param.GetKey()) {
		case NumHashesKey:
			val, err := strconv.ParseInt(param.GetValue(), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("Param num_hashes:%s is not a number", param.GetValue())
			}
			numHashes = int(val)
		case ShingleSizeKey:
			val, err := strconv.ParseInt(param.GetValue(), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("Param shingle_size:%s is not a number", param.GetValue())
			}
			shingleSize = int(val)
		case HashFuncKey:
			switch strings.ToLower(param.GetValue()) {
			case "xxhash", "xxhash64":
				hashFunc = HashFuncXXHash64
			case "sha1":
				hashFunc = HashFuncSHA1
			default:
				return nil, fmt.Errorf("Unknown hash function: %s", param.GetValue())
			}
		case SeedKey:
			val, err := strconv.ParseInt(param.GetValue(), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("Param seed:%s is not a number", param.GetValue())
			}
			seed = val
		case PermutationsKey:
			// Deserialize pre-computed permutations
			var err error
			permA, permB, err = DeserializePermutations(param.GetValue())
			if err != nil {
				return nil, fmt.Errorf("Failed to deserialize permutations: %w", err)
			}
			hasPerms = true
		}
	}

	// Initialize permutation parameters (a, b) for universal hashing
	// Use pre-computed if available, otherwise generate from seed
	if !hasPerms {
		permA, permB = InitPermutations(numHashes, seed)
	}

	runner := &MinHashFunctionRunner{
		tokenizer:   tokenizer,
		funSchema:   funSchema,
		inputField:  inputField,
		outputField: outputField,
		numHashes:   numHashes,
		shingleSize: shingleSize,
		hashFunc:    hashFunc,
		permA:       permA,
		permB:       permB,
	}

	// Initialize buffer pool
	runner.bufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 1024)
		},
	}

	return runner, nil
}

func InjectMinHashFunctionInternalParams(funSchema *schemapb.FunctionSchema) error {
	// check
	numHashes := defaultNumHashs
	seed := int64(defaultSeed)
	inject := false
	for _, param := range funSchema.GetParams() {
		if strings.ToLower(param.GetKey()) == PermutationsKey {
			inject = true
			break
		} else if strings.ToLower(param.GetKey()) == NumHashesKey {
			val, err := strconv.ParseInt(param.GetValue(), 10, 64)
			if err == nil {
				numHashes = int(val)
			}
		} else if strings.ToLower(param.GetKey()) == SeedKey {
			val, err := strconv.ParseInt(param.GetValue(), 10, 64)
			if err == nil {
				seed = val
			}
		}
	}
	if !inject {
		return nil
	}
	// extract params
	permA, permB := InitPermutations(numHashes, seed)
	permsEncoded := SerializePermutations(permA, permB)
	funSchema.Params = append(funSchema.Params, &commonpb.KeyValuePair{
		Key:   PermutationsKey,
		Value: permsEncoded,
	})
	return nil
}

// InitPermutations generates random (a, b) pairs for universal hashing
// Following the approach: h(x) = ((a * x + b) mod p) mod m
// where p is a large prime (Mersenne prime 2^61-1), and m is 2^32
func InitPermutations(numPerm int, seed int64) ([]uint64, []uint64) {
	rng := rand.New(rand.NewSource(seed))

	permA := make([]uint64, numPerm)
	permB := make([]uint64, numPerm)

	for i := 0; i < numPerm; i++ {
		// Generate 'a' in range [1, mersennePrime)
		// 'a' must be odd to ensure full period in modular arithmetic
		permA[i] = (rng.Uint64() % (mersennePrime - 1)) + 1
		if permA[i]%2 == 0 {
			permA[i] |= 1 // Ensure 'a' is odd
		}

		// Generate 'b' in range [0, mersennePrime)
		permB[i] = rng.Uint64() % mersennePrime
	}

	return permA, permB
}

// SerializePermutations serializes (a, b) parameters to base64 string
// Format: [numPerm(4 bytes)][a0(8 bytes)][b0(8 bytes)][a1][b1]...
func SerializePermutations(permA, permB []uint64) string {
	if len(permA) != len(permB) {
		return ""
	}

	numPerm := len(permA)
	// 4 bytes for numPerm + 16 bytes per permutation (8 for a, 8 for b)
	bufSize := 4 + numPerm*16
	buf := make([]byte, bufSize)

	// Write numPerm
	binary.LittleEndian.PutUint32(buf[0:4], uint32(numPerm))

	// Write (a, b) pairs
	offset := 4
	for i := 0; i < numPerm; i++ {
		binary.LittleEndian.PutUint64(buf[offset:offset+8], permA[i])
		binary.LittleEndian.PutUint64(buf[offset+8:offset+16], permB[i])
		offset += 16
	}

	// Encode to base64 for storage in protobuf string field
	return base64.StdEncoding.EncodeToString(buf)
}

// DeserializePermutations deserializes (a, b) parameters from base64 string
func DeserializePermutations(encoded string) ([]uint64, []uint64, error) {
	// Decode from base64
	buf, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to decode base64: %w", err)
	}

	// Check minimum size
	if len(buf) < 4 {
		return nil, nil, fmt.Errorf("invalid permutations data: too short")
	}

	// Read numPerm
	numPerm := int(binary.LittleEndian.Uint32(buf[0:4]))

	// Validate size
	expectedSize := 4 + numPerm*16
	if len(buf) != expectedSize {
		return nil, nil, fmt.Errorf("invalid permutations data: expected %d bytes, got %d", expectedSize, len(buf))
	}

	// Read (a, b) pairs
	permA := make([]uint64, numPerm)
	permB := make([]uint64, numPerm)

	offset := 4
	for i := 0; i < numPerm; i++ {
		permA[i] = binary.LittleEndian.Uint64(buf[offset : offset+8])
		permB[i] = binary.LittleEndian.Uint64(buf[offset+8 : offset+16])
		offset += 16
	}

	return permA, permB, nil
}

// CreateMinHashFunctionSchema creates a FunctionSchema with pre-computed permutations
// This ensures all components (Proxy, DataNode, QueryNode) use identical (a, b) parameters
func CreateMinHashFunctionSchema(
	name string,
	inputFieldID, outputFieldID int64,
	numHashes int,
	shingleSize int,
	hashFunc string,
	seed int64,
) (*schemapb.FunctionSchema, error) {
	// Generate permutations
	permA, permB := InitPermutations(numHashes, seed)

	// Serialize permutations
	permsEncoded := SerializePermutations(permA, permB)

	// Create function schema
	return &schemapb.FunctionSchema{
		Name:           name,
		Type:           schemapb.FunctionType_MinHash,
		InputFieldIds:  []int64{inputFieldID},
		OutputFieldIds: []int64{outputFieldID},
		Params: []*commonpb.KeyValuePair{
			{Key: NumHashesKey, Value: strconv.Itoa(numHashes)},
			{Key: ShingleSizeKey, Value: strconv.Itoa(shingleSize)},
			{Key: HashFuncKey, Value: hashFunc},
			{Key: SeedKey, Value: strconv.FormatInt(seed, 10)},
			{Key: PermutationsKey, Value: permsEncoded}, // Pre-computed permutations
		},
	}, nil
}

func (m *MinHashFunctionRunner) run(data []string, dst [][]byte) error {
	tokenizer, err := m.tokenizer.Clone()
	if err != nil {
		return err
	}
	defer tokenizer.Destroy()

	for i := 0; i < len(data); i++ {
		// 1. get tokens
		tokenStream := tokenizer.NewTokenStream(data[i])
		defer tokenStream.Destroy()

		tokens := []string{}
		for tokenStream.Advance() {
			tokens = append(tokens, tokenStream.Token())
		}

		// 2.  N-grams (shingles)
		shingles := generateShingles(tokens, m.shingleSize)

		// 3. compute MinHash signature with (a, b) permutations
		signature := m.computeMinHash(shingles)

		// 4. convert to  binary vector
		dst[i] = signatureToBinaryVector(signature)
	}

	return nil
}

func (m *MinHashFunctionRunner) BatchRun(inputs ...any) ([]any, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, errors.New("MinHash function closed")
	}

	text, ok := inputs[0].([]string)
	if !ok {
		return nil, errors.New("MinHash function input not string list")
	}

	rowNum := len(text)
	signatures := make([][]byte, rowNum)

	// 并发处理
	concurrency := int(8)
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

// generateShingles generates token-level n-grams (shingles) from a token array, to maintain semantic meaning for similarity computation.
// Note: This is different from Milvus's character-level n-gram (NgramTokenizer in Tantivy).
func generateShingles(tokens []string, shingleSize int) []string {
	if len(tokens) < shingleSize {
		return []string{strings.Join(tokens, " ")}
	}

	shingles := make([]string, 0, len(tokens)-shingleSize+1)
	for i := 0; i <= len(tokens)-shingleSize; i++ {
		shingle := strings.Join(tokens[i:i+shingleSize], " ")
		shingles = append(shingles, shingle)
	}
	return shingles
}

// computeMinHash computes MinHash signature with universal hashing
// Uses the formula: h_i(x) = ((a_i * hash(x) + b_i) mod p) mod m
func (m *MinHashFunctionRunner) computeMinHash(shingles []string) []uint32 {
	signature := make([]uint32, m.numHashes)

	// Initialize signature to max values
	for i := range signature {
		signature[i] = math.MaxUint32
	}

	// Process each shingle
	for _, shingle := range shingles {
		// Compute base hash value
		baseHash := m.baseHash(shingle)

		// Apply each permutation
		for i := 0; i < m.numHashes; i++ {
			// hash function: ((a * h + b) mod p) mod m
			permutedHash := m.permuteHash(baseHash, i)
			// Update signature with minimum
			if permutedHash < signature[i] {
				signature[i] = permutedHash
			}
		}
	}
	return signature
}

// baseHash computes the base hash value using the selected hash function
func (m *MinHashFunctionRunner) baseHash(text string) uint64 {
	switch m.hashFunc {
	case HashFuncXXHash64:
		return m.baseHashXXHash(text)
	case HashFuncSHA1:
		return m.baseHashSHA1(text)
	default:
		return m.baseHashXXHash(text)
	}
}

// baseHashSHA1 computes SHA1 hash (compatible with datasketch)
// Returns the first 64 bits of SHA1 hash for compatibility
func (m *MinHashFunctionRunner) baseHashSHA1(text string) uint64 {
	h := sha1.New()
	h.Write([]byte(text))
	digest := h.Sum(nil)
	// Return first 8 bytes (64 bits) as uint64, little-endian for datasketch compatibility
	return binary.LittleEndian.Uint64(digest[:8])
}

// baseHashXXHash computes xxHash64 with buffer pooling
func (m *MinHashFunctionRunner) baseHashXXHash(text string) uint64 {
	// Get buffer from pool
	buf := m.bufferPool.Get().([]byte)
	defer m.bufferPool.Put(buf)

	// Ensure buffer has enough capacity
	data := []byte(text)
	if cap(buf) < len(data) {
		buf = make([]byte, len(data))
	} else {
		buf = buf[:len(data)]
	}
	copy(buf, data)

	// Compute xxHash
	return xxhash.Sum64(buf)
}

// permuteHash applies universal hash transformation using (a, b) parameters
// Formula: ((a * hash + b) mod p) mod 2^32
func (m *MinHashFunctionRunner) permuteHash(hash uint64, permIndex int) uint32 {
	a := m.permA[permIndex]
	b := m.permB[permIndex]

	// Compute (a * hash + b) mod mersennePrime
	// Use 128-bit arithmetic to avoid overflow
	result := (a*hash + b) % mersennePrime

	// return lower 32 bits
	// todo: support different output types(int32, int64, etc.)
	return uint32(result & maxHash32)
}

func signatureToBinaryVector(signature []uint32) []byte {
	byteLength := len(signature) * 4
	result := make([]byte, byteLength)
	for i, hash := range signature {
		u8Hash := make([]byte, 4)
		binary.LittleEndian.PutUint32(u8Hash, hash)
		copy(result[i*4:(i+1)*4], u8Hash)
	}
	return result
}

func buildBinaryVectorFieldData(signatures [][]byte) *schemapb.FieldData {
	dim := len(signatures[0]) * 8

	flatData := make([]byte, 0, len(signatures)*len(signatures[0]))
	for _, sig := range signatures {
		flatData = append(flatData, sig...)
	}

	return &schemapb.FieldData{
		Type: schemapb.DataType_BinaryVector,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_BinaryVector{
					BinaryVector: flatData,
				},
			},
		},
	}
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
		m.tokenizer.Destroy()
		m.closed = true
	}
}
