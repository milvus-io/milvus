package function

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/stretchr/testify/suite"
)

// estimateJaccard estimates Jaccard similarity from two MinHash signatures
func estimateJaccard(x, y []uint32) float64 {
	if len(x) != len(y) {
		panic("Signatures must have same length")
	}
	matches := 0
	for i := range x {
		if x[i] == y[i] {
			matches++
		}
	}
	return float64(matches) / float64(len(x))
}

func TestMinHashFunctionRunnerSuite(t *testing.T) {
	suite.Run(t, new(MinHashFunctionRunnerSuite))
}

type MinHashFunctionRunnerSuite struct {
	suite.Suite
	schema    *schemapb.CollectionSchema
	funSchema *schemapb.FunctionSchema
}

func (s *MinHashFunctionRunnerSuite) SetupTest() {
	s.schema = &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "binary_vec", DataType: schemapb.DataType_BinaryVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "32768"}}},
		},
	}
	s.funSchema = &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_MinHash,
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"binary_vec"},
		Params: []*commonpb.KeyValuePair{
			{Key: "num_hashes", Value: "1024"},
			{Key: "seed", Value: "42"},
		},
	}
	err := ValidateMinHashFunction(s.schema, s.funSchema)
	s.NoError(err)
	err = InjectMinHashFunctionInternalParams(s.schema, s.funSchema)
	s.NoError(err)
}

func (s *MinHashFunctionRunnerSuite) TestValidateMinHashFunction() {
	// case 1: num_hashes size * 32 != binary dim, throw error
	schema1 := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "binary_vec", DataType: schemapb.DataType_BinaryVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "32768"}}},
		},
	}
	funSchema1 := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_MinHash,
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"binary_vec"},
		Params: []*commonpb.KeyValuePair{
			{Key: "num_hashes", Value: "100"},
			{Key: "seed", Value: "42"},
		},
	}

	err := ValidateMinHashFunction(schema1, funSchema1)
	s.Error(err)

	// case 2: no num_hashes specified
	funSchema2 := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_MinHash,
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"binary_vec"},
		Params: []*commonpb.KeyValuePair{
			{Key: "seed", Value: "42"},
		},
	}
	err = ValidateMinHashFunction(schema1, funSchema2)
	s.NoError(err)
	err = InjectMinHashFunctionInternalParams(schema1, funSchema2)
	s.NoError(err)
	runner, err := NewFunctionRunner(schema1, funSchema2)
	s.NoError(err)
	minHashRunner := runner.(*MinHashFunctionRunner)
	s.Equal(1024, minHashRunner.numHashes)
	s.NotEmpty(minHashRunner.permA)
	s.Equal(1024, len(minHashRunner.permA))
	s.Equal(1024, len(minHashRunner.permB))
}

func (s *MinHashFunctionRunnerSuite) TestInjectMinHashFunctionInternalParams() {
	runner, err := NewFunctionRunner(s.schema, s.funSchema)
	s.NoError(err)

	minHashRunner := runner.(*MinHashFunctionRunner)
	s.Equal(1024, minHashRunner.numHashes)
	s.Equal(3, minHashRunner.shingleSize)
	s.Equal(HashFuncXXHash64, minHashRunner.hashFunc)
	s.NotEmpty(minHashRunner.permA)

	s.Equal(1024, len(minHashRunner.permA))
	s.Equal(1024, len(minHashRunner.permB))
}

func (s *MinHashFunctionRunnerSuite) TestMinHashEmBeddingBasic() {
	_, err := NewFunctionRunner(s.schema, &schemapb.FunctionSchema{
		Name:          "test",
		Type:          schemapb.FunctionType_MinHash,
		InputFieldIds: []int64{101},
	})
	s.Error(err)

	runner, err := NewFunctionRunner(s.schema, s.funSchema)

	s.NoError(err)

	// test batch function run
	output, err := runner.BatchRun([]string{"test string", "test string 2"})
	s.NoError(err)

	fieldData, ok := output[0].(*schemapb.FieldData)
	s.True(ok)

	vectorField := fieldData.GetVectors()
	binaryVector := vectorField.GetBinaryVector()
	byteSize := 32768 / 8
	s.Equal(1, len(output))
	s.True(ok)
	s.Equal(byteSize*2, len(binaryVector))
	s.Equal(int64(byteSize), int64(vectorField.GetDim()/8))

	// return error because receive more than one field input
	_, err = runner.BatchRun([]string{}, []string{})
	s.Error(err)

	// return error because field not string
	_, err = runner.BatchRun([]int64{})
	s.Error(err)

	runner.Close()

	// run after close
	_, err = runner.BatchRun([]string{"test string", "test string 2"})
	s.Error(err)
}

func (s *MinHashFunctionRunnerSuite) TestInitPermutations() {
	numPerm := 128
	seed := int64(42)

	permA, permB := InitPermutations(numPerm, seed)

	// Verify correct number of permutations
	s.Equal(numPerm, len(permA))
	s.Equal(numPerm, len(permB))

	// Verify all 'a' values are odd and within range
	for i, a := range permA {
		s.True(a%2 == 1, "permA[%d] should be odd", i)
		s.True(a > 0 && a < mersennePrime, "permA[%d] out of range", i)
	}

	// Verify all 'b' values are within range
	for i, b := range permB {
		s.True(b >= 0 && b < mersennePrime, "permB[%d] out of range", i)
	}

	// Verify deterministic: same seed produces same permutations
	permA2, permB2 := InitPermutations(numPerm, seed)
	s.Equal(permA, permA2)
	s.Equal(permB, permB2)

	// Verify different seeds produce different permutations
	permA3, permB3 := InitPermutations(numPerm, seed+1)
	s.NotEqual(permA, permA3)
	s.NotEqual(permB, permB3)
}

func (s *MinHashFunctionRunnerSuite) TestSerializeDeserializePermutations() {
	numPerm := 128
	seed := int64(42)

	permA, permB := InitPermutations(numPerm, seed)

	encoded := SerializePermutations(permA, permB)
	s.NotEmpty(encoded)

	decodedA, decodedB, err := DeserializePermutations(encoded)
	s.NoError(err)

	s.Equal(permA, decodedA)
	s.Equal(permB, decodedB)
}

func (s *MinHashFunctionRunnerSuite) TestPermuteHash() {
	runner := &MinHashFunctionRunner{
		numHashes: 4,
	}
	runner.permA, runner.permB = InitPermutations(4, 1)

	baseHash := uint64(12345678)

	results := make(map[uint32]bool)
	for i := 0; i < runner.numHashes; i++ {
		result := runner.permuteHash(baseHash, i)
		s.False(results[result])
		results[result] = true
	}

	for i := 0; i < runner.numHashes; i++ {
		result1 := runner.permuteHash(baseHash, i)
		result2 := runner.permuteHash(baseHash, i)
		s.Equal(result1, result2)
	}

	baseHash2 := uint64(87654321)
	for i := 0; i < runner.numHashes; i++ {
		result1 := runner.permuteHash(baseHash, i)
		result2 := runner.permuteHash(baseHash2, i)
		s.NotEqual(result1, result2)
	}
}

func (s *MinHashFunctionRunnerSuite) TestBaseHashFunctions() {
	runner := &MinHashFunctionRunner{
		hashFunc: HashFuncSHA1,
	}

	text := "hello world"
	// Test sha1 hash
	hash1 := runner.baseHashSHA1(text)
	hash2 := runner.baseHashSHA1(text)
	s.Equal(hash1, hash2)

	// Test different text produces different hash
	hash3 := runner.baseHashSHA1("goodbye world")
	s.NotEqual(hash1, hash3)

	// Test xxHash
	runner.hashFunc = HashFuncXXHash64
	runner.bufferPool.New = func() interface{} {
		return make([]byte, 0, 1024)
	}

	hash4 := runner.baseHashXXHash(text)
	hash5 := runner.baseHashXXHash(text)
	s.Equal(hash4, hash5)
	s.NotEqual(hash1, hash4)
}

func (s *MinHashFunctionRunnerSuite) TestComputeMinHash() {
	runner := &MinHashFunctionRunner{
		numHashes: 128,
		hashFunc:  HashFuncXXHash64,
	}
	runner.permA, runner.permB = InitPermutations(128, 1)
	runner.bufferPool.New = func() interface{} {
		return make([]byte, 0, 1024)
	}

	shingles := []string{
		"the quick brown",
		"quick brown fox",
		"brown fox jumps",
	}

	signature := runner.computeMinHash(shingles)

	// Verify signature length
	s.Equal(128, len(signature))

	// Verify all values are less than MaxUint32
	for i, val := range signature {
		s.LessOrEqual(val, uint32(0xFFFFFFFF), "signature[%d] out of range", i)
	}

	// Verify deterministic
	signature2 := runner.computeMinHash(shingles)
	s.Equal(signature, signature2)

	// Verify different shingles produce different signature
	shingles2 := []string{
		"completely different text",
		"nothing in common",
	}
	signature3 := runner.computeMinHash(shingles2)
	s.NotEqual(signature, signature3)
}

func (s *MinHashFunctionRunnerSuite) TestMinHashJaccardSimilarity() {
	runner := &MinHashFunctionRunner{
		numHashes: 256,
		hashFunc:  HashFuncXXHash64,
	}
	runner.permA, runner.permB = InitPermutations(256, 42)
	runner.bufferPool.New = func() interface{} {
		return make([]byte, 0, 1024)
	}

	// Test case 1:
	shingles1 := []string{"a", "b", "c", "d"}
	sig1 := runner.computeMinHash(shingles1)
	sig2 := runner.computeMinHash(shingles1)

	similarity := estimateJaccard(sig1, sig2)
	s.InDelta(1.0, similarity, 0.01)

	// Test case 2:
	shingles2 := []string{"a", "b", "c"}
	shingles3 := []string{"x", "y", "z"}
	sig3 := runner.computeMinHash(shingles2)
	sig4 := runner.computeMinHash(shingles3)

	similarity2 := estimateJaccard(sig3, sig4)
	s.InDelta(0.0, similarity2, 0.01)

	// Test case 3:
	shingles4 := []string{"a", "b", "c", "d"}
	shingles5 := []string{"c", "d", "e", "f"}
	sig5 := runner.computeMinHash(shingles4)
	sig6 := runner.computeMinHash(shingles5)

	similarity3 := estimateJaccard(sig5, sig6)
	// Actual Jaccard = |{c,d}| / |{a,b,c,d,e,f}| = 2/6 = 0.333
	s.InDelta(0.333, similarity3, 0.15)
}
