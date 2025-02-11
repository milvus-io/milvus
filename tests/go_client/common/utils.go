package common

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strings"
	"time"

	"github.com/x448/float16"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/log"
)

var (
	letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	r           *rand.Rand
)

func init() {
	r = rand.New(rand.NewSource(time.Now().UnixNano()))
}

func GenRandomString(prefix string, n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[r.Intn(len(letterRunes))]
	}
	str := fmt.Sprintf("%s_%s", prefix, string(b))
	return str
}

// GenLongString gen invalid long string
func GenLongString(n int) string {
	var builder strings.Builder
	longString := "a"
	for i := 0; i < n; i++ {
		builder.WriteString(longString)
	}
	return builder.String()
}

func GenValidNames() []string {
	return []string{
		"a",
		"_",
		"_name",
		"_123",
		"name_",
		"_coll_123_",
	}
}

func GenInvalidNames() []string {
	invalidNames := []string{
		"",
		"   ",
		"(mn)",
		"中文",
		"%$#",
		"1",
		"[10]",
		"a  b",
		DefaultDynamicFieldName,
		GenLongString(MaxCollectionNameLen + 1),
	}
	return invalidNames
}

func GenFloatVector(dim int) []float32 {
	vector := make([]float32, 0, dim)
	for j := 0; j < dim; j++ {
		vector = append(vector, rand.Float32())
	}
	return vector
}

func GenFloat16Vector(dim int) []byte {
	ret := make([]byte, dim*2)
	for i := 0; i < dim; i++ {
		v := float16.Fromfloat32(rand.Float32()).Bits()
		binary.LittleEndian.PutUint16(ret[i*2:], v)
	}
	return ret
}

func GenBFloat16Vector(dim int) []byte {
	ret16 := make([]uint16, 0, dim)
	for i := 0; i < dim; i++ {
		f := rand.Float32()
		bits := math.Float32bits(f)
		bits >>= 16
		bits &= 0x7FFF
		ret16 = append(ret16, uint16(bits))
	}
	ret := make([]byte, len(ret16)*2)
	for i, value := range ret16 {
		binary.LittleEndian.PutUint16(ret[i*2:], value)
	}
	return ret
}

func GenBinaryVector(dim int) []byte {
	vector := make([]byte, dim/8)
	rand.Read(vector)
	return vector
}

func GenSparseVector(maxLen int) entity.SparseEmbedding {
	length := 1 + rand.Intn(1+maxLen)
	positions := make([]uint32, length)
	values := make([]float32, length)
	for i := 0; i < length; i++ {
		positions[i] = uint32(2*i + 1)
		values[i] = rand.Float32()
	}
	vector, err := entity.NewSliceSparseEmbedding(positions, values)
	if err != nil {
		log.Fatal("Generate vector failed %s", zap.Error(err))
	}
	return vector
}

// InvalidExprStruct invalid expr
type InvalidExprStruct struct {
	Expr   string
	ErrNil bool
	ErrMsg string
}

var InvalidExpressions = []InvalidExprStruct{
	{Expr: "id in [0]", ErrNil: true, ErrMsg: "fieldName(id) not found"},                                          // not exist field but no error
	{Expr: "int64 in not [0]", ErrNil: false, ErrMsg: "cannot parse expression"},                                  // wrong term expr keyword
	{Expr: "int64 < floatVec", ErrNil: false, ErrMsg: "not supported"},                                            // unsupported compare field
	{Expr: "floatVec in [0]", ErrNil: false, ErrMsg: "cannot be casted to FloatVector"},                           // value and field type mismatch
	{Expr: fmt.Sprintf("%s == 1", DefaultJSONFieldName), ErrNil: true, ErrMsg: ""},                                // hist empty
	{Expr: fmt.Sprintf("%s like 'a%%' ", DefaultJSONFieldName), ErrNil: true, ErrMsg: ""},                         // hist empty
	{Expr: fmt.Sprintf("%s like `a%%` ", DefaultJSONFieldName), ErrNil: false, ErrMsg: "cannot parse expression"}, // ``
	{Expr: fmt.Sprintf("%s > 1", DefaultDynamicFieldName), ErrNil: true, ErrMsg: ""},                              // hits empty
	{Expr: fmt.Sprintf("%s[\"dynamicList\"] == [2, 3]", DefaultDynamicFieldName), ErrNil: true, ErrMsg: ""},
	{Expr: fmt.Sprintf("%s['a'] == [2, 3]", DefaultJSONFieldName), ErrNil: true, ErrMsg: ""},      // json field not exist
	{Expr: fmt.Sprintf("%s['number'] == [2, 3]", DefaultJSONFieldName), ErrNil: true, ErrMsg: ""}, // field exist but type not match
	{Expr: fmt.Sprintf("%s[0] == [2, 3]", DefaultJSONFieldName), ErrNil: true, ErrMsg: ""},        // field exist but type not match
	{Expr: fmt.Sprintf("json_contains (%s['number'], 2)", DefaultJSONFieldName), ErrNil: true, ErrMsg: ""},
	{Expr: fmt.Sprintf("json_contains (%s['list'], [2])", DefaultJSONFieldName), ErrNil: true, ErrMsg: ""},
	{Expr: fmt.Sprintf("json_contains_all (%s['list'], 2)", DefaultJSONFieldName), ErrNil: false, ErrMsg: "operation element must be an array"},
	{Expr: fmt.Sprintf("JSON_CONTAINS_ANY (%s['list'], 2)", DefaultJSONFieldName), ErrNil: false, ErrMsg: "operation element must be an array"},
	{Expr: fmt.Sprintf("json_contains_aby (%s['list'], 2)", DefaultJSONFieldName), ErrNil: false, ErrMsg: "function json_contains_aby(json, int64_t) not found."},
	{Expr: fmt.Sprintf("json_contains_aby (%s['list'], 2)", DefaultJSONFieldName), ErrNil: false, ErrMsg: "function json_contains_aby(json, int64_t) not found."},
	{Expr: fmt.Sprintf("%s[-1] > %d", DefaultInt8ArrayField, TestCapacity), ErrNil: false, ErrMsg: "cannot parse expression"}, //  array[-1] >
	{Expr: fmt.Sprintf("%s[-1] > 1", DefaultJSONFieldName), ErrNil: false, ErrMsg: "invalid expression"},                      //  json[-1] >
}

// Language constants for text generation
const (
	English = "en"
	Chinese = "zh"
)

func GenText(lang string) string {
	englishTopics := []string{
		"information retrieval", "data mining", "machine learning",
		"natural language processing", "text analysis", "search engines",
		"document indexing", "query processing", "relevance ranking",
		"semantic search",
	}
	englishVerbs := []string{
		"is", "focuses on", "deals with", "involves", "combines",
		"utilizes", "improves", "enables", "enhances", "supports",
	}
	englishObjects := []string{
		"large datasets", "text documents", "user queries", "search results",
		"information needs", "relevance scores", "ranking algorithms",
		"index structures", "query expansion", "document collections",
	}

	chineseTopics := []string{
		"信息检索", "数据挖掘", "机器学习",
		"自然语言处理", "文本分析", "搜索引擎",
		"文档索引", "查询处理", "相关性排序",
		"语义搜索",
	}
	chineseVerbs := []string{
		"是", "专注于", "处理", "涉及", "结合",
		"利用", "改进", "实现", "提升", "支持",
	}
	chineseObjects := []string{
		"大规模数据集", "文本文档", "用户查询", "搜索结果",
		"信息需求", "相关性分数", "排序算法",
		"索引结构", "查询扩展", "文档集合",
	}

	var topic, verb, object string
	switch lang {
	case English:
		topic = englishTopics[rand.Intn(len(englishTopics))]
		verb = englishVerbs[rand.Intn(len(englishVerbs))]
		object = englishObjects[rand.Intn(len(englishObjects))]
		return fmt.Sprintf("%s %s %s", topic, verb, object)
	case Chinese:
		topic = chineseTopics[rand.Intn(len(chineseTopics))]
		verb = chineseVerbs[rand.Intn(len(chineseVerbs))]
		object = chineseObjects[rand.Intn(len(chineseObjects))]
		return fmt.Sprintf("%s%s%s", topic, verb, object)
	default:
		return "Unsupported language"
	}
}

func IsZeroValue(value interface{}) bool {
	return reflect.DeepEqual(value, reflect.Zero(reflect.TypeOf(value)).Interface())
}
