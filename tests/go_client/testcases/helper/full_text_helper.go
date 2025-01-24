package helper

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/base"
)

// FullTextParams contains parameters for full text search collection
type FullTextParams struct {
	CollectionName     string
	TextFields         []string
	MaxLength          int64
	AnalyzerType       string
	EnableDynamicField bool
	IsPartitionKey     bool
}

// NewFullTextParams creates a new FullTextParams with default values
func NewFullTextParams() *FullTextParams {
	return &FullTextParams{
		CollectionName:     "full_text_search",
		TextFields:         []string{"text"},
		MaxLength:          1000,
		AnalyzerType:       "standard",
		EnableDynamicField: false,
		IsPartitionKey:     false,
	}
}

// TWithCollectionName sets collection name
func (p *FullTextParams) TWithCollectionName(name string) *FullTextParams {
	p.CollectionName = name
	return p
}

// TWithTextFields sets text fields
func (p *FullTextParams) TWithTextFields(fields []string) *FullTextParams {
	p.TextFields = fields
	return p
}

// TWithMaxLength sets max length for varchar fields
func (p *FullTextParams) TWithMaxLength(maxLength int64) *FullTextParams {
	p.MaxLength = maxLength
	return p
}

// TWithAnalyzerType sets analyzer type
func (p *FullTextParams) TWithAnalyzerType(analyzerType string) *FullTextParams {
	p.AnalyzerType = analyzerType
	return p
}

// TWithEnableDynamicField sets enable dynamic field flag
func (p *FullTextParams) TWithEnableDynamicField(enable bool) *FullTextParams {
	p.EnableDynamicField = enable
	return p
}

// TWithIsPartitionKey sets is partition key flag
func (p *FullTextParams) TWithIsPartitionKey(isPartitionKey bool) *FullTextParams {
	p.IsPartitionKey = isPartitionKey
	return p
}

// Language type for text generation
type Language int

const (
	English Language = iota
	Chinese
)

// GenerateRandomText generates random text for testing in the specified language
func GenerateRandomText(lang Language) string {
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

// FullTextDataOption contains options for generating test data
type FullTextDataOption struct {
	EmptyPercent int
	Language     string
	Nullable     bool
}

// NewFullTextDataOption creates a new FullTextDataOption with default values
func NewFullTextDataOption() *FullTextDataOption {
	return &FullTextDataOption{
		EmptyPercent: 0,
		Language:     "en",
		Nullable:     false,
	}
}

// TWithEmptyPercent sets empty percent
func (o *FullTextDataOption) TWithEmptyPercent(percent int) *FullTextDataOption {
	o.EmptyPercent = percent
	return o
}

// TWithLanguage sets language
func (o *FullTextDataOption) TWithLanguage(lang string) *FullTextDataOption {
	o.Language = lang
	return o
}

// TWithNullable sets nullable flag
func (o *FullTextDataOption) TWithNullable(nullable bool) *FullTextDataOption {
	o.Nullable = nullable
	return o
}

// FullTextPrepare contains methods for preparing full text search collection
type FullTextPrepare struct{}

// CreateCollection creates a collection with full text search capability
func (f *FullTextPrepare) CreateCollection(ctx context.Context, t *testing.T, mc *base.MilvusClient, params *FullTextParams) (*entity.Schema, error) {
	schema := entity.NewSchema()
	schema.WithName(params.CollectionName)
	schema.WithDescription("Collection for testing full text search")
	schema.WithAutoID(false)
	schema.WithDynamicFieldEnabled(params.EnableDynamicField)

	// Add ID field
	idField := entity.NewField().
		WithName("id").
		WithDataType(entity.FieldTypeInt64).
		WithIsPrimaryKey(true).
		WithIsAutoID(false)
	schema.WithField(idField)
	analyzerParams := map[string]any{"tokenizer": params.AnalyzerType}
	t.Logf("analyzer params: %+v", analyzerParams)

	for _, fieldName := range params.TextFields {
		// Create text field with analyzer enabled
		textField := entity.NewField().
			WithName(fieldName).
			WithDataType(entity.FieldTypeVarChar).
			WithMaxLength(params.MaxLength).
			WithIsPartitionKey(params.IsPartitionKey).
			WithEnableAnalyzer(true).
			WithAnalyzerParams(analyzerParams)

		schema.WithField(textField)

		// Create sparse vector field for BM25 embeddings
		sparseField := entity.NewField().
			WithName(fieldName + "_sparse").
			WithDataType(entity.FieldTypeSparseVector)
		schema.WithField(sparseField)

		// Create BM25 function
		bm25Function := entity.NewFunction().
			WithName(fieldName + "_bm25_emb").
			WithInputFields(fieldName).
			WithOutputFields(fieldName + "_sparse").
			WithType(entity.FunctionTypeBM25)
		schema.WithFunction(bm25Function)
	}

	err := mc.CreateCollection(ctx, client.NewCreateCollectionOption(params.CollectionName, schema))
	if err != nil {
		return nil, err
	}

	t.Cleanup(func() {
		err := mc.DropCollection(ctx, client.NewDropCollectionOption(params.CollectionName))
		if err != nil {
			t.Errorf("Failed to drop collection: %v", err)
		}
	})

	return schema, nil
}

// CreateIndex creates BM25 index for full text search
func (f *FullTextPrepare) CreateIndex(ctx context.Context, t *testing.T, mc *base.MilvusClient, schema *entity.Schema, dropRatioBuild string) error {
	for _, field := range schema.Fields {
		if field.Name == "id" {
			continue
		}

		// Check if field is a sparse vector field
		if field.DataType != entity.FieldTypeSparseVector {
			continue
		}

		// Check if this field is an output of a BM25 function
		isBM25Output := false
		for _, fn := range schema.Functions {
			if fn.Type == entity.FunctionTypeBM25 && fn.OutputFieldNames[0] == field.Name {
				isBM25Output = true
				break
			}
		}
		if !isBM25Output {
			continue
		}

		indexParams := index.NewGenericIndex(field.Name+"_sparse_emb_index", map[string]string{
			index.IndexTypeKey:  "SPARSE_INVERTED_INDEX",
			index.MetricTypeKey: "BM25",
			"drop_ratio_build":  dropRatioBuild,
		})
		createIndexTask, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(schema.CollectionName, field.Name, indexParams))
		if err != nil {
			return err
		}
		err = createIndexTask.Await(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

// InsertData inserts test data for full text search
func (f *FullTextPrepare) InsertData(ctx context.Context, t *testing.T, mc *base.MilvusClient, schema *entity.Schema, numRows int) error {
	return f.InsertDataWithOption(ctx, t, mc, schema, numRows, NewFullTextDataOption())
}

// InsertDataWithOption inserts test data with specified options
func (f *FullTextPrepare) InsertDataWithOption(ctx context.Context, t *testing.T, mc *base.MilvusClient, schema *entity.Schema, numRows int, option *FullTextDataOption) error {
	var columns []column.Column

	for _, field := range schema.Fields {
		if field.Name == "id" {
			idValues := make([]int64, numRows)
			for i := 0; i < numRows; i++ {
				idValues[i] = int64(i)
			}
			idColumn := column.NewColumnInt64(field.Name, idValues)
			columns = append(columns, idColumn)
			continue
		}

		if field.DataType == entity.FieldTypeVarChar {
			textValues := make([]string, numRows)
			var lang Language
			switch option.Language {
			case "chinese":
				lang = Chinese
			default:
				lang = English
			}
			for i := 0; i < numRows; i++ {
				if rand.Float32()*100 < float32(option.EmptyPercent) {
					if option.Nullable {
						textValues[i] = ""
					} else {
						textValues[i] = " "
					}
				} else {
					textValues[i] = GenerateRandomText(lang)
				}
			}
			textColumn := column.NewColumnVarChar(field.Name, textValues)
			columns = append(columns, textColumn)
			t.Logf("insert data: %v", textValues[:10])
		}
		// Note: We don't need to create sparse vector columns as they will be generated by BM25 function
	}
	insertOpt := client.NewColumnBasedInsertOption(schema.CollectionName).WithColumns(columns...)
	_, err := mc.Insert(ctx, insertOpt)
	return err
}

// Search performs full text search
func (f *FullTextPrepare) Search(ctx context.Context, t *testing.T, mc *base.MilvusClient, schema *entity.Schema, query string, topK int, outputFields []string) ([]client.ResultSet, error) {
	return f.SearchWithOption(ctx, t, mc, schema, query, topK, outputFields, "0.2", entity.ClStrong)
}

// SearchWithOption performs full text search with specified options
func (f *FullTextPrepare) SearchWithOption(ctx context.Context, t *testing.T, mc *base.MilvusClient, schema *entity.Schema, query string, topK int, outputFields []string, dropRatioSearch string, consistencyLevel entity.ConsistencyLevel) ([]client.ResultSet, error) {
	searchOpt := client.NewSearchOption(schema.CollectionName, topK, []entity.Vector{
		entity.Text(query),
	}).
		WithConsistencyLevel(consistencyLevel).
		WithOutputFields(outputFields...).
		WithSearchParam("drop_ratio_search", dropRatioSearch)
	return mc.Search(ctx, searchOpt)
}

var DefaultFullTextPrepare = &FullTextPrepare{}
