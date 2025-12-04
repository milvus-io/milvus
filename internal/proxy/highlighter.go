package proxy

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	PreTagsKey             = "pre_tags"
	PostTagsKey            = "post_tags"
	HighlightSearchTextKey = "highlight_search_text"
	HighlightQueryKey      = "queries"
	FragmentOffsetKey      = "fragment_offset"
	FragmentSizeKey        = "fragment_size"
	FragmentNumKey         = "num_of_fragments"
	DefaultFragmentSize    = 100
	DefaultFragmentNum     = 5
	DefaultPreTag          = "<em>"
	DefaultPostTag         = "</em>"
)

type Highlighter interface {
	AsSearchPipelineOperator(t *searchTask) (operator, error)
	FieldIDs() []int64
}

// highlight task for one field
type highlightTask struct {
	*querypb.HighlightTask
	preTags  [][]byte
	postTags [][]byte
}

type highlightQuery struct {
	text          string
	fieldName     string
	highlightType querypb.HighlightQueryType
}

type LexicalHighlighter struct {
	tasks map[int64]*highlightTask // fieldID -> highlightTask
	// option for all highlight task
	// TODO: support set option for each task
	preTags         [][]byte
	postTags        [][]byte
	highlightSearch bool
	options         *querypb.HighlightOptions
	queries         []*highlightQuery
}

// add highlight task with search
// must used before addTaskWithQuery
func (h *LexicalHighlighter) addTaskWithSearchText(fieldID int64, fieldName string, analyzerName string, texts []string) error {
	_, ok := h.tasks[fieldID]
	if ok {
		return merr.WrapErrParameterInvalidMsg("not support hybrid search with highlight now. fieldID: %d", fieldID)
	}

	task := &highlightTask{
		preTags:  h.preTags,
		postTags: h.postTags,
		HighlightTask: &querypb.HighlightTask{
			FieldName: fieldName,
			FieldId:   fieldID,
			Options:   h.options,
		},
	}
	h.tasks[fieldID] = task

	task.Texts = texts
	task.SearchTextNum = int64(len(texts))
	if analyzerName != "" {
		task.AnalyzerNames = []string{}
		for i := 0; i < len(texts); i++ {
			task.AnalyzerNames = append(task.AnalyzerNames, analyzerName)
		}
	}
	return nil
}

func (h *LexicalHighlighter) addTaskWithQuery(fieldID int64, query *highlightQuery) {
	task, ok := h.tasks[fieldID]
	if !ok {
		task = &highlightTask{
			HighlightTask: &querypb.HighlightTask{
				Texts:     []string{},
				FieldId:   fieldID,
				FieldName: query.fieldName,
				Options:   h.options,
			},
			preTags:  h.preTags,
			postTags: h.postTags,
		}
		h.tasks[fieldID] = task
	}

	task.Texts = append(task.Texts, query.text)
	task.Queries = append(task.Queries, &querypb.HighlightQuery{
		Type: query.highlightType,
	})
}

func (h *LexicalHighlighter) initHighlightQueries(t *searchTask) error {
	// add query to highlight tasks
	for _, query := range h.queries {
		fieldID, ok := t.schema.MapFieldID(query.fieldName)
		if !ok {
			return merr.WrapErrParameterInvalidMsg("highlight field not found in schema: %s", query.fieldName)
		}
		h.addTaskWithQuery(fieldID, query)
	}
	return nil
}

func (h *LexicalHighlighter) AsSearchPipelineOperator(t *searchTask) (operator, error) {
	return newLexicalHighlightOperator(t, lo.Values(h.tasks))
}

func (h *LexicalHighlighter) FieldIDs() []int64 {
	return lo.Keys(h.tasks)
}

func NewLexicalHighlighter(highlighter *commonpb.Highlighter) (*LexicalHighlighter, error) {
	params := funcutil.KeyValuePair2Map(highlighter.GetParams())
	h := &LexicalHighlighter{
		tasks:   make(map[int64]*highlightTask),
		options: &querypb.HighlightOptions{},
	}

	// set pre_tags and post_tags
	if value, ok := params[PreTagsKey]; ok {
		tags := []string{}
		if err := json.Unmarshal([]byte(value), &tags); err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("unmarshal pre_tags as string array failed: %v", err)
		}
		if len(tags) == 0 {
			return nil, merr.WrapErrParameterInvalidMsg("pre_tags cannot be empty list")
		}

		h.preTags = make([][]byte, len(tags))
		for i, tag := range tags {
			h.preTags[i] = []byte(tag)
		}
	} else {
		h.preTags = [][]byte{[]byte(DefaultPreTag)}
	}

	if value, ok := params[PostTagsKey]; ok {
		tags := []string{}
		if err := json.Unmarshal([]byte(value), &tags); err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("unmarshal post_tags as string list failed: %v", err)
		}
		if len(tags) == 0 {
			return nil, merr.WrapErrParameterInvalidMsg("post_tags cannot be empty list")
		}
		h.postTags = make([][]byte, len(tags))
		for i, tag := range tags {
			h.postTags[i] = []byte(tag)
		}
	} else {
		h.postTags = [][]byte{[]byte(DefaultPostTag)}
	}

	// set fragment config
	if value, ok := params[FragmentSizeKey]; ok {
		fragmentSize, err := strconv.ParseInt(value, 10, 64)
		if err != nil || fragmentSize <= 0 {
			return nil, merr.WrapErrParameterInvalidMsg("invalid fragment_size: %s", value)
		}
		h.options.FragmentSize = fragmentSize
	} else {
		h.options.FragmentSize = DefaultFragmentSize
	}

	if value, ok := params[FragmentNumKey]; ok {
		fragmentNum, err := strconv.ParseInt(value, 10, 64)
		if err != nil || fragmentNum < 0 {
			return nil, merr.WrapErrParameterInvalidMsg("invalid num_of_fragments: %s", value)
		}
		h.options.NumOfFragments = fragmentNum
	} else {
		h.options.NumOfFragments = DefaultFragmentNum
	}

	if value, ok := params[FragmentOffsetKey]; ok {
		fragmentOffset, err := strconv.ParseInt(value, 10, 64)
		if err != nil || fragmentOffset < 0 {
			return nil, merr.WrapErrParameterInvalidMsg("invalid fragment_offset: %s", value)
		}
		h.options.FragmentOffset = fragmentOffset
	}

	if value, ok := params[HighlightSearchTextKey]; ok {
		enable, err := strconv.ParseBool(value)
		if err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("unmarshal highlight_search_text as bool failed: %v", err)
		}

		h.highlightSearch = enable
	}

	if value, ok := params[HighlightQueryKey]; ok {
		queries := []any{}
		if err := json.Unmarshal([]byte(value), &queries); err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("unmarshal highlight queries as json array failed: %v", err)
		}

		for _, query := range queries {
			m, ok := query.(map[string]any)
			if !ok {
				return nil, merr.WrapErrParameterInvalidMsg("unmarshal highlight queries failed: item in array is not json object")
			}

			text, ok := m["text"]
			if !ok {
				return nil, merr.WrapErrParameterInvalidMsg("unmarshal highlight queries failed: must set `text` in query")
			}

			textStr, ok := text.(string)
			if !ok {
				return nil, merr.WrapErrParameterInvalidMsg("unmarshal highlight queries failed: `text` must be string")
			}

			t, ok := m["type"]
			if !ok {
				return nil, merr.WrapErrParameterInvalidMsg("unmarshal highlight queries failed: must set `type` in query")
			}

			typeStr, ok := t.(string)
			if !ok {
				return nil, merr.WrapErrParameterInvalidMsg("unmarshal highlight queries failed: `type` must be string")
			}

			typeEnum, ok := querypb.HighlightQueryType_value[typeStr]
			if !ok {
				return nil, merr.WrapErrParameterInvalidMsg("unmarshal highlight queries failed: invalid highlight query type: %s", typeStr)
			}

			f, ok := m["field"]
			if !ok {
				return nil, merr.WrapErrParameterInvalidMsg("unmarshal highlight queries failed: must set `field` in query")
			}
			fieldStr, ok := f.(string)
			if !ok {
				return nil, merr.WrapErrParameterInvalidMsg("unmarshal highlight queries failed: `field` must be string")
			}

			h.queries = append(h.queries, &highlightQuery{
				text:          textStr,
				highlightType: querypb.HighlightQueryType(typeEnum),
				fieldName:     fieldStr,
			})
		}
	}
	return h, nil
}

type lexicalHighlightOperator struct {
	tasks        []*highlightTask
	fieldSchemas []*schemapb.FieldSchema
	lbPolicy     shardclient.LBPolicy
	scheduler    *taskScheduler

	collectionName string
	collectionID   int64
	dbName         string
}

func newLexicalHighlightOperator(t *searchTask, tasks []*highlightTask) (operator, error) {
	return &lexicalHighlightOperator{
		tasks:          tasks,
		lbPolicy:       t.lb,
		scheduler:      t.node.(*Proxy).sched,
		fieldSchemas:   typeutil.GetAllFieldSchemas(t.schema.CollectionSchema),
		collectionName: t.request.CollectionName,
		collectionID:   t.CollectionID,
		dbName:         t.request.DbName,
	}, nil
}

func (op *lexicalHighlightOperator) run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	result := inputs[0].(*milvuspb.SearchResults)
	datas := result.Results.GetFieldsData()
	req := &querypb.GetHighlightRequest{
		Topks: result.GetResults().GetTopks(),
		Tasks: lo.Map(op.tasks, func(task *highlightTask, _ int) *querypb.HighlightTask { return task.HighlightTask }),
	}

	for _, task := range req.GetTasks() {
		textFieldDatas, ok := lo.Find(datas, func(data *schemapb.FieldData) bool { return data.FieldId == task.GetFieldId() })
		if !ok {
			return nil, errors.Errorf("get highlight failed, text field not in output field %s: %d", task.GetFieldName(), task.GetFieldId())
		}
		texts := textFieldDatas.GetScalars().GetStringData().GetData()
		task.Texts = append(task.Texts, texts...)
		task.CorpusTextNum = int64(len(texts))
		field, ok := lo.Find(op.fieldSchemas, func(schema *schemapb.FieldSchema) bool {
			return schema.GetFieldID() == task.GetFieldId()
		})
		if !ok {
			return nil, errors.Errorf("get highlight failed, field not found in schema %s: %d", task.GetFieldName(), task.GetFieldId())
		}

		// if use multi analyzer
		// get analyzer field data
		helper := typeutil.CreateFieldSchemaHelper(field)
		if v, ok := helper.GetMultiAnalyzerParams(); ok {
			params := map[string]any{}
			err := json.Unmarshal([]byte(v), &params)
			if err != nil {
				return nil, errors.Errorf("get highlight failed, get invalid multi analyzer params-: %v", err)
			}
			analyzerField, ok := params["by_field"]
			if !ok {
				return nil, errors.Errorf("get highlight failed, get invalid multi analyzer params, no by_field")
			}

			analyzerFieldDatas, ok := lo.Find(datas, func(data *schemapb.FieldData) bool { return data.FieldName == analyzerField.(string) })
			if !ok {
				return nil, errors.Errorf("get highlight failed, analyzer field not in output field")
			}
			task.AnalyzerNames = append(task.AnalyzerNames, analyzerFieldDatas.GetScalars().GetStringData().GetData()...)
		}
	}

	task := &HighlightTask{
		ctx:                 ctx,
		lb:                  op.lbPolicy,
		Condition:           NewTaskCondition(ctx),
		GetHighlightRequest: req,
		collectionName:      op.collectionName,
		collectionID:        op.collectionID,
		dbName:              op.dbName,
	}
	if err := op.scheduler.dqQueue.Enqueue(task); err != nil {
		return nil, err
	}

	if err := task.WaitToFinish(); err != nil {
		return nil, err
	}

	rowNum := len(result.Results.GetScores())
	HighlightResults := []*commonpb.HighlightResult{}
	if rowNum != 0 {
		rowDatas := lo.Map(task.result.Results, func(result *querypb.HighlightResult, i int) *commonpb.HighlightData {
			return buildStringFragments(op.tasks[i/rowNum], i%rowNum, result.GetFragments())
		})

		for i, task := range req.GetTasks() {
			HighlightResults = append(HighlightResults, &commonpb.HighlightResult{
				FieldName: task.GetFieldName(),
				Datas:     rowDatas[i*rowNum : (i+1)*rowNum],
			})
		}
	}

	result.Results.HighlightResults = HighlightResults
	return []any{result}, nil
}

func buildStringFragments(task *highlightTask, idx int, frags []*querypb.HighlightFragment) *commonpb.HighlightData {
	startOffset := int(task.GetSearchTextNum()) + len(task.Queries)
	text := []rune(task.Texts[startOffset+idx])
	preTagsNum := len(task.preTags)
	postTagsNum := len(task.postTags)
	result := &commonpb.HighlightData{Fragments: make([]string, 0)}
	for _, frag := range frags {
		var fragString strings.Builder
		cursor := int(frag.GetStartOffset())
		for i := 0; i < len(frag.GetOffsets())/2; i++ {
			startOffset := int(frag.Offsets[i<<1])
			endOffset := int(frag.Offsets[(i<<1)+1])
			if cursor < startOffset {
				fragString.WriteString(string(text[cursor:startOffset]))
			}
			fragString.WriteString(string(task.preTags[i%preTagsNum]))
			fragString.WriteString(string(text[startOffset:endOffset]))
			fragString.WriteString(string(task.postTags[i%postTagsNum]))
			cursor = endOffset
		}
		if cursor < int(frag.GetEndOffset()) {
			fragString.WriteString(string(text[cursor:frag.GetEndOffset()]))
		}
		result.Fragments = append(result.Fragments, fragString.String())
	}
	return result
}
