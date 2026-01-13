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

package highlight

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/models"
)

type semanticHighlightProvider interface {
	highlight(ctx context.Context, query string, texts []string) ([][]string, [][]float32, error)
	maxBatch() int
}

type baseSemanticHighlightProvider struct {
	batchSize int
}

func (provider *baseSemanticHighlightProvider) maxBatch() int {
	return provider.batchSize
}

type SemanticHighlight struct {
	fieldNames map[int64]string
	fieldIDs   []int64
	provider   semanticHighlightProvider
	queries    []string
}

const (
	queryKeyName      string = "queries"
	inputFieldKeyName string = "input_fields"
)

func NewSemanticHighlight(collSchema *schemapb.CollectionSchema, params []*commonpb.KeyValuePair, conf map[string]string, extraInfo *models.ModelExtraInfo) (*SemanticHighlight, error) {
	queries := []string{}
	inputField := []string{}
	for _, param := range params {
		switch param.Key {
		case queryKeyName:
			if err := json.Unmarshal([]byte(param.Value), &queries); err != nil {
				return nil, fmt.Errorf("Parse queries failed, err: %v", err)
			}
		case inputFieldKeyName:
			if err := json.Unmarshal([]byte(param.Value), &inputField); err != nil {
				return nil, fmt.Errorf("Parse input_field failed, err: %v", err)
			}
		}
	}

	if len(queries) == 0 {
		return nil, fmt.Errorf("queries is required")
	}

	if len(inputField) == 0 {
		return nil, fmt.Errorf("input_field is required")
	}

	fieldIDMap := make(map[string]*schemapb.FieldSchema)
	fieldIDNameMap := make(map[int64]string)
	for _, field := range collSchema.Fields {
		fieldIDMap[field.Name] = field
		fieldIDNameMap[field.FieldID] = field.Name
	}

	fieldIDs := []int64{}
	for _, fieldName := range inputField {
		field, ok := fieldIDMap[fieldName]
		if !ok {
			return nil, fmt.Errorf("input_field %s not found", fieldName)
		}
		if field.DataType != schemapb.DataType_VarChar && field.DataType != schemapb.DataType_Text {
			return nil, fmt.Errorf("input_field %s is not a VarChar or Text field", fieldName)
		}

		fieldIDs = append(fieldIDs, field.FieldID)
	}

	// TODO: support other providers if have more providers
	provider, err := newZillizHighlightProvider(params, conf, extraInfo)
	if err != nil {
		return nil, err
	}

	return &SemanticHighlight{fieldNames: fieldIDNameMap, fieldIDs: fieldIDs, provider: provider, queries: queries}, nil
}

func (highlight *SemanticHighlight) FieldIDs() []int64 {
	return highlight.fieldIDs
}

func (highlight *SemanticHighlight) GetFieldName(id int64) string {
	return highlight.fieldNames[id]
}

func (highlight *SemanticHighlight) processOneQuery(ctx context.Context, query string, documents []string) ([][]string, [][]float32, error) {
	if len(documents) == 0 {
		return [][]string{}, [][]float32{}, nil
	}
	highlights, scores, err := highlight.provider.highlight(ctx, query, documents)
	if err != nil {
		return nil, nil, err
	}
	if len(highlights) != len(documents) || len(scores) != len(documents) {
		return nil, nil, fmt.Errorf("Highlights size must equal to documents size, but got highlights size [%d], scores size [%d], documents size [%d]", len(highlights), len(scores), len(documents))
	}

	return highlights, scores, nil
}

func (highlight *SemanticHighlight) Process(ctx context.Context, topks []int64, documents []string) ([][]string, [][]float32, error) {
	nq := len(topks)
	if len(highlight.queries) != nq {
		return nil, nil, fmt.Errorf("nq must equal to queries size, but got nq [%d], queries size [%d], queries: [%v]", nq, len(highlight.queries), highlight.queries)
	}
	if len(documents) == 0 {
		return [][]string{}, [][]float32{}, nil
	}

	highlights := make([][]string, 0, len(documents))
	scores := make([][]float32, 0, len(documents))
	start := int64(0)

	for i, query := range highlight.queries {
		size := topks[i]
		singleQueryHighlights, singleQueryScores, err := highlight.processOneQuery(ctx, query, documents[start:start+size])
		if err != nil {
			return nil, nil, err
		}
		highlights = append(highlights, singleQueryHighlights...)
		scores = append(scores, singleQueryScores...)
		start += size
	}
	return highlights, scores, nil
}
