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
	"fmt"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

const (
	Provider string = "provider"
)

const (
	OpenAIProvider string = "openai"
)

func getProvider(schema *schemapb.FunctionSchema) (string, error) {
	for _, param := range schema.Params {
		switch strings.ToLower(param.Key) {
		case Provider:
			return strings.ToLower(param.Value), nil
		default:
		}
	}
	return "", fmt.Errorf("The provider parameter was not found in the function's parameters")
}

func NewTextEmbeddingFunction(coll *schemapb.CollectionSchema, schema *schemapb.FunctionSchema) (*OpenAIEmbeddingFunction, error) {
	provider, err := getProvider(schema)
	if err != nil {
		return nil, err
	}
	if provider == OpenAIProvider {
		return NewOpenAIEmbeddingFunction(coll, schema)
	}
	return nil, fmt.Errorf("Provider: [%s] not exist, only supports [%s]", provider, OpenAIProvider)
}
