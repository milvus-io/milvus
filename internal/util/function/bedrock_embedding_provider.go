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
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type BedrockClient interface {
	InvokeModel(ctx context.Context, params *bedrockruntime.InvokeModelInput, optFns ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelOutput, error)
}

type BedrockEmbeddingProvider struct {
	fieldDim int64

	client        BedrockClient
	modelName     string
	embedDimParam int64
	normalize     bool

	maxBatch   int
	timeoutSec int
}

func createBedRockEmbeddingClient(awsAccessKeyId string, awsSecretAccessKey string, region string) (*bedrockruntime.Client, error) {
	if awsAccessKeyId == "" {
		return nil, fmt.Errorf("Missing credentials. Please pass `aws_access_key_id`, or configure the %s environment variable in the Milvus service.", bedrockAccessKeyId)
	}
	if awsSecretAccessKey == "" {
		return nil, fmt.Errorf("Missing credentials. Please pass `aws_secret_access_key`, or configure the %s environment variable in the Milvus service.", bedrockSAKEnvStr)
	}
	if region == "" {
		return nil, errors.New("Missing AWS Service region. Please pass `region` param.")
	}

	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			awsAccessKeyId, awsSecretAccessKey, "")),
	)
	if err != nil {
		return nil, err
	}

	return bedrockruntime.NewFromConfig(cfg), nil
}

func parseAccessInfo(params []*commonpb.KeyValuePair, confParams map[string]string) (string, string) {
	// function param > env > yaml
	var awsAccessKeyId, awsSecretAccessKey string

	// from function params
	if isEnableVerifiInfoInParamsKey(confParams) {
		for _, param := range params {
			switch strings.ToLower(param.Key) {
			case awsAKIdParamKey:
				awsAccessKeyId = param.Value
			case awsSAKParamKey:
				awsSecretAccessKey = param.Value
			}
		}
	}

	// from milvus.yaml
	if awsAccessKeyId == "" {
		awsAccessKeyId = confParams[awsAKIdParamKey]
	}
	if awsSecretAccessKey == "" {
		awsSecretAccessKey = confParams[awsSAKParamKey]
	}

	// from env
	if awsAccessKeyId == "" {
		awsAccessKeyId = os.Getenv(bedrockAccessKeyId)
	}
	if awsSecretAccessKey == "" {
		awsSecretAccessKey = os.Getenv(bedrockSAKEnvStr)
	}

	return awsAccessKeyId, awsSecretAccessKey
}

func NewBedrockEmbeddingProvider(fieldSchema *schemapb.FieldSchema, functionSchema *schemapb.FunctionSchema, c BedrockClient, params map[string]string) (*BedrockEmbeddingProvider, error) {
	fieldDim, err := typeutil.GetDim(fieldSchema)
	if err != nil {
		return nil, err
	}
	var region, modelName string
	var dim int64
	normalize := true

	for _, param := range functionSchema.Params {
		switch strings.ToLower(param.Key) {
		case modelNameParamKey:
			modelName = param.Value
		case dimParamKey:
			dim, err = parseAndCheckFieldDim(param.Value, fieldDim, fieldSchema.Name)
			if err != nil {
				return nil, err
			}
		case regionParamKey:
			region = param.Value
		case normalizeParamKey:
			switch strings.ToLower(param.Value) {
			case "false":
				normalize = false
			case "true":
				normalize = true
			default:
				return nil, fmt.Errorf("Illegal [%s:%s] param, ", normalizeParamKey, param.Value)
			}
		default:
		}
	}

	awsAccessKeyId, awsSecretAccessKey := parseAccessInfo(functionSchema.Params, params)

	var client BedrockClient
	if c == nil {
		client, err = createBedRockEmbeddingClient(awsAccessKeyId, awsSecretAccessKey, region)
		if err != nil {
			return nil, err
		}
	} else {
		client = c
	}

	return &BedrockEmbeddingProvider{
		client:        client,
		fieldDim:      fieldDim,
		modelName:     modelName,
		embedDimParam: dim,
		normalize:     normalize,
		maxBatch:      1,
		timeoutSec:    30,
	}, nil
}

func (provider *BedrockEmbeddingProvider) MaxBatch() int {
	// The bedrock model does not support batches, we support a small batch on the milvus side.
	return 12 * provider.maxBatch
}

func (provider *BedrockEmbeddingProvider) FieldDim() int64 {
	return provider.fieldDim
}

func (provider *BedrockEmbeddingProvider) CallEmbedding(texts []string, _ TextEmbeddingMode) (any, error) {
	numRows := len(texts)
	data := make([][]float32, 0, numRows)
	for i := 0; i < numRows; i += 1 {
		payload := BedRockRequest{
			InputText: texts[i],
			Normalize: provider.normalize,
		}
		if provider.embedDimParam != 0 {
			payload.Dimensions = provider.embedDimParam
		}

		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}

		output, err := provider.client.InvokeModel(context.Background(), &bedrockruntime.InvokeModelInput{
			Body:        payloadBytes,
			ModelId:     aws.String(provider.modelName),
			ContentType: aws.String("application/json"),
		})
		if err != nil {
			return nil, err
		}

		var resp BedRockResponse
		err = json.Unmarshal(output.Body, &resp)
		if err != nil {
			return nil, err
		}
		if len(resp.Embedding) != int(provider.fieldDim) {
			return nil, fmt.Errorf("The required embedding dim is [%d], but the embedding obtained from the model is [%d]",
				provider.fieldDim, len(resp.Embedding))
		}
		data = append(data, resp.Embedding)
	}
	return data, nil
}

type BedRockRequest struct {
	InputText  string `json:"inputText"`
	Dimensions int64  `json:"dimensions,omitempty"`
	Normalize  bool   `json:"normalize,omitempty"`
}

type BedRockResponse struct {
	Embedding           []float32 `json:"embedding"`
	InputTextTokenCount int       `json:"inputTextTokenCount"`
}
