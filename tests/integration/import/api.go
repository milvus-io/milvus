// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importv2

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/distributed/proxy/httpserver"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/log"
)

func Import(collectionName string, partitionName string, files [][]string) (string, error) {
	postURL := fmt.Sprintf("https://localhost:19530/v1/%s", httpserver.VectorImportPath)
	req := &httpserver.ImportReq{
		CollectionName: collectionName,
		PartitionName:  partitionName,
		Files:          files,
	}
	postData, err := json.Marshal(req)
	if err != nil {
		return "", errors.New(fmt.Sprintf("Marshal import response body failed, err=%s", err.Error()))
	}

	response, err := http.Post(postURL, "application/json", bytes.NewBuffer(postData)) //nolint
	if err != nil {
		return "", errors.New(fmt.Sprintf("%s failed, err=%s", httpserver.VectorImportPath, err.Error()))
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return "", errors.New(fmt.Sprintf("read %s body failed, err=%s", httpserver.VectorImportPath, err.Error()))
	}

	log.Info("POST response", zap.String("method", httpserver.VectorImportPath),
		zap.String("response body", string(body)))
	return "", nil
}

func DescribeImport(requestID string) (*internalpb.GetImportProgressResponse, error) {
	response, err := http.Get(fmt.Sprintf("https://localhost:19530/v1/%s?requestID=%s",
		httpserver.VectorImportDescribePath, requestID))
	if err != nil {
		return nil, errors.New(fmt.Sprintf("%s failed, err=%s", httpserver.VectorImportDescribePath, err.Error()))
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		fmt.Println("Error reading GET response body:", err)
		return nil, errors.New(fmt.Sprintf("read %s body failed, err=%s", httpserver.VectorImportDescribePath, err.Error()))
	}
	log.Info("GET response", zap.String("method", httpserver.VectorImportDescribePath),
		zap.String("response body", string(body)))
	return nil, nil
}

func ListImports() (*internalpb.GetImportProgressResponse, error) {
	response, err := http.Get(fmt.Sprintf("https://localhost:19530/v1/%s", httpserver.VectorImportListPath))
	if err != nil {
		return nil, errors.New(fmt.Sprintf("%s failed, err=%s", httpserver.VectorImportListPath, err.Error()))
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		fmt.Println("Error reading GET response body:", err)
		return nil, errors.New(fmt.Sprintf("read %s body failed, err=%s", httpserver.VectorImportListPath, err.Error()))
	}
	log.Info("GET response", zap.String("method", httpserver.VectorImportListPath),
		zap.String("response body", string(body)))
	return nil, nil
}
