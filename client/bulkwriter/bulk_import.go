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

package bulkwriter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// ResponseBase is the common milvus restful response struct.
type ResponseBase struct {
	Status  int    `json:"status"`
	Message string `json:"message"`
}

// CheckStatus checks the response status and return error if not ok.
func (b ResponseBase) CheckStatus() error {
	if b.Status != 0 {
		return fmt.Errorf("bulk import return error, status: %d, message: %s", b.Status, b.Message)
	}
	return nil
}

type BulkImportOption struct {
	// milvus params
	URL            string `json:"-"`
	CollectionName string `json:"collectionName"`
	// optional in cloud api, use object url instead
	Files [][]string `json:"files,omitempty"`
	// optional params
	PartitionName string `json:"partitionName,omitempty"`
	APIKey        string `json:"-"`
	// cloud extra params
	ObjectURL string `json:"objectUrl,omitempty"`
	ClusterID string `json:"clusterId,omitempty"`
	AccessKey string `json:"accessKey,omitempty"`
	SecretKey string `json:"secretKey,omitempty"`

	// reserved extra options
	Options map[string]string `json:"options,omitempty"`
}

func (opt *BulkImportOption) GetRequest() ([]byte, error) {
	return json.Marshal(opt)
}

func (opt *BulkImportOption) WithPartition(partitionName string) *BulkImportOption {
	opt.PartitionName = partitionName
	return opt
}

func (opt *BulkImportOption) WithAPIKey(key string) *BulkImportOption {
	opt.APIKey = key
	return opt
}

func (opt *BulkImportOption) WithOption(key, value string) *BulkImportOption {
	if opt.Options == nil {
		opt.Options = make(map[string]string)
	}
	opt.Options[key] = value
	return opt
}

// NewBulkImportOption returns BulkImportOption for Milvus bulk import API.
func NewBulkImportOption(uri string,
	collectionName string,
	files [][]string,
) *BulkImportOption {
	return &BulkImportOption{
		URL:            uri,
		CollectionName: collectionName,
		Files:          files,
	}
}

// NewCloudBulkImportOption returns import option for cloud import API.
func NewCloudBulkImportOption(uri string,
	collectionName string,
	apiKey string,
	objectURL string,
	clusterID string,
	accessKey string,
	secretKey string,
) *BulkImportOption {
	return &BulkImportOption{
		URL:            uri,
		CollectionName: collectionName,
		APIKey:         apiKey,
		ObjectURL:      objectURL,
		ClusterID:      clusterID,
		AccessKey:      accessKey,
		SecretKey:      secretKey,
	}
}

type BulkImportResponse struct {
	ResponseBase
	Data struct {
		JobID string `json:"jobId"`
	} `json:"data"`
}

// BulkImport is the API wrapper for restful import API.
func BulkImport(ctx context.Context, option *BulkImportOption) (*BulkImportResponse, error) {
	url := option.URL + "/v2/vectordb/jobs/import/create"
	bs, err := option.GetRequest()
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bs))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if option.APIKey != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", option.APIKey))
	}

	result := &BulkImportResponse{}
	err = doPostRequest(req, result)
	if err != nil {
		return nil, err
	}
	return result, result.CheckStatus()
}

type ListImportJobsOption struct {
	URL            string `json:"-"`
	CollectionName string `json:"collectionName"`
	ClusterID      string `json:"clusterId,omitempty"`
	APIKey         string `json:"-"`
	PageSize       int    `json:"pageSize,omitempty"`
	CurrentPage    int    `json:"currentPage,omitempty"`
}

func (opt *ListImportJobsOption) WithAPIKey(key string) *ListImportJobsOption {
	opt.APIKey = key
	return opt
}

func (opt *ListImportJobsOption) WithPageSize(pageSize int) *ListImportJobsOption {
	opt.PageSize = pageSize
	return opt
}

func (opt *ListImportJobsOption) WithCurrentPage(currentPage int) *ListImportJobsOption {
	opt.CurrentPage = currentPage
	return opt
}

func (opt *ListImportJobsOption) GetRequest() ([]byte, error) {
	return json.Marshal(opt)
}

func NewListImportJobsOption(uri string, collectionName string) *ListImportJobsOption {
	return &ListImportJobsOption{
		URL:            uri,
		CollectionName: collectionName,
		CurrentPage:    1,
		PageSize:       10,
	}
}

type ListImportJobsResponse struct {
	ResponseBase
	Data *ListImportJobData `json:"data"`
}

type ListImportJobData struct {
	Records []*ImportJobRecord `json:"records"`
}

type ImportJobRecord struct {
	JobID          string `json:"jobId"`
	CollectionName string `json:"collectionName"`
	State          string `json:"state"`
	Progress       int64  `json:"progress"`
	Reason         string `json:"reason"`
}

func ListImportJobs(ctx context.Context, option *ListImportJobsOption) (*ListImportJobsResponse, error) {
	url := option.URL + "/v2/vectordb/jobs/import/list"
	bs, err := option.GetRequest()
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bs))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if option.APIKey != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", option.APIKey))
	}

	result := &ListImportJobsResponse{}
	if err := doPostRequest(req, result); err != nil {
		return nil, err
	}

	return result, result.CheckStatus()
}

type GetImportProgressOption struct {
	URL   string `json:"-"`
	JobID string `json:"jobId"`
	// optional
	ClusterID string `json:"clusterId"`
	APIKey    string `json:"-"`
}

func (opt *GetImportProgressOption) GetRequest() ([]byte, error) {
	return json.Marshal(opt)
}

func (opt *GetImportProgressOption) WithAPIKey(key string) *GetImportProgressOption {
	opt.APIKey = key
	return opt
}

func NewGetImportProgressOption(uri string, jobID string) *GetImportProgressOption {
	return &GetImportProgressOption{
		URL:   uri,
		JobID: jobID,
	}
}

func NewCloudGetImportProgressOption(uri string, jobID string, apiKey string, clusterID string) *GetImportProgressOption {
	return &GetImportProgressOption{
		URL:       uri,
		JobID:     jobID,
		APIKey:    apiKey,
		ClusterID: clusterID,
	}
}

type GetImportProgressResponse struct {
	ResponseBase
	Data *ImportProgressData `json:"data"`
}

type ImportProgressData struct {
	CollectionName string                  `json:"collectionName"`
	JobID          string                  `json:"jobId"`
	CompleteTime   string                  `json:"completeTime"`
	State          string                  `json:"state"`
	Progress       int64                   `json:"progress"`
	ImportedRows   int64                   `json:"importedRows"`
	TotalRows      int64                   `json:"totalRows"`
	Reason         string                  `json:"reason"`
	FileSize       int64                   `json:"fileSize"`
	Details        []*ImportProgressDetail `json:"details"`
}

type ImportProgressDetail struct {
	FileName     string `json:"fileName"`
	FileSize     int64  `json:"fileSize"`
	Progress     int64  `json:"progress"`
	CompleteTime string `json:"completeTime"`
	State        string `json:"state"`
	ImportedRows int64  `json:"importedRows"`
	TotalRows    int64  `json:"totalRows"`
}

func GetImportProgress(ctx context.Context, option *GetImportProgressOption) (*GetImportProgressResponse, error) {
	url := option.URL + "/v2/vectordb/jobs/import/describe"

	bs, err := option.GetRequest()
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(bs))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if option.APIKey != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", option.APIKey))
	}

	result := &GetImportProgressResponse{}
	if err := doPostRequest(req, result); err != nil {
		return nil, err
	}
	return result, result.CheckStatus()
}

func doPostRequest(req *http.Request, response any) error {
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respData, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(respData, response)
}
