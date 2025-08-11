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

package milvusclient

import (
	"fmt"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
)

type SearchIteratorOption interface {
	// SearchOption returns the search option when iterate search
	SearchOption() *searchOption
	// Limit returns the overall limit of entries to iterate
	Limit() int64
	// ValidateParams performs the static params validation
	ValidateParams() error
}

type searchIteratorOption struct {
	*searchOption
	batchSize     int
	iteratorLimit int64
}

func (opt *searchIteratorOption) SearchOption() *searchOption {
	opt.annRequest.topK = opt.batchSize
	opt.WithSearchParam(IteratorSearchBatchSizeKey, fmt.Sprintf("%d", opt.batchSize))
	return opt.searchOption
}

func (opt *searchIteratorOption) Limit() int64 {
	return opt.iteratorLimit
}

// ValidateParams performs the static params validation
func (opt *searchIteratorOption) ValidateParams() error {
	if opt.batchSize <= 0 {
		return fmt.Errorf("batch size must be greater than 0")
	}
	return nil
}

func (opt *searchIteratorOption) WithBatchSize(batchSize int) *searchIteratorOption {
	opt.batchSize = batchSize
	return opt
}

func (opt *searchIteratorOption) WithPartitions(partitionNames ...string) *searchIteratorOption {
	opt.partitionNames = partitionNames
	return opt
}

func (opt *searchIteratorOption) WithFilter(expr string) *searchIteratorOption {
	opt.annRequest.WithFilter(expr)
	return opt
}

func (opt *searchIteratorOption) WithTemplateParam(key string, val any) *searchIteratorOption {
	opt.annRequest.WithTemplateParam(key, val)
	return opt
}

func (opt *searchIteratorOption) WithOffset(offset int) *searchIteratorOption {
	opt.annRequest.WithOffset(offset)
	return opt
}

func (opt *searchIteratorOption) WithOutputFields(fieldNames ...string) *searchIteratorOption {
	opt.outputFields = fieldNames
	return opt
}

func (opt *searchIteratorOption) WithConsistencyLevel(consistencyLevel entity.ConsistencyLevel) *searchIteratorOption {
	opt.consistencyLevel = consistencyLevel
	opt.useDefaultConsistencyLevel = false
	return opt
}

func (opt *searchIteratorOption) WithANNSField(annsField string) *searchIteratorOption {
	opt.annRequest.WithANNSField(annsField)
	return opt
}

func (opt *searchIteratorOption) WithGroupByField(groupByField string) *searchIteratorOption {
	opt.annRequest.WithGroupByField(groupByField)
	return opt
}

func (opt *searchIteratorOption) WithGroupSize(groupSize int) *searchIteratorOption {
	opt.annRequest.WithGroupSize(groupSize)
	return opt
}

func (opt *searchIteratorOption) WithStrictGroupSize(strictGroupSize bool) *searchIteratorOption {
	opt.annRequest.WithStrictGroupSize(strictGroupSize)
	return opt
}

func (opt *searchIteratorOption) WithIgnoreGrowing(ignoreGrowing bool) *searchIteratorOption {
	opt.annRequest.WithIgnoreGrowing(ignoreGrowing)
	return opt
}

func (opt *searchIteratorOption) WithAnnParam(ap index.AnnParam) *searchIteratorOption {
	opt.annRequest.WithAnnParam(ap)
	return opt
}

func (opt *searchIteratorOption) WithSearchParam(key, value string) *searchIteratorOption {
	opt.annRequest.WithSearchParam(key, value)
	return opt
}

// WithIteratorLimit sets the limit of entries to iterate
// if limit < 0, then it will be set to Unlimited
func (opt *searchIteratorOption) WithIteratorLimit(limit int64) *searchIteratorOption {
	if limit < 0 {
		limit = Unlimited
	}
	opt.iteratorLimit = limit
	return opt
}

func NewSearchIteratorOption(collectionName string, vector entity.Vector) *searchIteratorOption {
	return &searchIteratorOption{
		searchOption: NewSearchOption(collectionName, 1000, []entity.Vector{vector}).
			WithSearchParam(IteratorKey, "true").
			WithSearchParam(IteratorSearchV2Key, "true"),
		batchSize:     1000,
		iteratorLimit: Unlimited,
	}
}
