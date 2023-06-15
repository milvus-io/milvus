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

package importutil

import (
	"math"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

// Extra option keys to pass through import API
const (
	Bucket       = "bucket"   // the source files' minio bucket
	StartTs      = "start_ts" // start timestamp to filter data, only data between StartTs and EndTs will be imported
	EndTs        = "end_ts"   // end timestamp to filter data, only data between StartTs and EndTs will be imported
	OptionFormat = "start_ts: 10-digit physical timestamp, e.g. 1665995420, default 0 \n" +
		"end_ts: 10-digit physical timestamp, e.g. 1665995420, default math.MaxInt \n"
	BackupFlag = "backup"
)

type ImportOptions struct {
	OnlyValidate bool
	TsStartPoint uint64
	TsEndPoint   uint64
	IsBackup     bool // whether is triggered by backup tool
}

func DefaultImportOptions() ImportOptions {
	options := ImportOptions{
		OnlyValidate: false,
		TsStartPoint: 0,
		TsEndPoint:   math.MaxUint64,
	}
	return options
}

// ValidateOptions the options is illegal, return nil if illegal, return error if not.
// Illegal options:
//
//	start_ts: 10-digit physical timestamp, e.g. 1665995420
//	end_ts: 10-digit physical timestamp, e.g. 1665995420
func ValidateOptions(options []*commonpb.KeyValuePair) error {
	optionMap := funcutil.KeyValuePair2Map(options)
	// StartTs should be int
	_, ok := optionMap[StartTs]
	var startTs uint64
	var endTs uint64 = math.MaxInt64
	var err error
	if ok {
		startTs, err = strconv.ParseUint(optionMap[StartTs], 10, 64)
		if err != nil {
			return err
		}
	}
	// EndTs should be int
	_, ok = optionMap[EndTs]
	if ok {
		endTs, err = strconv.ParseUint(optionMap[EndTs], 10, 64)
		if err != nil {
			return err
		}
	}
	if startTs > endTs {
		return errors.New("start_ts shouldn't be larger than end_ts")
	}
	return nil
}

// ParseTSFromOptions get (start_ts, end_ts, error) from input options.
// return value will be composed to milvus system timestamp from physical timestamp
func ParseTSFromOptions(options []*commonpb.KeyValuePair) (uint64, uint64, error) {
	err := ValidateOptions(options)
	if err != nil {
		return 0, 0, err
	}
	var tsStart uint64
	var tsEnd uint64
	importOptions := funcutil.KeyValuePair2Map(options)
	value, ok := importOptions[StartTs]
	if ok {
		pTs, _ := strconv.ParseInt(value, 10, 64)
		tsStart = tsoutil.ComposeTS(pTs, 0)
	} else {
		tsStart = 0
	}
	value, ok = importOptions[EndTs]
	if ok {
		pTs, _ := strconv.ParseInt(value, 10, 64)
		tsEnd = tsoutil.ComposeTS(pTs, 0)
	} else {
		tsEnd = math.MaxUint64
	}
	return tsStart, tsEnd, nil
}

// IsBackup returns if the request is triggered by backup tool
func IsBackup(options []*commonpb.KeyValuePair) bool {
	isBackup, err := funcutil.GetAttrByKeyFromRepeatedKV(BackupFlag, options)
	if err != nil || strings.ToLower(isBackup) != "true" {
		return false
	}
	return true
}
