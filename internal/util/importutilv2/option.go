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

package importutilv2

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

const (
	StartTs    = "start_ts"
	EndTs      = "end_ts"
	BackupFlag = "backup"
)

type Options []*commonpb.KeyValuePair

func ParseTimeRange(options Options) (uint64, uint64, error) {
	var tsStart uint64
	var tsEnd uint64
	importOptions := funcutil.KeyValuePair2Map(options)
	value, ok := importOptions[StartTs]
	if ok {
		pTs, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return 0, 0, merr.WrapErrImportFailed(fmt.Sprintf("parse start_ts failed, err=%s", err))
		}
		tsStart = tsoutil.ComposeTS(pTs, 0)
	} else {
		tsStart = 0
	}
	value, ok = importOptions[EndTs]
	if ok {
		pTs, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return 0, 0, merr.WrapErrImportFailed(fmt.Sprintf("parse end_ts failed, err=%s", err))
		}
		tsEnd = tsoutil.ComposeTS(pTs, 0)
	} else {
		tsEnd = math.MaxUint64
	}
	if tsStart > tsEnd {
		return 0, 0, merr.WrapErrImportFailed(
			fmt.Sprintf("start_ts shouldn't be larger than end_ts, start_ts:%d, end_ts:%d", tsStart, tsEnd))
	}
	return tsStart, tsEnd, nil
}

func IsBackup(options Options) bool {
	isBackup, err := funcutil.GetAttrByKeyFromRepeatedKV(BackupFlag, options)
	if err != nil || strings.ToLower(isBackup) != "true" {
		return false
	}
	return true
}
