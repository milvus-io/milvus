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
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func TestImportNewReader(t *testing.T) {
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)
	cm.EXPECT().Reader(mock.Anything, mock.Anything).Return(nil, merr.WrapErrImportFailed("io error"))

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
				AutoID:       false,
			},
		},
	}

	checkFunc := func(name string, req *internalpb.ImportFile, options []*commonpb.KeyValuePair) {
		_, err := NewReader(ctx, cm, schema, req, options, 1024, &indexpb.StorageConfig{})
		assert.Error(t, err)
		assert.True(t, strings.Contains(err.Error(), name))
	}

	// binlog import
	req := &internalpb.ImportFile{
		Paths: []string{},
	}
	options := []*commonpb.KeyValuePair{
		{
			Key:   BackupFlag,
			Value: "true",
		},
	}
	checkFunc("no insert binlogs to import", req, options)

	// illegal timestamp
	options = append(options, &commonpb.KeyValuePair{
		Key:   StartTs,
		Value: "abc",
	})
	req.Paths = append(req.Paths, "dummy")
	checkFunc(fmt.Sprintf("parse %s failed", StartTs), req, options)

	// no file to import
	options = []*commonpb.KeyValuePair{}
	req.Paths = []string{}
	checkFunc("no file to import", req, options)

	// inconsistent file type
	req = &internalpb.ImportFile{
		Paths: []string{"1.npy", "2.csv"},
	}
	checkFunc("inconsistency in file types", req, options)

	// accepts only one json file
	req = &internalpb.ImportFile{
		Paths: []string{"1.json", "2.json"},
	}
	checkFunc("accepts only one file", req, options)

	// json file
	req = &internalpb.ImportFile{
		Paths: []string{"1.json"},
	}
	checkFunc("io error", req, options)

	// accepts multiple numpy files
	req = &internalpb.ImportFile{
		Paths: []string{"1.npy", "2.npy"},
	}
	checkFunc("no file for field", req, options)

	// numpy file
	req = &internalpb.ImportFile{
		Paths: []string{"pk.npy"},
	}
	checkFunc("io error", req, options)

	// accepts only one parquet file
	req = &internalpb.ImportFile{
		Paths: []string{"1.parquet", "2.parquet"},
	}
	checkFunc("accepts only one file", req, options)

	// parquet file
	req = &internalpb.ImportFile{
		Paths: []string{"1.parquet"},
	}
	checkFunc("io error", req, options)

	// accepts only one csv file
	req = &internalpb.ImportFile{
		Paths: []string{"1.csv", "2.csv"},
	}
	checkFunc("accepts only one file", req, options)

	// csv file
	req = &internalpb.ImportFile{
		Paths: []string{"1.csv"},
	}
	checkFunc("io error", req, options)

	// illegal sep
	options = []*commonpb.KeyValuePair{
		{
			Key:   CSVSep,
			Value: "\n",
		},
	}
	checkFunc("unsupported csv separator", req, options)

	// invalid file type
	req = &internalpb.ImportFile{
		Paths: []string{"1.txt"},
	}
	checkFunc("unexpected file type", req, options)
}
