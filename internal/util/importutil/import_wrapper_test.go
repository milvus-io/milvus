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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"math"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/mmap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

const (
	TempFilesPath = "/tmp/milvus_test/import/"
)

type MockChunkManager struct {
	readerErr  error
	size       int64
	sizeErr    error
	readBuf    map[string][]byte
	readErr    error
	listResult map[string][]string
	listErr    error
}

func (mc *MockChunkManager) RootPath() string {
	return TempFilesPath
}

func (mc *MockChunkManager) Path(ctx context.Context, filePath string) (string, error) {
	return "", nil
}

func (mc *MockChunkManager) Reader(ctx context.Context, filePath string) (storage.FileReader, error) {
	return nil, mc.readerErr
}

func (mc *MockChunkManager) Write(ctx context.Context, filePath string, content []byte) error {
	return nil
}

func (mc *MockChunkManager) MultiWrite(ctx context.Context, contents map[string][]byte) error {
	return nil
}

func (mc *MockChunkManager) Exist(ctx context.Context, filePath string) (bool, error) {
	return true, nil
}

func (mc *MockChunkManager) Read(ctx context.Context, filePath string) ([]byte, error) {
	if mc.readErr != nil {
		return nil, mc.readErr
	}

	val, ok := mc.readBuf[filePath]
	if !ok {
		return nil, errors.New("mock chunk manager: file path not found: " + filePath)
	}

	return val, nil
}

func (mc *MockChunkManager) MultiRead(ctx context.Context, filePaths []string) ([][]byte, error) {
	return nil, nil
}

func (mc *MockChunkManager) ListWithPrefix(ctx context.Context, prefix string, recursive bool) ([]string, []time.Time, error) {
	if mc.listErr != nil {
		return nil, nil, mc.listErr
	}

	result, ok := mc.listResult[prefix]
	if ok {
		return result, nil, nil
	}

	return nil, nil, nil
}

func (mc *MockChunkManager) ReadWithPrefix(ctx context.Context, prefix string) ([]string, [][]byte, error) {
	return nil, nil, nil
}

func (mc *MockChunkManager) ReadAt(ctx context.Context, filePath string, off int64, length int64) ([]byte, error) {
	return nil, nil
}

func (mc *MockChunkManager) Mmap(ctx context.Context, filePath string) (*mmap.ReaderAt, error) {
	return nil, nil
}

func (mc *MockChunkManager) Size(ctx context.Context, filePath string) (int64, error) {
	if mc.sizeErr != nil {
		return 0, mc.sizeErr
	}

	return mc.size, nil
}

func (mc *MockChunkManager) Remove(ctx context.Context, filePath string) error {
	return nil
}

func (mc *MockChunkManager) MultiRemove(ctx context.Context, filePaths []string) error {
	return nil
}

func (mc *MockChunkManager) RemoveWithPrefix(ctx context.Context, prefix string) error {
	return nil
}

type rowCounterTest struct {
	rowCount int
	callTime int
}

func createMockCallbackFunctions(t *testing.T, rowCounter *rowCounterTest) (AssignSegmentFunc, CreateBinlogsFunc, SaveSegmentFunc) {
	createBinlogFunc := func(fields BlockData, segmentID int64, partID int64) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {
		count := 0
		for _, data := range fields {
			assert.Less(t, 0, data.RowNum())
			if count == 0 {
				count = data.RowNum()
			} else {
				assert.Equal(t, count, data.RowNum())
			}
		}
		rowCounter.rowCount += count
		rowCounter.callTime++
		return nil, nil, nil
	}

	assignSegmentFunc := func(shardID int, partID int64) (int64, string, error) {
		return 100, "ch", nil
	}

	saveSegmentFunc := func(fieldsInsert []*datapb.FieldBinlog, fieldsStats []*datapb.FieldBinlog,
		segmentID int64, targetChName string, rowCount int64, partID int64) error {
		return nil
	}

	return assignSegmentFunc, createBinlogFunc, saveSegmentFunc
}

func Test_ImportWrapperNew(t *testing.T) {
	// NewDefaultFactory() use "/tmp/milvus" as default root path, and cannot specify root path
	// NewChunkManagerFactory() can specify the root path
	f := storage.NewChunkManagerFactory("local", storage.RootPath(TempFilesPath))
	ctx := context.Background()
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)
	wrapper := NewImportWrapper(ctx, nil, 1, nil, cm, nil, nil)
	assert.Nil(t, wrapper)

	schema := &schemapb.CollectionSchema{
		Name:        "schema",
		Description: "schema",
		AutoID:      true,
		Fields:      make([]*schemapb.FieldSchema, 0),
	}
	schema.Fields = append(schema.Fields, sampleSchema().Fields...)
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:      106,
		Name:         common.RowIDFieldName,
		IsPrimaryKey: true,
		AutoID:       false,
		Description:  "int64",
		DataType:     schemapb.DataType_Int64,
	})
	collectionInfo, err := NewCollectionInfo(schema, 2, []int64{1})
	assert.NoError(t, err)
	wrapper = NewImportWrapper(ctx, collectionInfo, 1, nil, cm, nil, nil)
	assert.NotNil(t, wrapper)

	assignSegFunc := func(shardID int, partID int64) (int64, string, error) {
		return 0, "", nil
	}
	createBinFunc := func(fields BlockData, segmentID int64, partID int64) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {
		return nil, nil, nil
	}
	saveBinFunc := func(fieldsInsert []*datapb.FieldBinlog, fieldsStats []*datapb.FieldBinlog,
		segmentID int64, targetChName string, rowCount int64, partID int64) error {
		return nil
	}

	err = wrapper.SetCallbackFunctions(assignSegFunc, createBinFunc, saveBinFunc)
	assert.NoError(t, err)
	err = wrapper.SetCallbackFunctions(assignSegFunc, createBinFunc, nil)
	assert.Error(t, err)
	err = wrapper.SetCallbackFunctions(assignSegFunc, nil, nil)
	assert.Error(t, err)
	err = wrapper.SetCallbackFunctions(nil, nil, nil)
	assert.Error(t, err)

	err = wrapper.Cancel()
	assert.NoError(t, err)
}

func Test_ImportWrapperRowBased(t *testing.T) {
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.NoError(t, err)
	defer os.RemoveAll(TempFilesPath)
	params.Params.Init()

	// NewDefaultFactory() use "/tmp/milvus" as default root path, and cannot specify root path
	// NewChunkManagerFactory() can specify the root path
	f := storage.NewChunkManagerFactory("local", storage.RootPath(TempFilesPath))
	ctx := context.Background()
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)

	idAllocator := newIDAllocator(ctx, t, nil)

	content := []byte(`{
		"rows":[
			{"FieldBool": true, "FieldInt8": 10, "FieldInt16": 101, "FieldInt32": 1001, "FieldInt64": 10001, "FieldFloat": 3.14, "FieldDouble": 1.56, "FieldString": "hello world", "FieldJSON": {"x": 2}, "FieldBinaryVector": [254, 0], "FieldFloatVector": [1.1, 1.2, 1.3, 1.4], "FieldJSON": {"a": 7, "b": true}},
			{"FieldBool": false, "FieldInt8": 11, "FieldInt16": 102, "FieldInt32": 1002, "FieldInt64": 10002, "FieldFloat": 3.15, "FieldDouble": 2.56, "FieldString": "hello world", "FieldJSON": "{\"k\": 2.5}", "FieldBinaryVector": [253, 0], "FieldFloatVector": [2.1, 2.2, 2.3, 2.4], "FieldJSON": {"a": 8, "b": 2}},
			{"FieldBool": true, "FieldInt8": 12, "FieldInt16": 103, "FieldInt32": 1003, "FieldInt64": 10003, "FieldFloat": 3.16, "FieldDouble": 3.56, "FieldString": "hello world", "FieldJSON": {"y": "hello"}, "FieldBinaryVector": [252, 0], "FieldFloatVector": [3.1, 3.2, 3.3, 3.4], "FieldJSON": {"a": 9, "b": false}},
			{"FieldBool": false, "FieldInt8": 13, "FieldInt16": 104, "FieldInt32": 1004, "FieldInt64": 10004, "FieldFloat": 3.17, "FieldDouble": 4.56, "FieldString": "hello world", "FieldJSON": "{}", "FieldBinaryVector": [251, 0], "FieldFloatVector": [4.1, 4.2, 4.3, 4.4], "FieldJSON": {"a": 10, "b": 2.15}},
			{"FieldBool": true, "FieldInt8": 14, "FieldInt16": 105, "FieldInt32": 1005, "FieldInt64": 10005, "FieldFloat": 3.18, "FieldDouble": 5.56, "FieldString": "hello world", "FieldJSON": "{\"x\": true}", "FieldBinaryVector": [250, 0], "FieldFloatVector": [5.1, 5.2, 5.3, 5.4], "FieldJSON": {"a": 11, "b": "s"}}
		]
	}`)

	filePath := TempFilesPath + "rows_1.json"
	err = cm.Write(ctx, filePath, content)
	assert.NoError(t, err)
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())

	rowCounter := &rowCounterTest{}
	assignSegmentFunc, flushFunc, saveSegmentFunc := createMockCallbackFunctions(t, rowCounter)

	importResult := &rootcoordpb.ImportResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		TaskId:     1,
		DatanodeId: 1,
		State:      commonpb.ImportState_ImportStarted,
		Segments:   make([]int64, 0),
		AutoIds:    make([]int64, 0),
		RowCount:   0,
	}
	reportFunc := func(res *rootcoordpb.ImportResult) error {
		return nil
	}
	collectionInfo, err := NewCollectionInfo(sampleSchema(), 2, []int64{1})
	assert.NoError(t, err)

	t.Run("success case", func(t *testing.T) {
		wrapper := NewImportWrapper(ctx, collectionInfo, 1, idAllocator, cm, importResult, reportFunc)
		wrapper.SetCallbackFunctions(assignSegmentFunc, flushFunc, saveSegmentFunc)
		files := make([]string, 0)
		files = append(files, filePath)
		err = wrapper.Import(files, ImportOptions{OnlyValidate: true})
		assert.NoError(t, err)
		assert.Equal(t, 0, rowCounter.rowCount)

		err = wrapper.Import(files, DefaultImportOptions())
		assert.NoError(t, err)
		assert.Equal(t, 5, rowCounter.rowCount)
		assert.Equal(t, commonpb.ImportState_ImportPersisted, importResult.State)
	})

	t.Run("parse error", func(t *testing.T) {
		content = []byte(`{
			"rows":[
				{"FieldBool": true, "FieldInt8": false, "FieldInt16": 101, "FieldInt32": 1001, "FieldInt64": 10001, "FieldFloat": 3.14, "FieldDouble": 1.56, "FieldString": "hello world", "FieldJSON": "{\"x\": 2}", "FieldBinaryVector": [254, 0], "FieldFloatVector": [1.1, 1.2, 1.3, 1.4], "FieldJSON": {"a": 9, "b": false}},
			]
		}`)

		filePath = TempFilesPath + "rows_2.json"
		err = cm.Write(ctx, filePath, content)
		assert.NoError(t, err)

		importResult.State = commonpb.ImportState_ImportStarted
		wrapper := NewImportWrapper(ctx, collectionInfo, 1, idAllocator, cm, importResult, reportFunc)
		wrapper.SetCallbackFunctions(assignSegmentFunc, flushFunc, saveSegmentFunc)
		files := make([]string, 0)
		files = append(files, filePath)
		err = wrapper.Import(files, ImportOptions{OnlyValidate: true})
		assert.Error(t, err)
		assert.NotEqual(t, commonpb.ImportState_ImportPersisted, importResult.State)
	})

	t.Run("file doesn't exist", func(t *testing.T) {
		files := make([]string, 0)
		files = append(files, "/dummy/dummy.json")
		wrapper := NewImportWrapper(ctx, collectionInfo, 1, idAllocator, cm, importResult, reportFunc)
		err = wrapper.Import(files, ImportOptions{OnlyValidate: true})
		assert.Error(t, err)
	})
}

func Test_ImportWrapperColumnBased_numpy(t *testing.T) {
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.NoError(t, err)
	defer os.RemoveAll(TempFilesPath)

	// NewDefaultFactory() use "/tmp/milvus" as default root path, and cannot specify root path
	// NewChunkManagerFactory() can specify the root path
	f := storage.NewChunkManagerFactory("local", storage.RootPath(TempFilesPath))
	ctx := context.Background()
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())

	idAllocator := newIDAllocator(ctx, t, nil)

	rowCounter := &rowCounterTest{}
	assignSegmentFunc, flushFunc, saveSegmentFunc := createMockCallbackFunctions(t, rowCounter)

	importResult := &rootcoordpb.ImportResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		TaskId:     1,
		DatanodeId: 1,
		State:      commonpb.ImportState_ImportStarted,
		Segments:   make([]int64, 0),
		AutoIds:    make([]int64, 0),
		RowCount:   0,
	}
	reportFunc := func(res *rootcoordpb.ImportResult) error {
		return nil
	}
	collectionInfo, err := NewCollectionInfo(sampleSchema(), 2, []int64{1})
	assert.NoError(t, err)

	files := createSampleNumpyFiles(t, cm)

	t.Run("success case", func(t *testing.T) {
		wrapper := NewImportWrapper(ctx, collectionInfo, 1, idAllocator, cm, importResult, reportFunc)
		wrapper.SetCallbackFunctions(assignSegmentFunc, flushFunc, saveSegmentFunc)

		err = wrapper.Import(files, DefaultImportOptions())
		assert.NoError(t, err)
		assert.Equal(t, 5, rowCounter.rowCount)
		assert.Equal(t, commonpb.ImportState_ImportPersisted, importResult.State)
	})

	t.Run("row count of fields not equal", func(t *testing.T) {
		filePath := path.Join(cm.RootPath(), "FieldInt8.npy")
		content, err := CreateNumpyData([]int8{10})
		assert.NoError(t, err)
		err = cm.Write(ctx, filePath, content)
		assert.NoError(t, err)
		files[1] = filePath

		importResult.State = commonpb.ImportState_ImportStarted
		wrapper := NewImportWrapper(ctx, collectionInfo, 1, idAllocator, cm, importResult, reportFunc)
		wrapper.SetCallbackFunctions(assignSegmentFunc, flushFunc, saveSegmentFunc)

		err = wrapper.Import(files, DefaultImportOptions())
		assert.Error(t, err)
		assert.NotEqual(t, commonpb.ImportState_ImportPersisted, importResult.State)
	})

	t.Run("file doesn't exist", func(t *testing.T) {
		files := make([]string, 0)
		files = append(files, "/dummy/dummy.npy")
		wrapper := NewImportWrapper(ctx, collectionInfo, 1, idAllocator, cm, importResult, reportFunc)
		err = wrapper.Import(files, DefaultImportOptions())
		assert.Error(t, err)
	})
}

func perfSchema(dim int) *schemapb.CollectionSchema {
	schema := &schemapb.CollectionSchema{
		Name:        "schema",
		Description: "schema",
		AutoID:      true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      101,
				Name:         "ID",
				IsPrimaryKey: true,
				AutoID:       false,
				Description:  "int64",
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      102,
				Name:         "Vector",
				IsPrimaryKey: false,
				Description:  "float_vector",
				DataType:     schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: strconv.Itoa(dim)},
				},
			},
		},
	}

	return schema
}

func Test_ImportWrapperRowBased_perf(t *testing.T) {
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.NoError(t, err)
	defer os.RemoveAll(TempFilesPath)

	// NewDefaultFactory() use "/tmp/milvus" as default root path, and cannot specify root path
	// NewChunkManagerFactory() can specify the root path
	f := storage.NewChunkManagerFactory("local", storage.RootPath(TempFilesPath))
	ctx := context.Background()
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())

	idAllocator := newIDAllocator(ctx, t, nil)

	tr := timerecord.NewTimeRecorder("row-based parse performance")

	type Entity struct {
		ID     int64
		Vector []float32
	}

	type Entities struct {
		Rows []*Entity
	}

	// change these parameters to test different cases
	dim := 128
	rowCount := 10000
	shardNum := 2
	segmentSize := 512 // unit: MB

	// generate rows data
	entities := &Entities{
		Rows: make([]*Entity, 0),
	}

	for i := 0; i < rowCount; i++ {
		entity := &Entity{
			ID:     int64(i),
			Vector: make([]float32, 0, dim),
		}
		for k := 0; k < dim; k++ {
			entity.Vector = append(entity.Vector, float32(i)+3.1415926)
		}
		entities.Rows = append(entities.Rows, entity)
	}
	tr.Record("generate " + strconv.Itoa(rowCount) + " rows")

	// generate a json file
	filePath := path.Join(cm.RootPath(), "row_perf.json")
	func() {
		var b bytes.Buffer
		bw := bufio.NewWriter(&b)

		encoder := json.NewEncoder(bw)
		err = encoder.Encode(entities)
		assert.NoError(t, err)
		err = bw.Flush()
		assert.NoError(t, err)
		err = cm.Write(ctx, filePath, b.Bytes())
		assert.NoError(t, err)
	}()
	tr.Record("generate large json file: " + filePath)

	rowCounter := &rowCounterTest{}
	assignSegmentFunc, flushFunc, saveSegmentFunc := createMockCallbackFunctions(t, rowCounter)

	schema := perfSchema(dim)

	importResult := &rootcoordpb.ImportResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		TaskId:     1,
		DatanodeId: 1,
		State:      commonpb.ImportState_ImportStarted,
		Segments:   make([]int64, 0),
		AutoIds:    make([]int64, 0),
		RowCount:   0,
	}
	reportFunc := func(res *rootcoordpb.ImportResult) error {
		return nil
	}
	collectionInfo, err := NewCollectionInfo(schema, int32(shardNum), []int64{1})
	assert.NoError(t, err)
	wrapper := NewImportWrapper(ctx, collectionInfo, int64(segmentSize), idAllocator, cm, importResult, reportFunc)
	wrapper.SetCallbackFunctions(assignSegmentFunc, flushFunc, saveSegmentFunc)

	files := make([]string, 0)
	files = append(files, filePath)
	err = wrapper.Import(files, DefaultImportOptions())
	assert.NoError(t, err)
	assert.Equal(t, rowCount, rowCounter.rowCount)

	tr.Record("parse large json file " + filePath)
}

func Test_ImportWrapperFileValidation(t *testing.T) {
	ctx := context.Background()

	cm := &MockChunkManager{
		size: 1,
	}

	idAllocator := newIDAllocator(ctx, t, nil)
	schema := &schemapb.CollectionSchema{
		Name:   "schema",
		AutoID: true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      101,
				Name:         "uid",
				IsPrimaryKey: true,
				AutoID:       true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      102,
				Name:         "bol",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Bool,
			},
		},
	}
	shardNum := 2
	segmentSize := 512 // unit: MB

	collectionInfo, err := NewCollectionInfo(schema, int32(shardNum), []int64{1})
	assert.NoError(t, err)
	wrapper := NewImportWrapper(ctx, collectionInfo, int64(segmentSize), idAllocator, cm, nil, nil)

	t.Run("unsupported file type", func(t *testing.T) {
		files := []string{"uid.txt"}
		rowBased, err := wrapper.fileValidation(files)
		assert.Error(t, err)
		assert.False(t, rowBased)
	})

	t.Run("duplicate files", func(t *testing.T) {
		files := []string{"a/1.json", "b/1.json"}
		rowBased, err := wrapper.fileValidation(files)
		assert.Error(t, err)
		assert.True(t, rowBased)

		files = []string{"a/uid.npy", "uid.npy", "b/bol.npy"}
		rowBased, err = wrapper.fileValidation(files)
		assert.Error(t, err)
		assert.False(t, rowBased)
	})

	t.Run("unsupported file for row-based", func(t *testing.T) {
		files := []string{"a/uid.json", "b/bol.npy"}
		rowBased, err := wrapper.fileValidation(files)
		assert.Error(t, err)
		assert.True(t, rowBased)
	})

	t.Run("unsupported file for column-based", func(t *testing.T) {
		files := []string{"a/uid.npy", "b/bol.json"}
		rowBased, err := wrapper.fileValidation(files)
		assert.Error(t, err)
		assert.False(t, rowBased)
	})

	t.Run("valid cases", func(t *testing.T) {
		files := []string{"a/1.json", "b/2.json"}
		rowBased, err := wrapper.fileValidation(files)
		assert.NoError(t, err)
		assert.True(t, rowBased)

		files = []string{"a/uid.npy", "b/bol.npy"}
		rowBased, err = wrapper.fileValidation(files)
		assert.NoError(t, err)
		assert.False(t, rowBased)
	})

	t.Run("empty file list", func(t *testing.T) {
		files := []string{}
		cm.size = 0
		wrapper = NewImportWrapper(ctx, collectionInfo, int64(segmentSize), idAllocator, cm, nil, nil)
		rowBased, err := wrapper.fileValidation(files)
		assert.NoError(t, err)
		assert.False(t, rowBased)
	})

	t.Run("file size exceed MaxFileSize limit", func(t *testing.T) {
		files := []string{"a/1.json"}
		cm.size = params.Params.CommonCfg.ImportMaxFileSize.GetAsInt64() + 1
		wrapper = NewImportWrapper(ctx, collectionInfo, int64(segmentSize), idAllocator, cm, nil, nil)
		rowBased, err := wrapper.fileValidation(files)
		assert.Error(t, err)
		assert.True(t, rowBased)
	})

	t.Run("failed to get file size", func(t *testing.T) {
		files := []string{"a/1.json"}
		cm.sizeErr = errors.New("error")
		rowBased, err := wrapper.fileValidation(files)
		assert.Error(t, err)
		assert.True(t, rowBased)
	})

	t.Run("file size is zero", func(t *testing.T) {
		files := []string{"a/1.json"}
		cm.sizeErr = nil
		cm.size = int64(0)
		rowBased, err := wrapper.fileValidation(files)
		assert.Error(t, err)
		assert.True(t, rowBased)
	})
}

func Test_ImportWrapperReportFailRowBased(t *testing.T) {
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.NoError(t, err)
	defer os.RemoveAll(TempFilesPath)

	// NewDefaultFactory() use "/tmp/milvus" as default root path, and cannot specify root path
	// NewChunkManagerFactory() can specify the root path
	f := storage.NewChunkManagerFactory("local", storage.RootPath(TempFilesPath))
	ctx := context.Background()
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)

	idAllocator := newIDAllocator(ctx, t, nil)

	content := []byte(`{
		"rows":[
			{"FieldBool": true, "FieldInt8": 10, "FieldInt16": 101, "FieldInt32": 1001, "FieldInt64": 10001, "FieldFloat": 3.14, "FieldDouble": 1.56, "FieldString": "hello world", "FieldJSON": "{\"x\": \"aaa\"}", "FieldBinaryVector": [254, 0], "FieldFloatVector": [1.1, 1.2, 1.3, 1.4], "FieldJSON": {"a": 9, "b": false}},
			{"FieldBool": false, "FieldInt8": 11, "FieldInt16": 102, "FieldInt32": 1002, "FieldInt64": 10002, "FieldFloat": 3.15, "FieldDouble": 2.56, "FieldString": "hello world", "FieldJSON": "{}", "FieldBinaryVector": [253, 0], "FieldFloatVector": [2.1, 2.2, 2.3, 2.4], "FieldJSON": {"a": 9, "b": false}},
			{"FieldBool": true, "FieldInt8": 12, "FieldInt16": 103, "FieldInt32": 1003, "FieldInt64": 10003, "FieldFloat": 3.16, "FieldDouble": 3.56, "FieldString": "hello world", "FieldJSON": "{\"x\": 2, \"y\": 5}", "FieldBinaryVector": [252, 0], "FieldFloatVector": [3.1, 3.2, 3.3, 3.4], "FieldJSON": {"a": 9, "b": false}},
			{"FieldBool": false, "FieldInt8": 13, "FieldInt16": 104, "FieldInt32": 1004, "FieldInt64": 10004, "FieldFloat": 3.17, "FieldDouble": 4.56, "FieldString": "hello world", "FieldJSON": "{\"x\": true}", "FieldBinaryVector": [251, 0], "FieldFloatVector": [4.1, 4.2, 4.3, 4.4], "FieldJSON": {"a": 9, "b": false}},
			{"FieldBool": true, "FieldInt8": 14, "FieldInt16": 105, "FieldInt32": 1005, "FieldInt64": 10005, "FieldFloat": 3.18, "FieldDouble": 5.56, "FieldString": "hello world", "FieldJSON": "{}", "FieldBinaryVector": [250, 0], "FieldFloatVector": [5.1, 5.2, 5.3, 5.4], "FieldJSON": {"a": 9, "b": false}}
		]
	}`)

	filePath := path.Join(cm.RootPath(), "rows_1.json")
	err = cm.Write(ctx, filePath, content)
	assert.NoError(t, err)
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())

	rowCounter := &rowCounterTest{}
	assignSegmentFunc, flushFunc, saveSegmentFunc := createMockCallbackFunctions(t, rowCounter)

	// success case
	importResult := &rootcoordpb.ImportResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		TaskId:     1,
		DatanodeId: 1,
		State:      commonpb.ImportState_ImportStarted,
		Segments:   make([]int64, 0),
		AutoIds:    make([]int64, 0),
		RowCount:   0,
	}
	reportFunc := func(res *rootcoordpb.ImportResult) error {
		return nil
	}
	collectionInfo, err := NewCollectionInfo(sampleSchema(), 2, []int64{1})
	assert.NoError(t, err)
	wrapper := NewImportWrapper(ctx, collectionInfo, 1, idAllocator, cm, importResult, reportFunc)
	wrapper.SetCallbackFunctions(assignSegmentFunc, flushFunc, saveSegmentFunc)

	files := []string{filePath}
	wrapper.reportImportAttempts = 2
	wrapper.reportFunc = func(res *rootcoordpb.ImportResult) error {
		return errors.New("mock error")
	}
	err = wrapper.Import(files, DefaultImportOptions())
	assert.Error(t, err)
	assert.Equal(t, 5, rowCounter.rowCount)
	assert.Equal(t, commonpb.ImportState_ImportPersisted, importResult.State)
}

func Test_ImportWrapperReportFailColumnBased_numpy(t *testing.T) {
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.NoError(t, err)
	defer os.RemoveAll(TempFilesPath)

	// NewDefaultFactory() use "/tmp/milvus" as default root path, and cannot specify root path
	// NewChunkManagerFactory() can specify the root path
	f := storage.NewChunkManagerFactory("local", storage.RootPath(TempFilesPath))
	ctx := context.Background()
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())

	idAllocator := newIDAllocator(ctx, t, nil)

	rowCounter := &rowCounterTest{}
	assignSegmentFunc, flushFunc, saveSegmentFunc := createMockCallbackFunctions(t, rowCounter)

	// success case
	importResult := &rootcoordpb.ImportResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		TaskId:     1,
		DatanodeId: 1,
		State:      commonpb.ImportState_ImportStarted,
		Segments:   make([]int64, 0),
		AutoIds:    make([]int64, 0),
		RowCount:   0,
	}
	reportFunc := func(res *rootcoordpb.ImportResult) error {
		return nil
	}
	collectionInfo, err := NewCollectionInfo(sampleSchema(), 2, []int64{1})
	assert.NoError(t, err)
	wrapper := NewImportWrapper(ctx, collectionInfo, 1, idAllocator, cm, importResult, reportFunc)
	wrapper.SetCallbackFunctions(assignSegmentFunc, flushFunc, saveSegmentFunc)

	wrapper.reportImportAttempts = 2
	wrapper.reportFunc = func(res *rootcoordpb.ImportResult) error {
		return errors.New("mock error")
	}

	files := createSampleNumpyFiles(t, cm)

	err = wrapper.Import(files, DefaultImportOptions())
	assert.Error(t, err)
	assert.Equal(t, 5, rowCounter.rowCount)
	assert.Equal(t, commonpb.ImportState_ImportPersisted, importResult.State)
}

func Test_ImportWrapperIsBinlogImport(t *testing.T) {
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	assert.NoError(t, err)
	defer os.RemoveAll(TempFilesPath)

	// NewDefaultFactory() use "/tmp/milvus" as default root path, and cannot specify root path
	// NewChunkManagerFactory() can specify the root path
	f := storage.NewChunkManagerFactory("local", storage.RootPath(TempFilesPath))
	ctx := context.Background()
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	assert.NoError(t, err)

	idAllocator := newIDAllocator(ctx, t, nil)
	schema := perfSchema(128)
	shardNum := 2
	segmentSize := 512 // unit: MB

	collectionInfo, err := NewCollectionInfo(schema, int32(shardNum), []int64{1})
	assert.NoError(t, err)
	wrapper := NewImportWrapper(ctx, collectionInfo, int64(segmentSize), idAllocator, cm, nil, nil)

	// empty paths
	paths := []string{}
	b := wrapper.isBinlogImport(paths)
	assert.False(t, b)

	// paths count should be 2
	paths = []string{
		"path1",
		"path2",
		"path3",
	}
	b = wrapper.isBinlogImport(paths)
	assert.False(t, b)

	// not path
	paths = []string{
		"path1.txt",
		"path2.jpg",
	}
	b = wrapper.isBinlogImport(paths)
	assert.False(t, b)

	// path doesn't exist
	paths = []string{
		"path1",
		"path2",
	}

	b = wrapper.isBinlogImport(paths)
	assert.True(t, b)

	// the delta log path is empty, success
	paths = []string{
		"path1",
		"",
	}
	b = wrapper.isBinlogImport(paths)
	assert.True(t, b)

	// path is empty string
	paths = []string{
		"",
		"",
	}
	b = wrapper.isBinlogImport(paths)
	assert.False(t, b)
}

func Test_ImportWrapperDoBinlogImport(t *testing.T) {
	ctx := context.Background()

	cm := &MockChunkManager{
		size: 1,
	}

	idAllocator := newIDAllocator(ctx, t, nil)
	schema := perfSchema(128)
	shardNum := 2
	segmentSize := 512 // unit: MB

	collectionInfo, err := NewCollectionInfo(schema, int32(shardNum), []int64{1})
	assert.NoError(t, err)
	wrapper := NewImportWrapper(ctx, collectionInfo, int64(segmentSize), idAllocator, cm, nil, nil)
	paths := []string{
		"/tmp",
		"/tmp",
	}
	wrapper.chunkManager = nil

	// failed to create new BinlogParser
	err = wrapper.doBinlogImport(paths, 0, math.MaxUint64)
	assert.Error(t, err)

	cm.listErr = errors.New("error")
	wrapper.chunkManager = cm

	rowCounter := &rowCounterTest{}
	assignSegmentFunc, flushFunc, saveSegmentFunc := createMockCallbackFunctions(t, rowCounter)
	wrapper.SetCallbackFunctions(assignSegmentFunc, flushFunc, saveSegmentFunc)

	// failed to call parser.Parse()
	err = wrapper.doBinlogImport(paths, 0, math.MaxUint64)
	assert.Error(t, err)

	// Import() failed
	err = wrapper.Import(paths, DefaultImportOptions())
	assert.Error(t, err)

	cm.listErr = nil
	wrapper.reportFunc = func(res *rootcoordpb.ImportResult) error {
		return nil
	}
	wrapper.importResult = &rootcoordpb.ImportResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		TaskId:     1,
		DatanodeId: 1,
		State:      commonpb.ImportState_ImportStarted,
		Segments:   make([]int64, 0),
		AutoIds:    make([]int64, 0),
		RowCount:   0,
	}

	// succeed
	err = wrapper.doBinlogImport(paths, 0, math.MaxUint64)
	assert.NoError(t, err)
}

func Test_ImportWrapperReportPersisted(t *testing.T) {
	ctx := context.Background()
	tr := timerecord.NewTimeRecorder("test")

	importResult := &rootcoordpb.ImportResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		TaskId:     1,
		DatanodeId: 1,
		State:      commonpb.ImportState_ImportStarted,
		Segments:   make([]int64, 0),
		AutoIds:    make([]int64, 0),
		RowCount:   0,
	}
	reportFunc := func(res *rootcoordpb.ImportResult) error {
		return nil
	}
	collectionInfo, err := NewCollectionInfo(sampleSchema(), 2, []int64{1})
	assert.NoError(t, err)
	wrapper := NewImportWrapper(ctx, collectionInfo, int64(1024), nil, nil, importResult, reportFunc)
	assert.NotNil(t, wrapper)

	rowCounter := &rowCounterTest{}
	assignSegmentFunc, flushFunc, saveSegmentFunc := createMockCallbackFunctions(t, rowCounter)
	err = wrapper.SetCallbackFunctions(assignSegmentFunc, flushFunc, saveSegmentFunc)
	assert.NoError(t, err)

	// success
	err = wrapper.reportPersisted(2, tr)
	assert.NoError(t, err)
	assert.NotEmpty(t, wrapper.importResult.GetInfos())

	// error when closing segments
	wrapper.saveSegmentFunc = func(fieldsInsert []*datapb.FieldBinlog, fieldsStats []*datapb.FieldBinlog,
		segmentID int64, targetChName string, rowCount int64, partID int64) error {
		return errors.New("error")
	}
	wrapper.workingSegments[0] = map[int64]*WorkingSegment{
		int64(1): {},
	}
	err = wrapper.reportPersisted(2, tr)
	assert.Error(t, err)

	// failed to report
	wrapper.saveSegmentFunc = func(fieldsInsert []*datapb.FieldBinlog, fieldsStats []*datapb.FieldBinlog,
		segmentID int64, targetChName string, rowCount int64, partID int64) error {
		return nil
	}
	wrapper.reportFunc = func(res *rootcoordpb.ImportResult) error {
		return errors.New("error")
	}
	err = wrapper.reportPersisted(2, tr)
	assert.Error(t, err)
}

func Test_ImportWrapperUpdateProgressPercent(t *testing.T) {
	ctx := context.Background()

	collectionInfo, err := NewCollectionInfo(sampleSchema(), 2, []int64{1})
	assert.NoError(t, err)
	wrapper := NewImportWrapper(ctx, collectionInfo, 1, nil, nil, nil, nil)
	assert.NotNil(t, wrapper)
	assert.Equal(t, int64(0), wrapper.progressPercent)

	wrapper.updateProgressPercent(5)
	assert.Equal(t, int64(5), wrapper.progressPercent)

	wrapper.updateProgressPercent(200)
	assert.Equal(t, int64(5), wrapper.progressPercent)

	wrapper.updateProgressPercent(100)
	assert.Equal(t, int64(100), wrapper.progressPercent)
}

func Test_ImportWrapperFlushFunc(t *testing.T) {
	ctx := context.Background()
	params.Params.Init()

	shardID := 0
	partitionID := int64(1)
	rowCounter := &rowCounterTest{}
	assignSegmentFunc, flushFunc, saveSegmentFunc := createMockCallbackFunctions(t, rowCounter)

	importResult := &rootcoordpb.ImportResult{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		TaskId:     1,
		DatanodeId: 1,
		State:      commonpb.ImportState_ImportStarted,
		Segments:   make([]int64, 0),
		AutoIds:    make([]int64, 0),
		RowCount:   0,
	}
	reportFunc := func(res *rootcoordpb.ImportResult) error {
		return nil
	}

	schema := sampleSchema()
	collectionInfo, err := NewCollectionInfo(schema, 2, []int64{1})
	assert.NoError(t, err)
	wrapper := NewImportWrapper(ctx, collectionInfo, 1, nil, nil, importResult, reportFunc)
	assert.NotNil(t, wrapper)
	wrapper.SetCallbackFunctions(assignSegmentFunc, flushFunc, saveSegmentFunc)

	t.Run("fieldsData is empty", func(t *testing.T) {
		blockData := initBlockData(schema)
		err = wrapper.flushFunc(blockData, shardID, partitionID)
		assert.NoError(t, err)
	})

	fieldsData := createFieldsData(schema, 5)
	blockData := createBlockData(schema, fieldsData)
	t.Run("fieldsData is not empty", func(t *testing.T) {
		err = wrapper.flushFunc(blockData, shardID, partitionID)
		assert.NoError(t, err)
		assert.Contains(t, wrapper.workingSegments, shardID)
		assert.Contains(t, wrapper.workingSegments[shardID], partitionID)
		assert.NotNil(t, wrapper.workingSegments[shardID][partitionID])
	})

	t.Run("close segment, saveSegmentFunc returns error", func(t *testing.T) {
		wrapper.saveSegmentFunc = func(fieldsInsert []*datapb.FieldBinlog, fieldsStats []*datapb.FieldBinlog,
			segmentID int64, targetChName string, rowCount int64, partID int64) error {
			return errors.New("error")
		}
		wrapper.segmentSize = 1
		wrapper.workingSegments = make(map[int]map[int64]*WorkingSegment)
		wrapper.workingSegments[shardID] = map[int64]*WorkingSegment{
			int64(1): {
				memSize: 100,
			},
		}

		err = wrapper.flushFunc(blockData, shardID, partitionID)
		assert.Error(t, err)
	})

	t.Run("assignSegmentFunc returns error", func(t *testing.T) {
		wrapper.assignSegmentFunc = func(shardID int, partID int64) (int64, string, error) {
			return 100, "ch", errors.New("error")
		}
		err = wrapper.flushFunc(blockData, 99, partitionID)
		assert.Error(t, err)
	})

	t.Run("createBinlogsFunc returns error", func(t *testing.T) {
		wrapper.saveSegmentFunc = func(fieldsInsert []*datapb.FieldBinlog, fieldsStats []*datapb.FieldBinlog,
			segmentID int64, targetChName string, rowCount int64, partID int64) error {
			return nil
		}
		wrapper.assignSegmentFunc = func(shardID int, partID int64) (int64, string, error) {
			return 100, "ch", nil
		}
		wrapper.createBinlogsFunc = func(fields BlockData, segmentID int64, partID int64) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {
			return nil, nil, errors.New("error")
		}
		err = wrapper.flushFunc(blockData, shardID, partitionID)
		assert.Error(t, err)
	})
}
