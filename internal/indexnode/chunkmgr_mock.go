package indexnode

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"golang.org/x/exp/mmap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const (
	vecFieldID = 101
)

var (
	errNotImplErr = fmt.Errorf("not implemented error")

	collschema = &schemapb.CollectionSchema{
		Name:        "mock_collection",
		Description: "mock",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      0,
				Name:         "int64",
				IsPrimaryKey: true,
				Description:  "",
				DataType:     schemapb.DataType_Int64,
				AutoID:       false,
			},
			{
				FieldID:      vecFieldID,
				Name:         "vector",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     schemapb.DataType_FloatVector,
				AutoID:       false,
			},
		},
	}

	collMeta = &etcdpb.CollectionMeta{
		Schema: &schemapb.CollectionSchema{
			Name:        "mock_index",
			Description: "mock",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      vecFieldID,
					Name:         "vector",
					IsPrimaryKey: false,
					Description:  "",
					DataType:     schemapb.DataType_FloatVector,
					AutoID:       false,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "8"},
					},
				},
			},
		},
	}
)

var mockChunkMgrIns = &mockChunkmgr{}

type mockStorageFactory struct{}

func (m *mockStorageFactory) NewChunkManager(context.Context, *indexpb.StorageConfig) (storage.ChunkManager, error) {
	return mockChunkMgrIns, nil
}

type mockChunkmgr struct {
	segmentData sync.Map
	indexedData sync.Map
}

var _ storage.ChunkManager = &mockChunkmgr{}

// var _ dependency.Factory = &mockFactory{}

func (c *mockChunkmgr) RootPath() string {
	return ""
}

func (c *mockChunkmgr) Path(ctx context.Context, filePath string) (string, error) {
	// TODO
	return filePath, errNotImplErr
}

func (c *mockChunkmgr) Size(ctx context.Context, filePath string) (int64, error) {
	// TODO
	return 0, errNotImplErr
}

func (c *mockChunkmgr) Write(ctx context.Context, filePath string, content []byte) error {
	c.indexedData.Store(filePath, content)
	return nil
}

func (c *mockChunkmgr) MultiWrite(ctx context.Context, contents map[string][]byte) error {
	// TODO
	return errNotImplErr
}

func (c *mockChunkmgr) Exist(ctx context.Context, filePath string) (bool, error) {
	// TODO
	return false, errNotImplErr
}

func (c *mockChunkmgr) Read(ctx context.Context, filePath string) ([]byte, error) {
	value, ok := c.segmentData.Load(filePath)
	if !ok {
		return nil, fmt.Errorf("data not exists")
	}
	return value.(*storage.Blob).Value, nil
}

func (c *mockChunkmgr) Reader(ctx context.Context, filePath string) (storage.FileReader, error) {
	// TODO
	return nil, errNotImplErr
}

func (c *mockChunkmgr) MultiRead(ctx context.Context, filePaths []string) ([][]byte, error) {
	// TODO
	return nil, errNotImplErr
}

func (c *mockChunkmgr) WalkWithPrefix(ctx context.Context, prefix string, recursive bool, walkFunc storage.ChunkObjectWalkFunc) error {
	return errNotImplErr
}

func (c *mockChunkmgr) Mmap(ctx context.Context, filePath string) (*mmap.ReaderAt, error) {
	// TODO
	return nil, errNotImplErr
}

func (c *mockChunkmgr) ReadAt(ctx context.Context, filePath string, off int64, length int64) ([]byte, error) {
	// TODO
	return nil, errNotImplErr
}

func (c *mockChunkmgr) Remove(ctx context.Context, filePath string) error {
	// TODO
	return errNotImplErr
}

func (c *mockChunkmgr) MultiRemove(ctx context.Context, filePaths []string) error {
	// TODO
	return errNotImplErr
}

func (c *mockChunkmgr) RemoveWithPrefix(ctx context.Context, prefix string) error {
	// TODO
	return errNotImplErr
}

func (c *mockChunkmgr) mockFieldData(numrows, dim int, collectionID, partitionID, segmentID int64) {
	idList := make([]int64, 0, numrows)
	tsList := make([]int64, 0, numrows)
	ts0 := time.Now().Unix()
	for i := 0; i < numrows; i++ {
		idList = append(idList, int64(i)+1)
		tsList = append(tsList, ts0+int64(i))
	}
	vecs := randomFloats(numrows, dim)
	idField := storage.Int64FieldData{
		Data: idList,
	}
	tsField := storage.Int64FieldData{
		Data: tsList,
	}
	vecField := storage.FloatVectorFieldData{
		Data: vecs,
		Dim:  dim,
	}
	insertData := &storage.InsertData{
		Data: map[int64]storage.FieldData{
			common.TimeStampField: &tsField,
			common.RowIDField:     &idField,
			vecFieldID:            &vecField,
		},
	}
	insertCodec := &storage.InsertCodec{
		Schema: collMeta,
	}
	blobs, err := insertCodec.Serialize(partitionID, segmentID, insertData)
	if err != nil {
		panic(err)
	}
	if len(blobs) != 1 {
		panic("invalid blobs")
	}
	c.segmentData.Store(dataPath(collectionID, partitionID, segmentID), blobs[0])
}

func NewMockChunkManager() *mockChunkmgr {
	return &mockChunkmgr{}
}

type mockFactory struct {
	chunkMgr *mockChunkmgr
}

func (f *mockFactory) NewCacheStorageChunkManager(context.Context) (storage.ChunkManager, error) {
	return nil, errNotImplErr
}

func (f *mockFactory) NewPersistentStorageChunkManager(context.Context) (storage.ChunkManager, error) {
	if f.chunkMgr != nil {
		return f.chunkMgr, nil
	}
	return nil, fmt.Errorf("factory not inited")
}

func (f *mockFactory) Init(*paramtable.ComponentParam) {
	// do nothing
}

func (f *mockFactory) NewMsgStream(context.Context) (msgstream.MsgStream, error) {
	// TOD
	return nil, errNotImplErr
}

func (f *mockFactory) NewTtMsgStream(context.Context) (msgstream.MsgStream, error) {
	// TODO
	return nil, errNotImplErr
}

func (f *mockFactory) NewMsgStreamDisposer(ctx context.Context) func([]string, string) error {
	// TODO
	return nil
}

func randomFloats(rows, dim int) []float32 {
	vecs := make([]float32, 0, rows)

	for i := 0; i < rows; i++ {
		vec := make([]float32, 0, dim)
		for j := 0; j < dim; j++ {
			vec = append(vec, rand.Float32())
		}
		vecs = append(vecs, vec...)
	}
	return vecs
}

func dataPath(collectionID, partitionID, segmentID int64) string {
	return fmt.Sprintf("%d-%d-%d", collectionID, partitionID, segmentID)
}
