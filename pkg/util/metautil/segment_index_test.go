package metautil

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

func TestIndexPathBuilder_V0_SingleFile(t *testing.T) {
	b := NewIndexPathBuilder("files", 0, 100, 200, 300, 1000, 1)
	path := b.BuildFilePath("index_data")
	assert.Equal(t, "files/index_files/1000/1/200/300/index_data", path)
}

func TestIndexPathBuilder_V1_SingleFile(t *testing.T) {
	b := NewIndexPathBuilder("files", 1, 100, 200, 300, 1000, 1)
	path := b.BuildFilePath("index_data")
	assert.Equal(t, "files/index_files/100/200/300/1000/1/index_data", path)
}

func TestIndexPathBuilder_V0_MultipleFiles(t *testing.T) {
	b := NewIndexPathBuilder("files", 0, 100, 200, 300, 1000, 1)
	paths := b.BuildFilePaths([]string{"a", "b", "c"})
	assert.Equal(t, []string{
		"files/index_files/1000/1/200/300/a",
		"files/index_files/1000/1/200/300/b",
		"files/index_files/1000/1/200/300/c",
	}, paths)
}

func TestIndexPathBuilder_V1_MultipleFiles(t *testing.T) {
	b := NewIndexPathBuilder("files", 1, 100, 200, 300, 1000, 1)
	paths := b.BuildFilePaths([]string{"a", "b", "c"})
	assert.Equal(t, []string{
		"files/index_files/100/200/300/1000/1/a",
		"files/index_files/100/200/300/1000/1/b",
		"files/index_files/100/200/300/1000/1/c",
	}, paths)
}

func TestIndexPathBuilder_V0_EmptyFileKeys(t *testing.T) {
	b := NewIndexPathBuilder("files", 0, 100, 200, 300, 1000, 1)
	paths := b.BuildFilePaths([]string{})
	assert.Empty(t, paths)
}

func TestIndexPathBuilder_V1_EmptyFileKeys(t *testing.T) {
	b := NewIndexPathBuilder("files", 1, 100, 200, 300, 1000, 1)
	paths := b.BuildFilePaths([]string{})
	assert.Empty(t, paths)
}

func TestIndexPathBuilder_V0_Prefix(t *testing.T) {
	b := NewIndexPathBuilder("root", 0, 100, 200, 300, 1000, 1)
	prefix := b.BuildPrefix()
	assert.Equal(t, "root/index_files/1000/1/200/300", prefix)
}

func TestIndexPathBuilder_V1_Prefix(t *testing.T) {
	b := NewIndexPathBuilder("root", 1, 100, 200, 300, 1000, 1)
	prefix := b.BuildPrefix()
	assert.Equal(t, "root/index_files/100/200/300/1000/1", prefix)
}

func TestIndexPathBuilder_V0_CollectionPrefix(t *testing.T) {
	b := NewIndexPathBuilder("root", 0, 100, 0, 0, 0, 0)
	prefix := b.BuildCollectionPrefix()
	assert.Equal(t, "root/index_files", prefix)
}

func TestIndexPathBuilder_V1_CollectionPrefix(t *testing.T) {
	b := NewIndexPathBuilder("root", 1, 100, 0, 0, 0, 0)
	prefix := b.BuildCollectionPrefix()
	assert.Equal(t, "root/index_files/100", prefix)
}

func TestIndexPathBuilder_FutureVersion_TreatedAsV1(t *testing.T) {
	b := NewIndexPathBuilder("files", 2, 100, 200, 300, 1000, 1)
	path := b.BuildFilePath("data")
	assert.Equal(t, "files/index_files/100/200/300/1000/1/data", path)
}

func TestIsCollectionRooted(t *testing.T) {
	assert.False(t, IsCollectionRooted(indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_BUILD_ROOTED))
	assert.True(t, IsCollectionRooted(indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED))
	assert.True(t, IsCollectionRooted(indexpb.IndexStorePathVersion(2)))
}
