package miniokv_test

import (
	"context"
	"strconv"
	"testing"

	miniokv "github.com/zilliztech/milvus-distributed/internal/kv/minio"
	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"

	"github.com/stretchr/testify/assert"
)

var Params paramtable.BaseTable

func newMinIOKVClient(ctx context.Context, bucketName string) (*miniokv.MinIOKV, error) {
	endPoint, _ := Params.Load("_MinioAddress")
	accessKeyID, _ := Params.Load("minio.accessKeyID")
	secretAccessKey, _ := Params.Load("minio.secretAccessKey")
	useSSLStr, _ := Params.Load("minio.useSSL")
	useSSL, _ := strconv.ParseBool(useSSLStr)
	option := &miniokv.Option{
		Address:           endPoint,
		AccessKeyID:       accessKeyID,
		SecretAccessKeyID: secretAccessKey,
		UseSSL:            useSSL,
		BucketName:        bucketName,
		CreateBucket:      true,
	}
	client, err := miniokv.NewMinIOKV(ctx, option)
	return client, err
}

func TestMinIOKV_Load(t *testing.T) {
	Params.Init()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bucketName := "fantastic-tech-test"
	MinIOKV, err := newMinIOKVClient(ctx, bucketName)
	assert.Nil(t, err)
	defer MinIOKV.RemoveWithPrefix("")

	err = MinIOKV.Save("abc", "123")
	assert.Nil(t, err)
	err = MinIOKV.Save("abcd", "1234")
	assert.Nil(t, err)

	val, err := MinIOKV.Load("abc")
	assert.Nil(t, err)
	assert.Equal(t, val, "123")

	keys, vals, err := MinIOKV.LoadWithPrefix("abc")
	assert.Nil(t, err)
	assert.Equal(t, len(keys), len(vals))
	assert.Equal(t, len(keys), 2)

	assert.Equal(t, vals[0], "123")
	assert.Equal(t, vals[1], "1234")

	err = MinIOKV.Save("key_1", "123")
	assert.Nil(t, err)
	err = MinIOKV.Save("key_2", "456")
	assert.Nil(t, err)
	err = MinIOKV.Save("key_3", "789")
	assert.Nil(t, err)

	keys = []string{"key_1", "key_100"}

	vals, err = MinIOKV.MultiLoad(keys)
	assert.NotNil(t, err)
	assert.Equal(t, len(vals), len(keys))
	assert.Equal(t, vals[0], "123")
	assert.Empty(t, vals[1])

	keys = []string{"key_1", "key_2"}

	vals, err = MinIOKV.MultiLoad(keys)
	assert.Nil(t, err)
	assert.Equal(t, len(vals), len(keys))
	assert.Equal(t, vals[0], "123")
	assert.Equal(t, vals[1], "456")

}

func TestMinIOKV_MultiSave(t *testing.T) {
	Params.Init()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bucketName := "fantastic-tech-test"
	MinIOKV, err := newMinIOKVClient(ctx, bucketName)
	assert.Nil(t, err)

	defer MinIOKV.RemoveWithPrefix("")

	err = MinIOKV.Save("key_1", "111")
	assert.Nil(t, err)

	kvs := map[string]string{
		"key_1": "123",
		"key_2": "456",
	}

	err = MinIOKV.MultiSave(kvs)
	assert.Nil(t, err)

	val, err := MinIOKV.Load("key_1")
	assert.Nil(t, err)
	assert.Equal(t, val, "123")
}

func TestMinIOKV_Remove(t *testing.T) {
	Params.Init()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bucketName := "fantastic-tech-test"
	MinIOKV, err := newMinIOKVClient(ctx, bucketName)
	assert.Nil(t, err)
	defer MinIOKV.RemoveWithPrefix("")

	err = MinIOKV.Save("key_1", "123")
	assert.Nil(t, err)
	err = MinIOKV.Save("key_2", "456")
	assert.Nil(t, err)

	val, err := MinIOKV.Load("key_1")
	assert.Nil(t, err)
	assert.Equal(t, val, "123")
	// delete "key_1"
	err = MinIOKV.Remove("key_1")
	assert.Nil(t, err)
	val, err = MinIOKV.Load("key_1")
	assert.Error(t, err)
	assert.Empty(t, val)

	val, err = MinIOKV.Load("key_2")
	assert.Nil(t, err)
	assert.Equal(t, val, "456")

	keys, vals, err := MinIOKV.LoadWithPrefix("key")
	assert.Nil(t, err)
	assert.Equal(t, len(keys), len(vals))
	assert.Equal(t, len(keys), 1)

	assert.Equal(t, vals[0], "456")

	// MultiRemove
	err = MinIOKV.Save("key_1", "111")
	assert.Nil(t, err)

	kvs := map[string]string{
		"key_1": "123",
		"key_2": "456",
		"key_3": "789",
		"key_4": "012",
	}

	err = MinIOKV.MultiSave(kvs)
	assert.Nil(t, err)
	val, err = MinIOKV.Load("key_1")
	assert.Nil(t, err)
	assert.Equal(t, val, "123")
	val, err = MinIOKV.Load("key_3")
	assert.Nil(t, err)
	assert.Equal(t, val, "789")

	keys = []string{"key_1", "key_2", "key_3"}
	err = MinIOKV.MultiRemove(keys)
	assert.Nil(t, err)

	val, err = MinIOKV.Load("key_1")
	assert.Error(t, err)
	assert.Empty(t, val)
}
