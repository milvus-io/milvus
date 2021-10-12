// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package miniokv_test

import (
	"context"
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	miniokv "github.com/milvus-io/milvus/internal/kv/minio"
	"github.com/milvus-io/milvus/internal/util/paramtable"

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

func TestMinIOKV_LoadPartial(t *testing.T) {
	Params.Init()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bucketName := "fantastic-tech-test"
	minIOKV, err := newMinIOKVClient(ctx, bucketName)
	assert.Nil(t, err)

	defer minIOKV.RemoveWithPrefix("")

	key := "TestMinIOKV_LoadPartial_key"
	value := "TestMinIOKV_LoadPartial_value"

	err = minIOKV.Save(key, value)
	assert.NoError(t, err)

	var start, end int64
	var partial []byte

	start, end = 1, 2
	partial, err = minIOKV.LoadPartial(key, start, end)
	assert.NoError(t, err)
	assert.ElementsMatch(t, partial, []byte(value[start:end]))

	start, end = 0, int64(len(value))
	partial, err = minIOKV.LoadPartial(key, start, end)
	assert.NoError(t, err)
	assert.ElementsMatch(t, partial, []byte(value[start:end]))

	// error case
	start, end = 5, 3
	_, err = minIOKV.LoadPartial(key, start, end)
	assert.Error(t, err)

	start, end = 1, 1
	_, err = minIOKV.LoadPartial(key, start, end)
	assert.Error(t, err)

	start, end = -1, 1
	_, err = minIOKV.LoadPartial(key, start, end)
	assert.Error(t, err)

	start, end = 1, -1
	_, err = minIOKV.LoadPartial(key, start, end)
	assert.Error(t, err)

	err = minIOKV.Remove(key)
	assert.NoError(t, err)
	start, end = 1, 2
	_, err = minIOKV.LoadPartial(key, start, end)
	assert.Error(t, err)
}

func TestMinIOKV_FGetObject(t *testing.T) {
	Params.Init()
	path := "/tmp/milvus/data"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bucketName := "fantastic-tech-test"
	MinIOKV, err := newMinIOKVClient(ctx, bucketName)
	assert.Nil(t, err)
	defer MinIOKV.RemoveWithPrefix("")

	name1 := "31280791048324/4325023534/53443534/key_1"
	value1 := "123"
	err = MinIOKV.Save(name1, value1)
	assert.Nil(t, err)
	name2 := "312895849354/31205934503459/18948129301/key_2"
	value2 := "333"
	err = MinIOKV.Save(name2, value2)
	assert.Nil(t, err)

	err = MinIOKV.FGetObject(name1, path)
	assert.Nil(t, err)

	err = MinIOKV.FGetObject(name2, path)
	assert.Nil(t, err)

	err = MinIOKV.FGetObject("fail", path)
	assert.NotNil(t, err)

	file1, err := os.Open(path + name1)
	assert.Nil(t, err)
	content1, err := ioutil.ReadAll(file1)
	assert.Nil(t, err)
	assert.Equal(t, value1, string(content1))
	defer file1.Close()
	defer os.Remove(path + name1)

	file2, err := os.Open(path + name2)
	assert.Nil(t, err)
	content2, err := ioutil.ReadAll(file2)
	assert.Nil(t, err)
	assert.Equal(t, value2, string(content2))
	defer file1.Close()
	defer os.Remove(path + name2)
}

func TestMinIOKV_FGetObjects(t *testing.T) {
	Params.Init()
	path := "/tmp/milvus/data"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bucketName := "fantastic-tech-test"
	MinIOKV, err := newMinIOKVClient(ctx, bucketName)
	assert.Nil(t, err)
	defer MinIOKV.RemoveWithPrefix("")

	name1 := "31280791048324/4325023534/53443534/key_1"
	value1 := "123"
	err = MinIOKV.Save(name1, value1)
	assert.Nil(t, err)
	name2 := "312895849354/31205934503459/18948129301/key_2"
	value2 := "333"
	err = MinIOKV.Save(name2, value2)
	assert.Nil(t, err)

	err = MinIOKV.FGetObjects([]string{name1, name2}, path)
	assert.Nil(t, err)

	err = MinIOKV.FGetObjects([]string{"fail1", "fail2"}, path)
	assert.NotNil(t, err)

	file1, err := os.Open(path + name1)
	assert.Nil(t, err)
	content1, err := ioutil.ReadAll(file1)
	assert.Nil(t, err)
	assert.Equal(t, value1, string(content1))
	defer file1.Close()
	defer os.Remove(path + name1)

	file2, err := os.Open(path + name2)
	assert.Nil(t, err)
	content2, err := ioutil.ReadAll(file2)
	assert.Nil(t, err)
	assert.Equal(t, value2, string(content2))
	defer file1.Close()
	defer os.Remove(path + name2)
}
