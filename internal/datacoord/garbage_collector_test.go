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

package datacoord

import (
	"bytes"
	"context"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_garbageCollector_basic(t *testing.T) {
	bucketName := `datacoord-ut` + strings.ToLower(funcutil.RandomString(8))
	rootPath := `gc` + funcutil.RandomString(8)
	//TODO change to Params
	cli, _, err := initUtOSSEnv(bucketName, rootPath, 0)
	require.NoError(t, err)

	mockAllocator := newMockAllocator()
	meta, err := newMemoryMeta(mockAllocator)
	assert.Nil(t, err)

	t.Run("normal gc", func(t *testing.T) {
		gc := newGarbageCollector(meta, GcOption{
			cli:              cli,
			enabled:          true,
			checkInterval:    time.Millisecond * 10,
			missingTolerance: time.Hour * 24,
			dropTolerance:    time.Hour * 24,
			bucketName:       bucketName,
			rootPath:         rootPath,
		})
		gc.start()

		time.Sleep(time.Millisecond * 20)
		assert.NotPanics(t, func() {
			gc.close()
		})
	})

	t.Run("with nil cli", func(t *testing.T) {
		gc := newGarbageCollector(meta, GcOption{
			cli:              nil,
			enabled:          true,
			checkInterval:    time.Millisecond * 10,
			missingTolerance: time.Hour * 24,
			dropTolerance:    time.Hour * 24,
			bucketName:       bucketName,
			rootPath:         rootPath,
		})
		assert.NotPanics(t, func() {
			gc.start()
		})

		assert.NotPanics(t, func() {
			gc.close()
		})
	})

}

func Test_garbageCollector_scan(t *testing.T) {
	bucketName := `datacoord-ut` + strings.ToLower(funcutil.RandomString(8))
	rootPath := `gc` + funcutil.RandomString(8)
	//TODO change to Params
	cli, files, err := initUtOSSEnv(bucketName, rootPath, 3)
	require.NoError(t, err)

	mockAllocator := newMockAllocator()
	meta, err := newMemoryMeta(mockAllocator)
	assert.Nil(t, err)

	t.Run("missing all but save tolerance", func(t *testing.T) {
		gc := newGarbageCollector(meta, GcOption{
			cli:              cli,
			enabled:          true,
			checkInterval:    time.Minute * 30,
			missingTolerance: time.Hour * 24,
			dropTolerance:    time.Hour * 24,
			bucketName:       bucketName,
			rootPath:         rootPath,
		})
		gc.scan()

		current := make([]string, 0, 3)
		for info := range cli.ListObjects(context.TODO(), bucketName, minio.ListObjectsOptions{Prefix: rootPath, Recursive: true}) {
			current = append(current, info.Key)
		}
		assert.ElementsMatch(t, files, current)
	})
	t.Run("all hit, no gc", func(t *testing.T) {
		segment := buildSegment(1, 10, 100, "ch")
		segment.State = commonpb.SegmentState_Flushed
		segment.Binlogs = []*datapb.FieldBinlog{{FieldID: 0, Binlogs: []string{files[0]}}}
		segment.Statslogs = []*datapb.FieldBinlog{{FieldID: 0, Binlogs: []string{files[1]}}}
		segment.Deltalogs = []*datapb.DeltaLogInfo{{DeltaLogPath: files[2]}}
		err = meta.AddSegment(segment)
		require.NoError(t, err)

		gc := newGarbageCollector(meta, GcOption{
			cli:              cli,
			enabled:          true,
			checkInterval:    time.Minute * 30,
			missingTolerance: time.Hour * 24,
			dropTolerance:    time.Hour * 24,
			bucketName:       bucketName,
			rootPath:         rootPath,
		})
		gc.start()
		gc.scan()

		current := make([]string, 0, 3)
		for info := range cli.ListObjects(context.TODO(), bucketName, minio.ListObjectsOptions{Prefix: rootPath, Recursive: true}) {
			current = append(current, info.Key)
		}
		assert.ElementsMatch(t, files, current)
		gc.close()
	})

	t.Run("dropped gc one", func(t *testing.T) {
		segment := buildSegment(1, 10, 100, "ch")
		segment.State = commonpb.SegmentState_Dropped
		segment.Deltalogs = []*datapb.DeltaLogInfo{{DeltaLogPath: files[2]}}
		err = meta.AddSegment(segment)
		require.NoError(t, err)

		gc := newGarbageCollector(meta, GcOption{
			cli:              cli,
			enabled:          true,
			checkInterval:    time.Minute * 30,
			missingTolerance: time.Hour * 24,
			dropTolerance:    0,
			bucketName:       bucketName,
			rootPath:         rootPath,
		})
		gc.start()
		gc.scan()

		current := make([]string, 0, 3)
		for info := range cli.ListObjects(context.TODO(), bucketName, minio.ListObjectsOptions{Prefix: rootPath, Recursive: true}) {
			current = append(current, info.Key)
		}
		assert.ElementsMatch(t, files[:2], current)
		gc.close()
	})
	t.Run("missing gc all", func(t *testing.T) {
		gc := newGarbageCollector(meta, GcOption{
			cli:              cli,
			enabled:          true,
			checkInterval:    time.Minute * 30,
			missingTolerance: 0,
			dropTolerance:    0,
			bucketName:       bucketName,
			rootPath:         rootPath,
		})
		gc.start()
		gc.scan()

		current := make([]string, 0, 3)
		for info := range cli.ListObjects(context.TODO(), bucketName, minio.ListObjectsOptions{Prefix: rootPath, Recursive: true}) {
			current = append(current, info.Key)
		}
		assert.Equal(t, 0, len(current))
		gc.close()
	})

	cleanupOSS(cli, bucketName, rootPath)
}

// initialize unit test sso env
func initUtOSSEnv(bucket, root string, n int) (*minio.Client, []string, error) {
	Params.Init()
	cli, err := minio.New(Params.MinioAddress, &minio.Options{
		Creds:  credentials.NewStaticV4(Params.MinioAccessKeyID, Params.MinioSecretAccessKey, ""),
		Secure: Params.MinioUseSSL,
	})
	if err != nil {
		return nil, nil, err
	}
	has, err := cli.BucketExists(context.TODO(), bucket)
	if err != nil {
		return nil, nil, err
	}
	if !has {
		err = cli.MakeBucket(context.TODO(), bucket, minio.MakeBucketOptions{})
		if err != nil {
			return nil, nil, err
		}
	}
	keys := make([]string, 0, n)
	content := []byte("test")
	for i := 0; i < n; i++ {
		reader := bytes.NewReader(content)
		token := funcutil.RandomString(8)
		token = path.Join(root, token)
		info, err := cli.PutObject(context.TODO(), bucket, token, reader, int64(len(content)), minio.PutObjectOptions{})
		if err != nil {
			return nil, nil, err
		}
		keys = append(keys, info.Key)
	}
	return cli, keys, nil
}

func cleanupOSS(cli *minio.Client, bucket, root string) {
	ch := cli.ListObjects(context.TODO(), bucket, minio.ListObjectsOptions{Prefix: root, Recursive: true})
	cli.RemoveObjects(context.TODO(), bucket, ch, minio.RemoveObjectsOptions{})
	cli.RemoveBucket(context.TODO(), bucket)
}
