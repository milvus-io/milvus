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

package etcd

import (
	"context"
	"path"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

func TestEtcd(t *testing.T) {
	err := InitEtcdServer(true, "", "/tmp/data", "stdout", "info")
	assert.NoError(t, err)
	defer StopEtcdServer()

	etcdCli, err := GetEtcdClient(true, false, []string{}, "", "", "", "")
	assert.NoError(t, err)

	key := path.Join("test", "test")
	_, err = etcdCli.Put(context.TODO(), key, "value")
	assert.NoError(t, err)

	resp, err := etcdCli.Get(context.TODO(), key)
	assert.NoError(t, err)
	assert.False(t, resp.Count < 1)
	assert.Equal(t, string(resp.Kvs[0].Value), "value")

	_, err = GetEtcdClient(false, true, []string{},
		"../../../configs/cert/client.pem",
		"../../../configs/cert/client.key",
		"../../../configs/cert/ca.pem",
		"some not right word")
	assert.Error(t, err)

	_, err = GetEtcdClient(false, true, []string{},
		"../../../configs/cert/client.pem",
		"../../../configs/cert/client.key",
		"wrong/file",
		"1.2")
	assert.Error(t, err)

	_, err = GetEtcdClient(false, true, []string{},
		"wrong/file",
		"../../../configs/cert/client.key",
		"../../../configs/cert/ca.pem",
		"1.2")
	assert.Error(t, err)
}

func Test_buildKvGroup(t *testing.T) {
	t.Run("length not equal", func(t *testing.T) {
		keys := []string{"k1", "k2"}
		values := []string{"v1"}
		_, err := buildKvGroup(keys, values)
		assert.Error(t, err)
	})

	t.Run("duplicate", func(t *testing.T) {
		keys := []string{"k1", "k1"}
		values := []string{"v1", "v2"}
		_, err := buildKvGroup(keys, values)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		keys := []string{"k1", "k2"}
		values := []string{"v1", "v2"}
		kvs, err := buildKvGroup(keys, values)
		assert.NoError(t, err)
		for i, k := range keys {
			v, ok := kvs[k]
			assert.True(t, ok)
			assert.Equal(t, values[i], v)
		}
	})
}

func Test_SaveByBatch(t *testing.T) {
	t.Run("empty kvs", func(t *testing.T) {
		kvs := map[string]string{}

		group := 0
		count := 0
		saveFn := func(partialKvs map[string]string) error {
			group++
			count += len(partialKvs)
			return nil
		}

		limit := 2
		err := SaveByBatchWithLimit(kvs, limit, saveFn)
		assert.NoError(t, err)
		assert.Equal(t, 0, group)
		assert.Equal(t, 0, count)
	})

	t.Run("normal case", func(t *testing.T) {
		kvs := map[string]string{
			"k1": "v1",
			"k2": "v2",
			"k3": "v3",
		}

		group := 0
		count := 0
		saveFn := func(partialKvs map[string]string) error {
			group++
			count += len(partialKvs)
			return nil
		}

		limit := 2
		err := SaveByBatchWithLimit(kvs, limit, saveFn)
		assert.NoError(t, err)
		assert.Equal(t, 2, group)
		assert.Equal(t, 3, count)
	})

	t.Run("multi save failed", func(t *testing.T) {
		saveFn := func(partialKvs map[string]string) error {
			return errors.New("mock")
		}
		kvs := map[string]string{
			"k1": "v1",
			"k2": "v2",
			"k3": "v3",
		}
		limit := 2
		err := SaveByBatchWithLimit(kvs, limit, saveFn)
		assert.Error(t, err)
	})
}

func Test_RemoveByBatch(t *testing.T) {
	t.Run("empty kvs case", func(t *testing.T) {
		var kvs []string

		group := 0
		count := 0
		removeFn := func(partialKvs []string) error {
			group++
			count += len(partialKvs)
			return nil
		}

		limit := 2
		err := RemoveByBatchWithLimit(kvs, limit, removeFn)
		assert.NoError(t, err)
		assert.Equal(t, 0, group)
		assert.Equal(t, 0, count)
	})

	t.Run("normal case", func(t *testing.T) {
		kvs := []string{"k1", "k2", "k3", "k4", "k5"}

		group := 0
		count := 0
		removeFn := func(partialKvs []string) error {
			group++
			count += len(partialKvs)
			return nil
		}

		limit := 2
		err := RemoveByBatchWithLimit(kvs, limit, removeFn)
		assert.NoError(t, err)
		assert.Equal(t, 3, group)
		assert.Equal(t, 5, count)
	})

	t.Run("multi remove failed", func(t *testing.T) {
		removeFn := func(partialKvs []string) error {
			return errors.New("mock")
		}
		kvs := []string{"k1", "k2", "k3", "k4", "k5"}
		limit := 2
		err := RemoveByBatchWithLimit(kvs, limit, removeFn)
		assert.Error(t, err)
	})
}

func Test_min(t *testing.T) {
	type args struct {
		a int
		b int
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			args: args{a: 1, b: 2},
			want: 1,
		},
		{
			args: args{a: 4, b: 3},
			want: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := min(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("min() = %v, want %v", got, tt.want)
			}
		})
	}
}
