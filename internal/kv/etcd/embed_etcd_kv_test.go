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

package etcdkv_test

import (
	"context"
	"fmt"
	"path"
	"sort"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/exp/maps"

	embed_etcd_kv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/pkg/v2/kv"
	"github.com/milvus-io/milvus/pkg/v2/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestEmbedEtcd(te *testing.T) {
	te.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.StandaloneDeployMode)
	param := new(paramtable.ComponentParam)
	te.Setenv("etcd.use.embed", "true")
	te.Setenv("etcd.config.path", "../../../configs/advanced/etcd.yaml")

	dir := te.TempDir()
	te.Setenv("etcd.data.dir", dir)

	param.Init(paramtable.NewBaseTable())

	te.Run("etcdKV SaveAndLoad", func(t *testing.T) {
		rootPath := "/etcd/test/root/saveandload"
		metaKv, err := embed_etcd_kv.NewMetaKvFactory(rootPath, &param.EtcdCfg)
		require.NoError(te, err)
		assert.NotNil(te, metaKv)
		require.NoError(t, err)

		defer metaKv.Close()
		defer metaKv.RemoveWithPrefix(context.TODO(), "")

		saveAndLoadTests := []struct {
			key   string
			value string
		}{
			{"test1", "value1"},
			{"test2", "value2"},
			{"test1/a", "value_a"},
			{"test1/b", "value_b"},
		}

		for i, test := range saveAndLoadTests {
			if i < 4 {
				err = metaKv.Save(context.TODO(), test.key, test.value)
				assert.NoError(t, err)
			}

			val, err := metaKv.Load(context.TODO(), test.key)
			assert.NoError(t, err)
			assert.Equal(t, test.value, val)
		}

		invalidLoadTests := []struct {
			invalidKey string
		}{
			{"t"},
			{"a"},
			{"test1a"},
		}

		for _, test := range invalidLoadTests {
			val, err := metaKv.Load(context.TODO(), test.invalidKey)
			assert.Error(t, err)
			assert.Zero(t, val)
		}

		loadPrefixTests := []struct {
			prefix string

			expectedKeys   []string
			expectedValues []string
			expectedError  error
		}{
			{"test", []string{
				metaKv.GetPath("test1"),
				metaKv.GetPath("test2"),
				metaKv.GetPath("test1/a"),
				metaKv.GetPath("test1/b"),
			}, []string{"value1", "value2", "value_a", "value_b"}, nil},
			{"test1", []string{
				metaKv.GetPath("test1"),
				metaKv.GetPath("test1/a"),
				metaKv.GetPath("test1/b"),
			}, []string{"value1", "value_a", "value_b"}, nil},
			{"test2", []string{metaKv.GetPath("test2")}, []string{"value2"}, nil},
			{"", []string{
				metaKv.GetPath("test1"),
				metaKv.GetPath("test2"),
				metaKv.GetPath("test1/a"),
				metaKv.GetPath("test1/b"),
			}, []string{"value1", "value2", "value_a", "value_b"}, nil},
			{"test1/a", []string{metaKv.GetPath("test1/a")}, []string{"value_a"}, nil},
			{"a", []string{}, []string{}, nil},
			{"root", []string{}, []string{}, nil},
			{"/etcd/test/root", []string{}, []string{}, nil},
		}

		for _, test := range loadPrefixTests {
			actualKeys, actualValues, err := metaKv.LoadWithPrefix(context.TODO(), test.prefix)
			assert.ElementsMatch(t, test.expectedKeys, actualKeys)
			assert.ElementsMatch(t, test.expectedValues, actualValues)
			assert.Equal(t, test.expectedError, err)
		}

		removeTests := []struct {
			validKey   string
			invalidKey string
		}{
			{"test1", "abc"},
			{"test1/a", "test1/lskfjal"},
			{"test1/b", "test1/b"},
			{"test2", "-"},
		}

		for _, test := range removeTests {
			err = metaKv.Remove(context.TODO(), test.validKey)
			assert.NoError(t, err)

			_, err = metaKv.Load(context.TODO(), test.validKey)
			assert.Error(t, err)

			err = metaKv.Remove(context.TODO(), test.validKey)
			assert.NoError(t, err)
			err = metaKv.Remove(context.TODO(), test.invalidKey)
			assert.NoError(t, err)
		}
	})

	te.Run("etcdKV SaveAndLoadBytes", func(t *testing.T) {
		rootPath := "/etcd/test/root/saveandloadbytes"
		_metaKv, err := embed_etcd_kv.NewMetaKvFactory(rootPath, &param.EtcdCfg)
		metaKv := _metaKv.(*embed_etcd_kv.EmbedEtcdKV)
		require.NoError(te, err)
		assert.NotNil(te, metaKv)
		require.NoError(t, err)

		defer metaKv.Close()
		defer metaKv.RemoveWithPrefix(context.TODO(), "")

		saveAndLoadTests := []struct {
			key   string
			value []byte
		}{
			{"test1", []byte("value1")},
			{"test2", []byte("value2")},
			{"test1/a", []byte("value_a")},
			{"test1/b", []byte("value_b")},
		}

		for i, test := range saveAndLoadTests {
			if i < 4 {
				err = metaKv.SaveBytes(context.TODO(), test.key, test.value)
				assert.NoError(t, err)
			}

			val, err := metaKv.LoadBytes(context.TODO(), test.key)
			assert.NoError(t, err)
			assert.Equal(t, test.value, val)
		}

		invalidLoadTests := []struct {
			invalidKey string
		}{
			{"t"},
			{"a"},
			{"test1a"},
		}

		for _, test := range invalidLoadTests {
			val, err := metaKv.LoadBytes(context.TODO(), test.invalidKey)
			assert.Error(t, err)
			assert.Zero(t, val)
		}

		loadPrefixTests := []struct {
			prefix string

			expectedKeys   []string
			expectedValues [][]byte
			expectedError  error
		}{
			{"test", []string{
				metaKv.GetPath("test1"),
				metaKv.GetPath("test2"),
				metaKv.GetPath("test1/a"),
				metaKv.GetPath("test1/b"),
			}, [][]byte{[]byte("value1"), []byte("value2"), []byte("value_a"), []byte("value_b")}, nil},
			{"test1", []string{
				metaKv.GetPath("test1"),
				metaKv.GetPath("test1/a"),
				metaKv.GetPath("test1/b"),
			}, [][]byte{[]byte("value1"), []byte("value_a"), []byte("value_b")}, nil},
			{"test2", []string{metaKv.GetPath("test2")}, [][]byte{[]byte("value2")}, nil},
			{"", []string{
				metaKv.GetPath("test1"),
				metaKv.GetPath("test2"),
				metaKv.GetPath("test1/a"),
				metaKv.GetPath("test1/b"),
			}, [][]byte{[]byte("value1"), []byte("value2"), []byte("value_a"), []byte("value_b")}, nil},
			{"test1/a", []string{metaKv.GetPath("test1/a")}, [][]byte{[]byte("value_a")}, nil},
			{"a", []string{}, [][]byte{}, nil},
			{"root", []string{}, [][]byte{}, nil},
			{"/etcd/test/root", []string{}, [][]byte{}, nil},
		}

		for _, test := range loadPrefixTests {
			actualKeys, actualValues, err := metaKv.LoadBytesWithPrefix(context.TODO(), test.prefix)
			assert.ElementsMatch(t, test.expectedKeys, actualKeys)
			assert.ElementsMatch(t, test.expectedValues, actualValues)
			assert.Equal(t, test.expectedError, err)

			actualKeys, actualValues, versions, err := metaKv.LoadBytesWithPrefix2(context.TODO(), test.prefix)
			assert.ElementsMatch(t, test.expectedKeys, actualKeys)
			assert.ElementsMatch(t, test.expectedValues, actualValues)
			assert.NotZero(t, versions)
			assert.Equal(t, test.expectedError, err)
		}

		removeTests := []struct {
			validKey   string
			invalidKey string
		}{
			{"test1", "abc"},
			{"test1/a", "test1/lskfjal"},
			{"test1/b", "test1/b"},
			{"test2", "-"},
		}

		for _, test := range removeTests {
			err = metaKv.Remove(context.TODO(), test.validKey)
			assert.NoError(t, err)

			_, err = metaKv.Load(context.TODO(), test.validKey)
			assert.Error(t, err)

			err = metaKv.Remove(context.TODO(), test.validKey)
			assert.NoError(t, err)
			err = metaKv.Remove(context.TODO(), test.invalidKey)
			assert.NoError(t, err)
		}
	})

	te.Run("etcdKV LoadBytesWithRevision", func(t *testing.T) {
		rootPath := "/etcd/test/root/LoadBytesWithRevision"
		_metaKv, err := embed_etcd_kv.NewMetaKvFactory(rootPath, &param.EtcdCfg)
		metaKv := _metaKv.(*embed_etcd_kv.EmbedEtcdKV)
		assert.NoError(t, err)

		defer metaKv.Close()
		defer metaKv.RemoveWithPrefix(context.TODO(), "")

		prepareKV := []struct {
			inKey   string
			inValue []byte
		}{
			{"a", []byte("a_version1")},
			{"b", []byte("b_version2")},
			{"a", []byte("a_version3")},
			{"c", []byte("c_version4")},
			{"a/suba", []byte("a_version5")},
		}

		for _, test := range prepareKV {
			err = metaKv.SaveBytes(context.TODO(), test.inKey, test.inValue)
			require.NoError(t, err)
		}

		loadWithRevisionTests := []struct {
			inKey string

			expectedKeyNo  int
			expectedValues [][]byte
		}{
			{"a", 2, [][]byte{[]byte("a_version3"), []byte("a_version5")}},
			{"b", 1, [][]byte{[]byte("b_version2")}},
			{"c", 1, [][]byte{[]byte("c_version4")}},
		}

		for _, test := range loadWithRevisionTests {
			keys, values, revision, err := metaKv.LoadBytesWithRevision(context.TODO(), test.inKey)
			assert.NoError(t, err)
			assert.Equal(t, test.expectedKeyNo, len(keys))
			assert.ElementsMatch(t, test.expectedValues, values)
			assert.NotZero(t, revision)
		}
	})

	te.Run("etcdKV MultiSaveAndMultiLoad", func(t *testing.T) {
		rootPath := "/etcd/test/root/multi_save_and_multi_load"
		metaKv, err := embed_etcd_kv.NewMetaKvFactory(rootPath, &param.EtcdCfg)
		assert.NoError(t, err)

		defer metaKv.Close()
		defer metaKv.RemoveWithPrefix(context.TODO(), "")

		multiSaveTests := map[string]string{
			"key_1":      "value_1",
			"key_2":      "value_2",
			"key_3/a":    "value_3a",
			"multikey_1": "multivalue_1",
			"multikey_2": "multivalue_2",
			"_":          "other",
		}

		err = metaKv.MultiSave(context.TODO(), multiSaveTests)
		assert.NoError(t, err)
		for k, v := range multiSaveTests {
			actualV, err := metaKv.Load(context.TODO(), k)
			assert.NoError(t, err)
			assert.Equal(t, v, actualV)
		}

		multiLoadTests := []struct {
			inputKeys      []string
			expectedValues []string
		}{
			{[]string{"key_1"}, []string{"value_1"}},
			{[]string{"key_1", "key_2", "key_3/a"}, []string{"value_1", "value_2", "value_3a"}},
			{[]string{"multikey_1", "multikey_2"}, []string{"multivalue_1", "multivalue_2"}},
			{[]string{"_"}, []string{"other"}},
		}

		for _, test := range multiLoadTests {
			vs, err := metaKv.MultiLoad(context.TODO(), test.inputKeys)
			assert.NoError(t, err)
			assert.Equal(t, test.expectedValues, vs)
		}

		invalidMultiLoad := []struct {
			invalidKeys    []string
			expectedValues []string
		}{
			{[]string{"a", "key_1"}, []string{"", "value_1"}},
			{[]string{".....", "key_1"}, []string{"", "value_1"}},
			{[]string{"*********"}, []string{""}},
			{[]string{"key_1", "1"}, []string{"value_1", ""}},
		}

		for _, test := range invalidMultiLoad {
			vs, err := metaKv.MultiLoad(context.TODO(), test.invalidKeys)
			assert.Error(t, err)
			assert.Equal(t, test.expectedValues, vs)
		}

		removeWithPrefixTests := []string{
			"key_1",
			"multi",
		}

		for _, k := range removeWithPrefixTests {
			err = metaKv.RemoveWithPrefix(context.TODO(), k)
			assert.NoError(t, err)

			ks, vs, err := metaKv.LoadWithPrefix(context.TODO(), k)
			assert.Empty(t, ks)
			assert.Empty(t, vs)
			assert.NoError(t, err)
		}

		multiRemoveTests := []string{
			"key_2",
			"key_3/a",
			"multikey_2",
			"_",
		}

		err = metaKv.MultiRemove(context.TODO(), multiRemoveTests)
		assert.NoError(t, err)

		ks, vs, err := metaKv.LoadWithPrefix(context.TODO(), "")
		assert.NoError(t, err)
		assert.Empty(t, ks)
		assert.Empty(t, vs)

		multiSaveAndRemoveTests := []struct {
			multiSaves   map[string]string
			multiRemoves []string
		}{
			{map[string]string{"key_1": "value_1"}, []string{}},
			{map[string]string{"key_2": "value_2"}, []string{"key_1"}},
			{map[string]string{"key_3/a": "value_3a"}, []string{"key_2"}},
			{map[string]string{"multikey_1": "multivalue_1"}, []string{}},
			{map[string]string{"multikey_2": "multivalue_2"}, []string{"multikey_1", "key_3/a"}},
			{make(map[string]string), []string{"multikey_2"}},
		}
		for _, test := range multiSaveAndRemoveTests {
			err = metaKv.MultiSaveAndRemove(context.TODO(), test.multiSaves, test.multiRemoves)
			assert.NoError(t, err)
		}

		ks, vs, err = metaKv.LoadWithPrefix(context.TODO(), "")
		assert.NoError(t, err)
		assert.Empty(t, ks)
		assert.Empty(t, vs)
	})

	te.Run("etcdKV MultiSaveAndMultiLoadBytes", func(t *testing.T) {
		rootPath := "/etcd/test/root/multi_save_and_multi_load"
		_metaKv, err := embed_etcd_kv.NewMetaKvFactory(rootPath, &param.EtcdCfg)
		metaKv := _metaKv.(*embed_etcd_kv.EmbedEtcdKV)
		assert.NoError(t, err)

		defer metaKv.Close()
		defer metaKv.RemoveWithPrefix(context.TODO(), "")

		multiSaveTests := map[string][]byte{
			"key_1":      []byte("value_1"),
			"key_2":      []byte("value_2"),
			"key_3/a":    []byte("value_3a"),
			"multikey_1": []byte("multivalue_1"),
			"multikey_2": []byte("multivalue_2"),
			"_":          []byte("other"),
		}

		err = metaKv.MultiSaveBytes(context.TODO(), multiSaveTests)
		assert.NoError(t, err)
		for k, v := range multiSaveTests {
			actualV, err := metaKv.LoadBytes(context.TODO(), k)
			assert.NoError(t, err)
			assert.Equal(t, v, actualV)
		}

		multiLoadTests := []struct {
			inputKeys      []string
			expectedValues [][]byte
		}{
			{[]string{"key_1"}, [][]byte{[]byte("value_1")}},
			{[]string{"key_1", "key_2", "key_3/a"}, [][]byte{[]byte("value_1"), []byte("value_2"), []byte("value_3a")}},
			{[]string{"multikey_1", "multikey_2"}, [][]byte{[]byte("multivalue_1"), []byte("multivalue_2")}},
			{[]string{"_"}, [][]byte{[]byte("other")}},
		}

		for _, test := range multiLoadTests {
			vs, err := metaKv.MultiLoadBytes(context.TODO(), test.inputKeys)
			assert.NoError(t, err)
			assert.Equal(t, test.expectedValues, vs)
		}

		invalidMultiLoad := []struct {
			invalidKeys    []string
			expectedValues [][]byte
		}{
			{[]string{"a", "key_1"}, [][]byte{[]byte(""), []byte("value_1")}},
			{[]string{".....", "key_1"}, [][]byte{[]byte(""), []byte("value_1")}},
			{[]string{"*********"}, [][]byte{[]byte("")}},
			{[]string{"key_1", "1"}, [][]byte{[]byte("value_1"), []byte("")}},
		}

		for _, test := range invalidMultiLoad {
			vs, err := metaKv.MultiLoadBytes(context.TODO(), test.invalidKeys)
			assert.Error(t, err)
			assert.Equal(t, test.expectedValues, vs)
		}

		removeWithPrefixTests := []string{
			"key_1",
			"multi",
		}

		for _, k := range removeWithPrefixTests {
			err = metaKv.RemoveWithPrefix(context.TODO(), k)
			assert.NoError(t, err)

			ks, vs, err := metaKv.LoadBytesWithPrefix(context.TODO(), k)
			assert.Empty(t, ks)
			assert.Empty(t, vs)
			assert.NoError(t, err)
		}

		multiRemoveTests := []string{
			"key_2",
			"key_3/a",
			"multikey_2",
			"_",
		}

		err = metaKv.MultiRemove(context.TODO(), multiRemoveTests)
		assert.NoError(t, err)

		ks, vs, err := metaKv.LoadBytesWithPrefix(context.TODO(), "")
		assert.NoError(t, err)
		assert.Empty(t, ks)
		assert.Empty(t, vs)

		multiSaveAndRemoveTests := []struct {
			multiSaves   map[string][]byte
			multiRemoves []string
		}{
			{map[string][]byte{"key_1": []byte("value_1")}, []string{}},
			{map[string][]byte{"key_2": []byte("value_2")}, []string{"key_1"}},
			{map[string][]byte{"key_3/a": []byte("value_3a")}, []string{"key_2"}},
			{map[string][]byte{"multikey_1": []byte("multivalue_1")}, []string{}},
			{map[string][]byte{"multikey_2": []byte("multivalue_2")}, []string{"multikey_1", "key_3/a"}},
			{map[string][]byte{}, []string{"multikey_2"}},
		}
		for _, test := range multiSaveAndRemoveTests {
			err = metaKv.MultiSaveBytesAndRemove(context.TODO(), test.multiSaves, test.multiRemoves)
			assert.NoError(t, err)
		}

		ks, vs, err = metaKv.LoadBytesWithPrefix(context.TODO(), "")
		assert.NoError(t, err)
		assert.Empty(t, ks)
		assert.Empty(t, vs)
	})

	te.Run("etcdKV MultiSaveAndRemoveWithPrefix", func(t *testing.T) {
		rootPath := "/etcd/test/root/multi_remove_with_prefix"
		metaKv, err := embed_etcd_kv.NewMetaKvFactory(rootPath, &param.EtcdCfg)
		require.NoError(t, err)

		defer metaKv.Close()
		defer metaKv.RemoveWithPrefix(context.TODO(), "")

		prepareTests := map[string]string{
			"x/abc/1": "1",
			"x/abc/2": "2",
			"x/def/1": "10",
			"x/def/2": "20",
			"x/den/1": "100",
			"x/den/2": "200",
		}

		// MultiSaveAndRemoveWithPrefix
		err = metaKv.MultiSave(context.TODO(), prepareTests)
		require.NoError(t, err)
		multiSaveAndRemoveWithPrefixTests := []struct {
			multiSave map[string]string
			prefix    []string

			loadPrefix         string
			lengthBeforeRemove int
			lengthAfterRemove  int
		}{
			{map[string]string{}, []string{"x/abc", "x/def", "x/den"}, "x", 6, 0},
			{map[string]string{"y/a": "vvv", "y/b": "vvv"}, []string{}, "y", 0, 2},
			{map[string]string{"y/c": "vvv"}, []string{}, "y", 2, 3},
			{map[string]string{"p/a": "vvv"}, []string{"y/a", "y"}, "y", 3, 0},
			{map[string]string{}, []string{"p"}, "p", 1, 0},
			{nil, []string{"p"}, "p", 0, 0},
		}

		for _, test := range multiSaveAndRemoveWithPrefixTests {
			k, _, err := metaKv.LoadWithPrefix(context.TODO(), test.loadPrefix)
			assert.NoError(t, err)
			assert.Equal(t, test.lengthBeforeRemove, len(k))

			err = metaKv.MultiSaveAndRemoveWithPrefix(context.TODO(), test.multiSave, test.prefix)
			assert.NoError(t, err)

			k, _, err = metaKv.LoadWithPrefix(context.TODO(), test.loadPrefix)
			assert.NoError(t, err)
			assert.Equal(t, test.lengthAfterRemove, len(k))
		}
	})

	te.Run("etcdKV MultiRemoveWithPrefixBytes", func(t *testing.T) {
		rootPath := "/etcd/test/root/multi_remove_with_prefix_bytes"
		_metaKv, err := embed_etcd_kv.NewMetaKvFactory(rootPath, &param.EtcdCfg)
		metaKv := _metaKv.(*embed_etcd_kv.EmbedEtcdKV)
		require.NoError(t, err)

		defer metaKv.Close()
		defer metaKv.RemoveWithPrefix(context.TODO(), "")

		prepareTests := map[string][]byte{
			"x/abc/1": []byte("1"),
			"x/abc/2": []byte("2"),
			"x/def/1": []byte("10"),
			"x/def/2": []byte("20"),
			"x/den/1": []byte("100"),
			"x/den/2": []byte("200"),
		}

		k, v, err := metaKv.LoadBytesWithPrefix(context.TODO(), "/")
		assert.NoError(t, err)
		assert.Zero(t, len(k))
		assert.Zero(t, len(v))

		// MultiSaveAndRemoveWithPrefix
		err = metaKv.MultiSaveBytes(context.TODO(), prepareTests)
		require.NoError(t, err)
		multiSaveAndRemoveWithPrefixTests := []struct {
			multiSave map[string][]byte
			prefix    []string

			loadPrefix         string
			lengthBeforeRemove int
			lengthAfterRemove  int
		}{
			{map[string][]byte{}, []string{"x/abc", "x/def", "x/den"}, "x", 6, 0},
			{map[string][]byte{"y/a": []byte("vvv"), "y/b": []byte("vvv")}, []string{}, "y", 0, 2},
			{map[string][]byte{"y/c": []byte("vvv")}, []string{}, "y", 2, 3},
			{map[string][]byte{"p/a": []byte("vvv")}, []string{"y/a", "y"}, "y", 3, 0},
			{map[string][]byte{}, []string{"p"}, "p", 1, 0},
		}

		for _, test := range multiSaveAndRemoveWithPrefixTests {
			k, _, err = metaKv.LoadBytesWithPrefix(context.TODO(), test.loadPrefix)
			assert.NoError(t, err)
			assert.Equal(t, test.lengthBeforeRemove, len(k))

			err = metaKv.MultiSaveBytesAndRemoveWithPrefix(context.TODO(), test.multiSave, test.prefix)
			assert.NoError(t, err)

			k, _, err = metaKv.LoadBytesWithPrefix(context.TODO(), test.loadPrefix)
			assert.NoError(t, err)
			assert.Equal(t, test.lengthAfterRemove, len(k))
		}
	})

	te.Run("etcdKV Watch", func(t *testing.T) {
		rootPath := "/etcd/test/root/watch"
		watchKv, err := embed_etcd_kv.NewWatchKVFactory(rootPath, &param.EtcdCfg)
		assert.NoError(t, err)

		defer watchKv.Close()
		defer watchKv.RemoveWithPrefix(context.TODO(), "")

		ch := watchKv.Watch(context.TODO(), "x")
		resp := <-ch
		assert.True(t, resp.Created)

		ch = watchKv.WatchWithPrefix(context.TODO(), "x")
		resp = <-ch
		assert.True(t, resp.Created)
	})

	te.Run("Etcd Revision Bytes", func(t *testing.T) {
		rootPath := "/etcd/test/root/revision_bytes"
		_metaKv, err := embed_etcd_kv.NewMetaKvFactory(rootPath, &param.EtcdCfg)
		metaKv := _metaKv.(*embed_etcd_kv.EmbedEtcdKV)
		assert.NoError(t, err)

		defer metaKv.Close()
		defer metaKv.RemoveWithPrefix(context.TODO(), "")

		revisionTests := []struct {
			inKey       string
			fistValue   []byte
			secondValue []byte
		}{
			{"a", []byte("v1"), []byte("v11")},
			{"y", []byte("v2"), []byte("v22")},
			{"z", []byte("v3"), []byte("v33")},
		}

		for _, test := range revisionTests {
			err = metaKv.SaveBytes(context.TODO(), test.inKey, test.fistValue)
			require.NoError(t, err)

			_, _, revision, _ := metaKv.LoadBytesWithRevision(context.TODO(), test.inKey)
			ch := metaKv.WatchWithRevision(context.TODO(), test.inKey, revision+1)

			err = metaKv.SaveBytes(context.TODO(), test.inKey, test.secondValue)
			require.NoError(t, err)

			resp := <-ch
			assert.Equal(t, 1, len(resp.Events))
			assert.Equal(t, test.secondValue, resp.Events[0].Kv.Value)
			assert.Equal(t, revision+1, resp.Header.Revision)
		}

		success, err := metaKv.CompareVersionAndSwapBytes(context.TODO(), "a/b/c", 0, []byte("1"))
		assert.NoError(t, err)
		assert.True(t, success)

		value, err := metaKv.LoadBytes(context.TODO(), "a/b/c")
		assert.NoError(t, err)
		assert.Equal(t, value, []byte("1"))

		success, err = metaKv.CompareVersionAndSwapBytes(context.TODO(), "a/b/c", 0, []byte("1"))
		assert.NoError(t, err)
		assert.False(t, success)
	})

	te.Run("Etcd WalkWithPagination", func(t *testing.T) {
		rootPath := "/etcd/test/root/walkWithPagination"
		metaKv, err := embed_etcd_kv.NewMetaKvFactory(rootPath, &param.EtcdCfg)
		assert.NoError(t, err)

		defer metaKv.Close()
		defer metaKv.RemoveWithPrefix(context.TODO(), "")

		kvs := map[string]string{
			"A/100":    "v1",
			"AA/100":   "v2",
			"AB/100":   "v3",
			"AB/2/100": "v4",
			"B/100":    "v5",
		}

		err = metaKv.MultiSave(context.TODO(), kvs)
		assert.NoError(t, err)
		for k, v := range kvs {
			actualV, err := metaKv.Load(context.TODO(), k)
			assert.NoError(t, err)
			assert.Equal(t, v, actualV)
		}

		t.Run("apply function error ", func(t *testing.T) {
			err = metaKv.WalkWithPrefix(context.TODO(), "A", 5, func(key []byte, value []byte) error {
				return errors.New("error")
			})
			assert.Error(t, err)
		})

		t.Run("get with non-exist prefix ", func(t *testing.T) {
			err = metaKv.WalkWithPrefix(context.TODO(), "non-exist-prefix", 5, func(key []byte, value []byte) error {
				return nil
			})
			assert.NoError(t, err)
		})

		t.Run("with different pagination", func(t *testing.T) {
			testFn := func(pagination int) {
				expected := map[string]string{
					"A/100":    "v1",
					"AA/100":   "v2",
					"AB/100":   "v3",
					"AB/2/100": "v4",
				}

				expectedSortedKey := maps.Keys(expected)
				sort.Strings(expectedSortedKey)

				ret := make(map[string]string)
				actualSortedKey := make([]string, 0)

				err = metaKv.WalkWithPrefix(context.TODO(), "A", pagination, func(key []byte, value []byte) error {
					k := string(key)
					k = k[len(rootPath)+1:]
					ret[k] = string(value)
					actualSortedKey = append(actualSortedKey, k)
					return nil
				})

				assert.NoError(t, err)
				assert.Equal(t, expected, ret, fmt.Errorf("pagination: %d", pagination))
				assert.Equal(t, expectedSortedKey, actualSortedKey, fmt.Errorf("pagination: %d", pagination))
			}

			testFn(-100)
			testFn(-1)
			testFn(0)
			testFn(1)
			testFn(5)
			testFn(100)
		})
	})

	te.Run("test has", func(t *testing.T) {
		rootPath := "/etcd/test/root/has"
		kv, err := embed_etcd_kv.NewMetaKvFactory(rootPath, &param.EtcdCfg)
		assert.NoError(t, err)

		defer kv.Close()
		defer kv.RemoveWithPrefix(context.TODO(), "")

		has, err := kv.Has(context.TODO(), "key1")
		assert.NoError(t, err)
		assert.False(t, has)

		err = kv.Save(context.TODO(), "key1", "value1")
		assert.NoError(t, err)

		has, err = kv.Has(context.TODO(), "key1")
		assert.NoError(t, err)
		assert.True(t, has)

		err = kv.Remove(context.TODO(), "key1")
		assert.NoError(t, err)

		has, err = kv.Has(context.TODO(), "key1")
		assert.NoError(t, err)
		assert.False(t, has)
	})

	te.Run("test has prefix", func(t *testing.T) {
		rootPath := "/etcd/test/root/hasprefix"
		kv, err := embed_etcd_kv.NewMetaKvFactory(rootPath, &param.EtcdCfg)
		assert.NoError(t, err)

		defer kv.Close()
		defer kv.RemoveWithPrefix(context.TODO(), "")

		has, err := kv.HasPrefix(context.TODO(), "key")
		assert.NoError(t, err)
		assert.False(t, has)

		err = kv.Save(context.TODO(), "key1", "value1")
		assert.NoError(t, err)

		has, err = kv.HasPrefix(context.TODO(), "key")
		assert.NoError(t, err)
		assert.True(t, has)

		err = kv.Remove(context.TODO(), "key1")
		assert.NoError(t, err)

		has, err = kv.HasPrefix(context.TODO(), "key")
		assert.NoError(t, err)
		assert.False(t, has)
	})
}

type EmbedEtcdKVSuite struct {
	suite.Suite

	param *paramtable.ComponentParam

	rootPath string
	kv       kv.MetaKv
}

func (s *EmbedEtcdKVSuite) SetupSuite() {
	te := s.T()
	te.Setenv(metricsinfo.DeployModeEnvKey, metricsinfo.StandaloneDeployMode)
	param := new(paramtable.ComponentParam)
	te.Setenv("etcd.use.embed", "true")
	te.Setenv("etcd.config.path", "../../../configs/advanced/etcd.yaml")

	dir := te.TempDir()
	te.Setenv("etcd.data.dir", dir)

	param.Init(paramtable.NewBaseTable())
	s.param = param
}

func (s *EmbedEtcdKVSuite) SetupTest() {
	s.rootPath = path.Join("unittest/etcdkv", funcutil.RandomString(8))

	metaKv, err := embed_etcd_kv.NewMetaKvFactory(s.rootPath, &s.param.EtcdCfg)
	s.Require().NoError(err)
	s.kv = metaKv
}

func (s *EmbedEtcdKVSuite) TearDownTest() {
	if s.kv != nil {
		s.kv.RemoveWithPrefix(context.TODO(), "")
		s.kv.Close()
		s.kv = nil
	}
}

func (s *EmbedEtcdKVSuite) TestTxnWithPredicates() {
	etcdKV := s.kv

	prepareKV := map[string]string{
		"lease1": "1",
		"lease2": "2",
	}

	err := etcdKV.MultiSave(context.TODO(), prepareKV)
	s.Require().NoError(err)

	badPredicate := predicates.NewMockPredicate(s.T())
	badPredicate.EXPECT().Type().Return(0)
	badPredicate.EXPECT().Target().Return(predicates.PredTargetValue)

	multiSaveAndRemovePredTests := []struct {
		tag           string
		multiSave     map[string]string
		preds         []predicates.Predicate
		expectSuccess bool
	}{
		{"predicate_ok", map[string]string{"a": "b"}, []predicates.Predicate{predicates.ValueEqual("lease1", "1")}, true},
		{"predicate_fail", map[string]string{"a": "b"}, []predicates.Predicate{predicates.ValueEqual("lease1", "2")}, false},
		{"bad_predicate", map[string]string{"a": "b"}, []predicates.Predicate{badPredicate}, false},
	}

	for _, test := range multiSaveAndRemovePredTests {
		s.Run(test.tag, func() {
			err := etcdKV.MultiSaveAndRemove(context.TODO(), test.multiSave, nil, test.preds...)
			if test.expectSuccess {
				s.NoError(err)
			} else {
				s.Error(err)
			}
			err = etcdKV.MultiSaveAndRemoveWithPrefix(context.TODO(), test.multiSave, nil, test.preds...)
			if test.expectSuccess {
				s.NoError(err)
			} else {
				s.Error(err)
			}
		})
	}
}

func TestEmbedEtcdKV(t *testing.T) {
	suite.Run(t, new(EmbedEtcdKVSuite))
}
