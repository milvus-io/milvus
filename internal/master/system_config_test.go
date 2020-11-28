package master

import (
	"context"
	"fmt"
	"log"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/kv"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/clientv3"
)

func Test_SysConfig(t *testing.T) {
	Init()
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{Params.EtcdAddress},
		DialTimeout: 5 * time.Second,
	})
	require.Nil(t, err)
	_, err = cli.Delete(ctx, "/test/root", clientv3.WithPrefix())
	require.Nil(t, err)

	rootPath := "/test/root"
	configKV := kv.NewEtcdKV(cli, rootPath)
	defer configKV.Close()

	sc := SysConfig{kv: configKV}
	require.Equal(t, rootPath, sc.kv.GetPath("."))

	t.Run("tests on contig_test.yaml", func(t *testing.T) {
		err = sc.InitFromFile(".")
		require.Nil(t, err)

		testKeys := []string{
			"/etcd/address",
			"/master/port",
			"/master/proxyidlist",
			"/master/segmentthresholdfactor",
			"/pulsar/token",
			"/reader/stopflag",
			"/proxy/timezone",
			"/proxy/network/address",
			"/proxy/storage/path",
			"/storage/accesskey",
		}

		testVals := []string{
			"localhost",
			"53100",
			"[1 2]",
			"0.75",
			"eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJKb2UifQ.ipevRNuRP6HflG8cFKnmUPtypruRC4fb1DWtoLL62SY",
			"-1",
			"UTC+8",
			"0.0.0.0",
			"/var/lib/milvus",
			"",
		}

		vals, err := sc.Get(testKeys)
		assert.Nil(t, err)
		for i := range testVals {
			assert.Equal(t, testVals[i], vals[i])
		}

		keys, vals, err := sc.GetByPrefix("/master")
		assert.Nil(t, err)
		for i := range keys {
			assert.True(t, strings.HasPrefix(keys[i], "/master/"))
		}
		assert.Equal(t, len(keys), len(vals))
		assert.Equal(t, 21, len(keys))

		// Test get all configs
		keys, vals, err = sc.GetByPrefix("/")
		assert.Nil(t, err)
		assert.Equal(t, len(keys), len(vals))
		assert.Equal(t, 73, len(vals))

		// Test get configs with prefix not exist
		keys, vals, err = sc.GetByPrefix("/config")
		assert.Nil(t, err)
		assert.Equal(t, len(keys), len(vals))
		assert.Equal(t, 0, len(keys))
		assert.Equal(t, 0, len(vals))

		_, _, err = sc.GetByPrefix("//././../../../../../..//../")
		assert.Nil(t, err)
		_, _, err = sc.GetByPrefix("/master/./address")
		assert.Nil(t, err)
		_, _, err = sc.GetByPrefix(".")
		assert.Nil(t, err)
		_, _, err = sc.GetByPrefix("\\")
		assert.Nil(t, err)

	})

	t.Run("getConfigFiles", func(t *testing.T) {
		filePath := "../../configs"
		vipers, err := sc.getConfigFiles(filePath)
		assert.Nil(t, err)
		assert.NotNil(t, vipers[0])

		filePath = "/path/not/exists"
		_, err = sc.getConfigFiles(filePath)
		assert.NotNil(t, err)
		log.Println(err)
	})

	t.Run("Test saveToEtcd Normal", func(t *testing.T) {
		_, err = cli.Delete(ctx, "/test/root/config", clientv3.WithPrefix())
		require.Nil(t, err)

		v := viper.New()

		v.Set("a.suba1", "v1")
		v.Set("a.suba2", "v2")
		v.Set("a.suba3.subsuba1", "v3")
		v.Set("a.suba3.subsuba2", "v4")

		secondRootPath := "config"
		err := sc.saveToEtcd(v, secondRootPath)
		assert.Nil(t, err)

		value, err := sc.kv.Load(path.Join(secondRootPath, "a/suba1"))
		assert.Nil(t, err)
		assert.Equal(t, "v1", value)
		value, err = sc.kv.Load(path.Join(secondRootPath, "a/suba2"))
		assert.Nil(t, err)
		assert.Equal(t, "v2", value)
		value, err = sc.kv.Load(path.Join(secondRootPath, "a/suba3/subsuba1"))
		assert.Nil(t, err)
		assert.Equal(t, "v3", value)
		value, err = sc.kv.Load(path.Join(secondRootPath, "a/suba3/subsuba2"))
		assert.Nil(t, err)
		assert.Equal(t, "v4", value)

		keys, values, err := sc.kv.LoadWithPrefix(path.Join(secondRootPath, "a"))
		assert.Nil(t, err)
		assert.Equal(t, 4, len(keys))
		assert.Equal(t, 4, len(values))
		assert.ElementsMatch(t, []string{
			path.Join(sc.kv.GetPath(secondRootPath), "/a/suba1"),
			path.Join(sc.kv.GetPath(secondRootPath), "/a/suba2"),
			path.Join(sc.kv.GetPath(secondRootPath), "/a/suba3/subsuba1"),
			path.Join(sc.kv.GetPath(secondRootPath), "/a/suba3/subsuba2"),
		}, keys)
		assert.ElementsMatch(t, []string{"v1", "v2", "v3", "v4"}, values)

		keys = []string{
			"/a/suba1",
			"/a/suba2",
			"/a/suba3/subsuba1",
			"/a/suba3/subsuba2",
		}
		values, err = sc.Get(keys)
		assert.Nil(t, err)
		assert.ElementsMatch(t, []string{"v1", "v2", "v3", "v4"}, values)

		keysAfter, values, err := sc.GetByPrefix("/a")
		fmt.Println(keysAfter)
		assert.Nil(t, err)
		assert.ElementsMatch(t, []string{"v1", "v2", "v3", "v4"}, values)
		assert.ElementsMatch(t, keys, keysAfter)

	})

	t.Run("Test saveToEtcd Different value types", func(t *testing.T) {
		v := viper.New()

		v.Set("string", "string")
		v.Set("number", 1)
		v.Set("nil", nil)
		v.Set("float", 1.2)
		v.Set("intslice", []int{100, 200})
		v.Set("stringslice", []string{"a", "b"})
		v.Set("stringmapstring", map[string]string{"k1": "1", "k2": "2"})

		secondRootPath := "test_save_to_etcd_different_value_types"
		err := sc.saveToEtcd(v, secondRootPath)
		require.Nil(t, err)

		keys, values, err := sc.kv.LoadWithPrefix(path.Join("/", secondRootPath))
		assert.Nil(t, err)
		assert.Equal(t, 7, len(keys))
		assert.Equal(t, 7, len(values))

		assert.ElementsMatch(t, []string{
			path.Join(sc.kv.GetPath(secondRootPath), "nil"),
			path.Join(sc.kv.GetPath(secondRootPath), "string"),
			path.Join(sc.kv.GetPath(secondRootPath), "number"),
			path.Join(sc.kv.GetPath(secondRootPath), "float"),
			path.Join(sc.kv.GetPath(secondRootPath), "intslice"),
			path.Join(sc.kv.GetPath(secondRootPath), "stringslice"),
			path.Join(sc.kv.GetPath(secondRootPath), "stringmapstring"),
		}, keys)
		assert.ElementsMatch(t, []string{"", "string", "1", "1.2", "[100 200]", "[a b]", "map[k1:1 k2:2]"}, values)
	})
}
