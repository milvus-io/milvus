///go:build L0

package testcases

import (
	"strings"
	"testing"
	"time"

	clientv2 "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
	"github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// test connect and close, connect again
func TestConnectClose(t *testing.T) {
	// connect
	ctx := helper.CreateContext(t, time.Second*common.DefaultTimeout)
	mc, errConnect := base.NewMilvusClient(ctx, &defaultCfg)
	common.CheckErr(t, errConnect, true)

	// verify that connect success
	listOpt := clientv2.NewListCollectionOption()
	_, errList := mc.ListCollections(ctx, listOpt)
	common.CheckErr(t, errList, true)

	// close connect and verify
	err := mc.Close(ctx)
	common.CheckErr(t, err, true)
	_, errList2 := mc.ListCollections(ctx, listOpt)
	common.CheckErr(t, errList2, false, "service not ready[SDK=0]: not connected")

	// connect again
	mc, errConnect2 := base.NewMilvusClient(ctx, &defaultCfg)
	common.CheckErr(t, errConnect2, true)
	_, errList3 := mc.ListCollections(ctx, listOpt)
	common.CheckErr(t, errList3, true)
}

func genInvalidClientConfig() []clientv2.ClientConfig {
	invalidClientConfigs := []clientv2.ClientConfig{
		{Address: "aaa"},                                    // not exist address
		{Address: strings.Split(*addr, ":")[0]},             // Address=localhost
		{Address: strings.Split(*addr, ":")[1]},             // Address=19530
		{Address: *addr, Username: "aaa"},                   // not exist username
		{Address: *addr, Username: "root", Password: "aaa"}, // wrong password
		{Address: *addr, DBName: "aaa"},                     // not exist db
	}
	return invalidClientConfigs
}

// test connect with timeout and invalid addr
func TestConnectInvalidAddr(t *testing.T) {
	// connect
	ctx := helper.CreateContext(t, time.Second*5)
	for _, invalidCfg := range genInvalidClientConfig() {
		cfg := invalidCfg
		_, errConnect := base.NewMilvusClient(ctx, &cfg)
		common.CheckErr(t, errConnect, false, "context deadline exceeded")
	}
}

// test connect repeatedly
func TestConnectRepeat(t *testing.T) {
	// connect
	ctx := helper.CreateContext(t, time.Second*10)

	_, errConnect := base.NewMilvusClient(ctx, &defaultCfg)
	common.CheckErr(t, errConnect, true)

	// connect again
	mc, errConnect2 := base.NewMilvusClient(ctx, &defaultCfg)
	common.CheckErr(t, errConnect2, true)

	_, err := mc.ListCollections(ctx, clientv2.NewListCollectionOption())
	common.CheckErr(t, err, true)
}

// test close repeatedly
func TestCloseRepeat(t *testing.T) {
	// connect
	ctx := helper.CreateContext(t, time.Second*10)
	mc, errConnect2 := base.NewMilvusClient(ctx, &defaultCfg)
	common.CheckErr(t, errConnect2, true)

	// close and again
	err := mc.Close(ctx)
	common.CheckErr(t, err, true)
	err = mc.Close(ctx)
	common.CheckErr(t, err, true)
}
