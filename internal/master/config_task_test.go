package master

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
)

func TestMaster_ConfigTask(t *testing.T) {
	Init()

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	etcdCli, err := clientv3.New(clientv3.Config{Endpoints: []string{Params.EtcdAddress}})
	require.Nil(t, err)
	_, err = etcdCli.Delete(ctx, "/test/root", clientv3.WithPrefix())
	require.Nil(t, err)

	Params = ParamTable{
		Address: Params.Address,
		Port:    Params.Port,

		EtcdAddress:   Params.EtcdAddress,
		EtcdRootPath:  "/test/root",
		PulsarAddress: Params.PulsarAddress,

		ProxyIDList:     []typeutil.UniqueID{1, 2},
		WriteNodeIDList: []typeutil.UniqueID{3, 4},

		TopicNum:                    5,
		QueryNodeNum:                3,
		SoftTimeTickBarrierInterval: 300,

		// segment
		SegmentSize:           536870912 / 1024 / 1024,
		SegmentSizeFactor:     0.75,
		DefaultRecordSize:     1024,
		MinSegIDAssignCnt:     1048576 / 1024,
		MaxSegIDAssignCnt:     Params.MaxSegIDAssignCnt,
		SegIDAssignExpiration: 2000,

		// msgChannel
		ProxyTimeTickChannelNames:     []string{"proxy1", "proxy2"},
		WriteNodeTimeTickChannelNames: []string{"write3", "write4"},
		InsertChannelNames:            []string{"dm0", "dm1"},
		K2SChannelNames:               []string{"k2s0", "k2s1"},
		QueryNodeStatsChannelName:     "statistic",
		MsgChannelSubName:             Params.MsgChannelSubName,
	}

	svr, err := CreateServer(ctx)
	require.Nil(t, err)
	err = svr.Run(10002)
	defer svr.Close()
	require.Nil(t, err)

	conn, err := grpc.DialContext(ctx, "127.0.0.1:10002", grpc.WithInsecure(), grpc.WithBlock())
	require.Nil(t, err)
	defer conn.Close()

	cli := masterpb.NewMasterClient(conn)
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

	sc := SysConfig{kv: svr.kvBase}
	sc.InitFromFile(".")

	configRequest := &internalpb.SysConfigRequest{
		MsgType:     internalpb.MsgType_kGetSysConfigs,
		ReqID:       1,
		Timestamp:   11,
		ProxyID:     1,
		Keys:        testKeys,
		KeyPrefixes: []string{},
	}

	response, err := cli.GetSysConfigs(ctx, configRequest)
	assert.Nil(t, err)
	assert.ElementsMatch(t, testKeys, response.Keys)
	assert.ElementsMatch(t, testVals, response.Values)
	assert.Equal(t, len(response.GetKeys()), len(response.GetValues()))

	configRequest = &internalpb.SysConfigRequest{
		MsgType:     internalpb.MsgType_kGetSysConfigs,
		ReqID:       1,
		Timestamp:   11,
		ProxyID:     1,
		Keys:        []string{},
		KeyPrefixes: []string{"/master"},
	}

	response, err = cli.GetSysConfigs(ctx, configRequest)
	assert.Nil(t, err)
	for i := range response.GetKeys() {
		assert.True(t, strings.HasPrefix(response.GetKeys()[i], "/master"))
	}
	assert.Equal(t, len(response.GetKeys()), len(response.GetValues()))

	t.Run("Test duplicate keys and key prefix", func(t *testing.T) {
		configRequest.Keys = []string{}
		configRequest.KeyPrefixes = []string{"/master"}

		resp, err := cli.GetSysConfigs(ctx, configRequest)
		require.Nil(t, err)
		assert.Equal(t, len(resp.GetKeys()), len(resp.GetValues()))
		assert.NotEqual(t, 0, len(resp.GetKeys()))

		configRequest.Keys = []string{"/master/port"}
		configRequest.KeyPrefixes = []string{"/master"}

		respDup, err := cli.GetSysConfigs(ctx, configRequest)
		require.Nil(t, err)
		assert.Equal(t, len(respDup.GetKeys()), len(respDup.GetValues()))
		assert.NotEqual(t, 0, len(respDup.GetKeys()))
		assert.Equal(t, len(respDup.GetKeys()), len(resp.GetKeys()))
	})

}
