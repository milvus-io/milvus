package dataservice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/types"
)

func TestRegisterNode(t *testing.T) {
	Params.Init()
	Params.DataNodeNum = 1
	var err error
	factory := msgstream.NewPmsFactory()
	m := map[string]interface{}{
		"pulsarAddress":  Params.PulsarAddress,
		"receiveBufSize": 1024,
		"pulsarBufSize":  1024,
	}
	err = factory.SetParams(m)
	assert.Nil(t, err)
	svr, err := CreateServer(context.TODO(), factory)
	assert.Nil(t, err)
	ms := newMockMasterService()
	err = ms.Init()
	assert.Nil(t, err)
	err = ms.Start()
	assert.Nil(t, err)
	defer ms.Stop()
	svr.SetMasterClient(ms)
	svr.createDataNodeClient = func(addr string) types.DataNode {
		return newMockDataNodeClient(0)
	}
	assert.Nil(t, err)
	err = svr.Init()
	assert.Nil(t, err)
	err = svr.Start()
	assert.Nil(t, err)
	defer svr.Stop()
	t.Run("register node", func(t *testing.T) {
		resp, err := svr.RegisterNode(context.TODO(), &datapb.RegisterNodeRequest{
			Base: &commonpb.MsgBase{
				MsgType:   0,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  1000,
			},
			Address: &commonpb.Address{
				Ip:   "localhost",
				Port: 1000,
			},
		})
		assert.Nil(t, err)
		assert.EqualValues(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.EqualValues(t, Params.DataNodeNum, svr.cluster.GetNumOfNodes())
		assert.EqualValues(t, []int64{1000}, svr.cluster.GetNodeIDs())
	})
}
