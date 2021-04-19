package proxy

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/proxy/mock"
	"testing"
	"time"
)

func TestMatserOracle_GetTimestamp(t *testing.T) {
	tso, _:= NewMasterTSO(&mock.TSOClient{})
	defer tso.Close()

	ctx := context.TODO()
	ts0, err := tso.GetTimestamp(ctx, 100)
	assert.Nil(t, err)
	ts1, err := tso.GetTimestamp(ctx, 100)
	t.Logf("ts0=%v, ts1=%v", ts0, ts1)
	assert.Nil(t, err)
	assert.Greater(t, ts1, ts0)
	assert.Greater(t, ts1, ts0 + 99)

	time.Sleep(time.Second * 3)
	ts0, err = tso.GetTimestamp(ctx, 100)
	assert.Nil(t, err)
	ts1, err = tso.GetTimestamp(ctx, 100)
	t.Logf("ts0=%v, ts1=%v", ts0, ts1)
	assert.Nil(t, err)
	assert.Greater(t, ts1, ts0)
	assert.Greater(t, ts1, ts0 + 99)

	_, err = tso.GetTimestamp(ctx, 2<<30)
	assert.NotNil(t, err)
	t.Log(err)
}
