package miniokv

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

func TestMinioStats(t *testing.T) {
	var p paramtable.BaseTable
	p.Init()
	endPoint, _ := p.Load("_MinioAddress")
	accessKeyID, _ := p.Load("minio.accessKeyID")
	secretAccessKey, _ := p.Load("minio.secretAccessKey")
	useSSLStr, _ := p.Load("minio.useSSL")
	useSSL, _ := strconv.ParseBool(useSSLStr)
	option := &Option{
		Address:           endPoint,
		AccessKeyID:       accessKeyID,
		SecretAccessKeyID: secretAccessKey,
		UseSSL:            useSSL,
		BucketName:        "teststats",
		CreateBucket:      true,
	}
	cli, err := NewMinIOKV(context.TODO(), option)
	assert.Nil(t, err)

	startCh := make(chan struct{})
	receiveCh := make(chan struct{})

	w := NewMinioStatsWatcher(cli.minioClient, "teststats")
	w.helper.eventAfterStartWatch = func() {
		var e struct{}
		startCh <- e
	}
	w.helper.eventAfterNotify = func() {
		var e struct{}
		receiveCh <- e
	}
	go w.StartBackground(context.TODO())

	<-startCh
	err = cli.Save("a", string([]byte{65, 65, 65, 65, 65}))
	assert.Nil(t, err)

	<-receiveCh

	size := w.GetObjectCreateSize()
	assert.EqualValues(t, 5, size)
}
