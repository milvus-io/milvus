package writenode

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/clientv3"

	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
)

func clearEtcd(rootPath string) error {
	etcdAddr := Params.EtcdAddress
	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	if err != nil {
		return err
	}
	etcdKV := etcdkv.NewEtcdKV(etcdClient, rootPath)

	err = etcdKV.RemoveWithPrefix("writer/segment")
	if err != nil {
		return err
	}
	_, _, err = etcdKV.LoadWithPrefix("writer/segment")
	if err != nil {
		return err
	}
	log.Println("Clear ETCD with prefix writer/segment ")

	err = etcdKV.RemoveWithPrefix("writer/ddl")
	if err != nil {
		return err
	}
	_, _, err = etcdKV.LoadWithPrefix("writer/ddl")
	if err != nil {
		return err
	}
	log.Println("Clear ETCD with prefix writer/ddl")
	return nil

}

func TestFlushSyncService_Start(t *testing.T) {
	const ctxTimeInMillisecond = 3000
	const closeWithDeadline = false
	var ctx context.Context
	var cancel context.CancelFunc

	if closeWithDeadline {
		d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
		ctx, cancel = context.WithDeadline(context.Background(), d)
		defer cancel()
	} else {
		// ctx = context.Background()
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
	}

	ddChan := make(chan *ddlFlushSyncMsg, 10)
	defer close(ddChan)
	insertChan := make(chan *insertFlushSyncMsg, 10)
	defer close(insertChan)

	testPath := "/test/writenode/root/meta"
	err := clearEtcd(testPath)
	require.NoError(t, err)
	Params.MetaRootPath = testPath
	fService := newFlushSyncService(ctx, ddChan, insertChan)
	assert.Equal(t, testPath, fService.metaTable.client.(*etcdkv.EtcdKV).GetPath("."))

	t.Run("FlushSyncService", func(t *testing.T) {
		go fService.start()

		SegID := UniqueID(100)
		ddMsgs := genDdlFlushSyncMsgs(SegID)
		insertMsgs := geninsertFlushSyncMsgs(SegID)

		for _, msg := range ddMsgs {
			ddChan <- msg
			time.Sleep(time.Millisecond * 50)
		}

		for _, msg := range insertMsgs {
			insertChan <- msg
			time.Sleep(time.Millisecond * 50)
		}

		ret, err := fService.metaTable.getSegBinlogPaths(SegID)
		assert.NoError(t, err)
		assert.Equal(t, map[int64][]string{
			0: {"x", "y", "z"},
			1: {"x", "y", "z"},
			2: {"x", "y", "z"},
			3: {"x", "y", "z"},
			4: {"x", "y", "z"},
		}, ret)

		ts, err := fService.metaTable.getFlushOpenTime(SegID)
		assert.NoError(t, err)
		assert.Equal(t, Timestamp(1000), ts)

		ts, err = fService.metaTable.getFlushCloseTime(SegID)
		assert.NoError(t, err)
		assert.Equal(t, Timestamp(2010), ts)

		cp, err := fService.metaTable.checkFlushComplete(SegID)
		assert.NoError(t, err)
		assert.Equal(t, true, cp)

		cp, err = fService.metaTable.checkFlushComplete(SegID)
		assert.NoError(t, err)
		assert.Equal(t, true, cp)

	})
}

func genDdlFlushSyncMsgs(segID UniqueID) []*ddlFlushSyncMsg {
	ret := make([]*ddlFlushSyncMsg, 0)
	for i := 0; i < 5; i++ {
		ret = append(ret, &ddlFlushSyncMsg{
			flushCompleted: false,
			ddlBinlogPathMsg: ddlBinlogPathMsg{
				collID: UniqueID(100),
				paths:  []string{"a", "b", "c"},
			},
		})
	}

	ret = append(ret, &ddlFlushSyncMsg{
		flushCompleted: true,
		ddlBinlogPathMsg: ddlBinlogPathMsg{
			segID: segID,
		},
	})
	return ret
}

func geninsertFlushSyncMsgs(segID UniqueID) []*insertFlushSyncMsg {
	ret := make([]*insertFlushSyncMsg, 0)
	for i := 0; i < 5; i++ {
		ret = append(ret, &insertFlushSyncMsg{
			flushCompleted: false,
			insertBinlogPathMsg: insertBinlogPathMsg{
				ts:      Timestamp(1000 + i),
				segID:   segID,
				fieldID: int64(i),
				paths:   []string{"x", "y", "z"},
			},
		})
	}

	ret = append(ret, &insertFlushSyncMsg{
		flushCompleted: true,
		insertBinlogPathMsg: insertBinlogPathMsg{
			ts:    Timestamp(2010),
			segID: segID,
		},
	})
	return ret
}
