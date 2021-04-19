package writenode

import (
	"context"
	"log"

	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	"go.etcd.io/etcd/clientv3"
)

type (
	flushSyncService struct {
		ctx           context.Context
		metaTable     *metaTable
		ddChan        chan *ddlFlushSyncMsg
		insertChan    chan *insertFlushSyncMsg
		ddFlushed     map[UniqueID]bool // Segment ID
		insertFlushed map[UniqueID]bool // Segment ID
	}
)

func newFlushSyncService(ctx context.Context,
	ddChan chan *ddlFlushSyncMsg, insertChan chan *insertFlushSyncMsg) *flushSyncService {

	service := &flushSyncService{
		ctx:           ctx,
		ddChan:        ddChan,
		insertChan:    insertChan,
		ddFlushed:     make(map[UniqueID]bool),
		insertFlushed: make(map[UniqueID]bool),
	}

	// New metaTable
	etcdAddr := Params.EtcdAddress
	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddr}})
	if err != nil {
		panic(err)
	}
	etcdKV := etcdkv.NewEtcdKV(etcdClient, Params.MetaRootPath)
	metaKV, err2 := NewMetaTable(etcdKV)
	if err2 != nil {
		panic(err2)
	}

	service.metaTable = metaKV
	return service
}

func (fService *flushSyncService) completeDDFlush(segID UniqueID) {
	if _, ok := fService.ddFlushed[segID]; !ok {
		fService.ddFlushed[segID] = true
		return
	}

	fService.ddFlushed[segID] = true
}

func (fService *flushSyncService) completeInsertFlush(segID UniqueID) {
	if _, ok := fService.insertFlushed[segID]; !ok {
		fService.insertFlushed[segID] = true
		return
	}
	fService.insertFlushed[segID] = true
}

func (fService *flushSyncService) FlushCompleted(segID UniqueID) bool {
	isddFlushed, ok := fService.ddFlushed[segID]
	if !ok {
		return false
	}

	isinsertFlushed, ok := fService.insertFlushed[segID]
	if !ok {
		return false
	}
	return isddFlushed && isinsertFlushed
}

func (fService *flushSyncService) start() {
	for {
		select {
		case <-fService.ctx.Done():
			return

		case ddFlushMsg := <-fService.ddChan:
			if ddFlushMsg == nil {
				continue
			}
			if !ddFlushMsg.flushCompleted {
				err := fService.metaTable.AppendDDLBinlogPaths(ddFlushMsg.collID, ddFlushMsg.paths)
				if err != nil {
					log.Println("Append segBinlog Error")
					// GOOSE TODO error handling
				}
				continue
			}
			fService.completeDDFlush(ddFlushMsg.segID)

		case insertFlushMsg := <-fService.insertChan:
			if insertFlushMsg == nil {
				continue
			}
			if !insertFlushMsg.flushCompleted {
				err := fService.metaTable.AppendSegBinlogPaths(insertFlushMsg.ts, insertFlushMsg.segID, insertFlushMsg.fieldID,
					insertFlushMsg.paths)
				if err != nil {
					log.Println("Append segBinlog Error")
					// GOOSE TODO error handling
				}
				continue
			}
			fService.completeInsertFlush(insertFlushMsg.segID)

			if fService.FlushCompleted(insertFlushMsg.segID) {
				log.Printf("Seg(%d) flush completed.", insertFlushMsg.segID)
				fService.metaTable.CompleteFlush(insertFlushMsg.ts, insertFlushMsg.segID)
			}
		}
	}
}
