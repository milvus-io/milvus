package master

import (
	"context"
	"log"
)

type IndexLoadInfo struct {
	segmentID      UniqueID
	fieldID        UniqueID
	fieldName      string
	indexFilePaths []string
}

type IndexLoadScheduler struct {
	indexLoadChan chan *IndexLoadInfo
	client        LoadIndexClient
	metaTable     *metaTable

	ctx    context.Context
	cancel context.CancelFunc
}

func NewIndexLoadScheduler(ctx context.Context, client LoadIndexClient, metaTable *metaTable) *IndexLoadScheduler {
	ctx2, cancel := context.WithCancel(ctx)
	indexLoadChan := make(chan *IndexLoadInfo, 100)

	return &IndexLoadScheduler{
		client:        client,
		metaTable:     metaTable,
		indexLoadChan: indexLoadChan,
		ctx:           ctx2,
		cancel:        cancel,
	}
}

func (scheduler *IndexLoadScheduler) schedule(info interface{}) error {
	indexLoadInfo := info.(*IndexLoadInfo)
	err := scheduler.client.LoadIndex(indexLoadInfo.indexFilePaths, indexLoadInfo.segmentID, indexLoadInfo.fieldID, indexLoadInfo.fieldName)
	//TODO: Save data to meta table
	if err != nil {
		return err
	}

	return nil
}
func (scheduler *IndexLoadScheduler) scheduleLoop() {
	for {
		select {
		case info := <-scheduler.indexLoadChan:
			err := scheduler.schedule(info)
			if err != nil {
				log.Println(err)
			}
		case <-scheduler.ctx.Done():
			log.Print("server is closed, exit flush scheduler loop")
			return
		}
	}
}

func (scheduler *IndexLoadScheduler) Enqueue(info interface{}) error {
	scheduler.indexLoadChan <- info.(*IndexLoadInfo)
	return nil
}

func (scheduler *IndexLoadScheduler) Start() error {
	go scheduler.scheduleLoop()
	return nil
}

func (scheduler *IndexLoadScheduler) Close() {
	scheduler.cancel()
}
