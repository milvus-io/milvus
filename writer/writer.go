package src

import (
	"context"
	"github.com/czs007/suvlim/pulsar/schema"
	"github.com/czs007/suvlim/storage/pkg"
	"github.com/czs007/suvlim/storage/pkg/types"
)

type write_node_time_sync struct {
	delete_time_sync uint64
	insert_time_sync uint64
}

type write_node struct {
	open_segment_id         int64
	next_segment_id         int64
	next_segment_start_time uint64
	stroe                   *types.Store
	time_sync_table         *write_node_time_sync
}

func NewWriteNode(ctx context.Context, open_segment_id int64, time_sync uint64) (*write_node, error) {
	ctx = context.Background()
	tikv_store, err := storage.NewStore(ctx, "tikv")
	write_table_time_sync := &write_node_time_sync{delete_time_sync: time_sync, insert_time_sync: time_sync}
	if err != nil {
		return nil, err
	}
	return &write_node{
		stroe:           tikv_store,
		time_sync_table: write_table_time_sync,
	}, nil
}

func (s *write_node) InsertBatchData(ctx context.Context, data []schema.InsertMsg, time_sync uint64) error {
	return nil
}

func (s *write_node) DeleteBatchData(ctx context.Context, data []schema.DeleteMsg, time_sync uint64) error {
	return nil
}

func (s *write_node) AddNewSegment(segment_id int64, open_time uint64) error {
	return nil
}

func (s *write_node) UpdateInsertTimeSync(time_sync uint64) {
	s.time_sync_table.insert_time_sync = time_sync
}

func (s *write_node) UpdateDeleteTimeSync(time_sync uint64) {
	s.time_sync_table.delete_time_sync = time_sync
}
