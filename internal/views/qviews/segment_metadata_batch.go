package qviews

import (
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// Bound aggregate messages without rejecting an otherwise valid large segment.
// A single snapshot larger than this target is sent alone, subject to the RPC's
// configured maximum. No snapshot is split across messages.
const segmentMetadataBatchBytes = 4 << 20

func embeddedMessageSize(message proto.Message) int {
	return 1 + protowire.SizeBytes(proto.Size(message))
}

func SendSegmentLoadInfoRequests(request *querypb.WatchQueryViewSegmentLoadInfoRequest, send func(*querypb.WatchQueryViewSegmentLoadInfoRequest) error) error {
	batch := &querypb.WatchQueryViewSegmentLoadInfoRequest{Base: request.Base}
	size := proto.Size(batch)
	flush := func() error {
		if len(batch.Subscribe)+len(batch.Unsubscribe) == 0 {
			return nil
		}
		if err := send(batch); err != nil {
			return err
		}
		batch = &querypb.WatchQueryViewSegmentLoadInfoRequest{Base: request.Base}
		size = proto.Size(batch)
		return nil
	}
	for _, subscription := range request.Unsubscribe {
		entrySize := embeddedMessageSize(subscription)
		if size+entrySize > segmentMetadataBatchBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		batch.Unsubscribe = append(batch.Unsubscribe, subscription)
		size += entrySize
	}
	for _, subscription := range request.Subscribe {
		entrySize := embeddedMessageSize(subscription)
		if size+entrySize > segmentMetadataBatchBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		batch.Subscribe = append(batch.Subscribe, subscription)
		size += entrySize
	}
	return flush()
}

func SendSegmentLoadInfoResponses(response *querypb.WatchQueryViewSegmentLoadInfoResponse, send func(*querypb.WatchQueryViewSegmentLoadInfoResponse) error) error {
	if len(response.Snapshots) == 0 {
		return send(response)
	}
	indexes := make(map[int64]*querypb.QueryViewCollectionIndexes, len(response.CollectionIndexes))
	indexSizes := make(map[int64]int, len(response.CollectionIndexes))
	for _, collection := range response.CollectionIndexes {
		indexes[collection.CollectionID] = collection
		indexSizes[collection.CollectionID] = embeddedMessageSize(collection)
	}
	batch := &querypb.WatchQueryViewSegmentLoadInfoResponse{Status: response.Status}
	collections := make(map[int64]bool)
	size := proto.Size(batch)
	for _, snapshot := range response.Snapshots {
		entrySize := embeddedMessageSize(snapshot)
		indexSize := 0
		if !collections[snapshot.CollectionID] {
			indexSize = indexSizes[snapshot.CollectionID]
		}
		if len(batch.Snapshots) > 0 && size+entrySize+indexSize > segmentMetadataBatchBytes {
			if err := send(batch); err != nil {
				return err
			}
			batch = &querypb.WatchQueryViewSegmentLoadInfoResponse{Status: response.Status}
			collections = make(map[int64]bool)
			size = proto.Size(batch)
		}
		if !collections[snapshot.CollectionID] {
			if index := indexes[snapshot.CollectionID]; index != nil {
				batch.CollectionIndexes = append(batch.CollectionIndexes, index)
				size += indexSizes[snapshot.CollectionID]
			}
			collections[snapshot.CollectionID] = true
		}
		batch.Snapshots = append(batch.Snapshots, snapshot)
		size += entrySize
	}
	return send(batch)
}
