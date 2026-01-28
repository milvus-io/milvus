package proxy

import (
	"context"
	"sort"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// repackUpsertDataForStreamingService repacks upsert data (insert + delete) by hashing primary keys
func repackUpsertDataForStreamingService(
	ctx context.Context,
	channelNames []string,
	insertMsg *msgstream.InsertMsg,
	deleteMsg *msgstream.DeleteMsg,
	result *milvuspb.MutationResult,
	idAllocator allocator.Interface,
	ts uint64,
	dbName string,
	ez *message.CipherConfig,
) ([]message.MutableMessage, error) {
	// Hash insert data by primary keys
	channel2InsertRowOffsets := assignChannelsByPK(result.IDs, channelNames, insertMsg)

	// Hash delete data by primary keys
	deleteHashValues := typeutil.HashPK2Channels(deleteMsg.PrimaryKeys, channelNames)

	// Group delete primary keys by channel
	channel2DeletePKs := make(map[string]*schemapb.IDs)
	for idx, hashValue := range deleteHashValues {
		channelName := channelNames[hashValue]
		if channel2DeletePKs[channelName] == nil {
			channel2DeletePKs[channelName] = &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: make([]int64, 0)},
				},
			}
			if deleteMsg.PrimaryKeys.GetStrId() != nil {
				channel2DeletePKs[channelName] = &schemapb.IDs{
					IdField: &schemapb.IDs_StrId{
						StrId: &schemapb.StringArray{Data: make([]string, 0)},
					},
				}
			}
		}

		// Append primary key to channel group
		if intPKs := deleteMsg.PrimaryKeys.GetIntId(); intPKs != nil {
			channel2DeletePKs[channelName].GetIntId().Data = append(
				channel2DeletePKs[channelName].GetIntId().Data,
				intPKs.Data[idx],
			)
		} else if strPKs := deleteMsg.PrimaryKeys.GetStrId(); strPKs != nil {
			channel2DeletePKs[channelName].GetStrId().Data = append(
				channel2DeletePKs[channelName].GetStrId().Data,
				strPKs.Data[idx],
			)
		}
	}

	partitionName := insertMsg.PartitionName
	partitionID, err := globalMetaCache.GetPartitionID(ctx, dbName, insertMsg.CollectionName, partitionName)
	if err != nil {
		return nil, err
	}

	// Build upsert messages for each channel
	messages := make([]message.MutableMessage, 0)
	for channel, insertRowOffsets := range channel2InsertRowOffsets {
		// Generate insert messages for this channel
		insertMsgs, err := genInsertMsgsByPartition(ctx, 0, partitionID, partitionName, insertRowOffsets, channel, insertMsg)
		if err != nil {
			return nil, err
		}

		// Get delete data for this channel
		deletePKs := channel2DeletePKs[channel]
		if deletePKs == nil {
			// Initialize with correct PK type based on deleteMsg
			if deleteMsg.PrimaryKeys.GetStrId() != nil {
				deletePKs = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: make([]string, 0)}}}
			} else {
				deletePKs = &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: make([]int64, 0)}}}
			}
		}

		// Generate delete messages for this channel
		deleteMsgsForChannel, _, err := repackDeleteMsgByHash(
			ctx, deletePKs, []string{channel}, idAllocator, ts,
			deleteMsg.CollectionID, deleteMsg.CollectionName,
			deleteMsg.PartitionID, deleteMsg.PartitionName, dbName,
		)
		if err != nil {
			return nil, err
		}

		// Combine insert and delete into upsert messages
		for idx, insertMsgItem := range insertMsgs {
			insertRequest := insertMsgItem.(*msgstream.InsertMsg).InsertRequest

			var deleteRequest *msgpb.DeleteRequest
			if len(deleteMsgsForChannel) > 0 {
				// Use the first delete message, or corresponding one if available
				deleteIdx := idx
				if deleteIdx >= len(deleteMsgsForChannel[0]) {
					deleteIdx = len(deleteMsgsForChannel[0]) - 1
				}
				if deleteIdx >= 0 && deleteIdx < len(deleteMsgsForChannel[0]) {
					deleteRequest = deleteMsgsForChannel[0][deleteIdx].DeleteRequest
				}
			}

			// If no delete request, create empty one
			if deleteRequest == nil {
				// Initialize with correct PK type based on deleteMsg
				var primaryKeys *schemapb.IDs
				if deleteMsg.PrimaryKeys.GetStrId() != nil {
					primaryKeys = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: make([]string, 0)}}}
				} else {
					primaryKeys = &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: make([]int64, 0)}}}
				}
				deleteRequest = &msgpb.DeleteRequest{
					Base:           insertRequest.Base,
					CollectionID:   deleteMsg.CollectionID,
					PartitionID:    deleteMsg.PartitionID,
					CollectionName: deleteMsg.CollectionName,
					PartitionName:  deleteMsg.PartitionName,
					DbName:         dbName,
					ShardName:      channel,
					PrimaryKeys:    primaryKeys,
				}
			}

			// Build upsert message
			upsertBody := &message.UpsertMessageBody{
				InsertRequest: insertRequest,
				DeleteRequest: deleteRequest,
			}

			deleteRows := uint64(0)
			if deleteRequest.PrimaryKeys.GetIntId() != nil {
				deleteRows = uint64(len(deleteRequest.PrimaryKeys.GetIntId().Data))
			} else if deleteRequest.PrimaryKeys.GetStrId() != nil {
				deleteRows = uint64(len(deleteRequest.PrimaryKeys.GetStrId().Data))
			}

			msg, err := message.NewUpsertMessageBuilderV2().
				WithVChannel(channel).
				WithHeader(&message.UpsertMessageHeader{
					CollectionId: insertMsg.CollectionID,
					Partitions: []*message.PartitionSegmentAssignment{
						{
							PartitionId: partitionID,
							Rows:        insertRequest.GetNumRows(),
							BinarySize:  0,
						},
					},
					DeleteRows: deleteRows,
				}).
				WithBody(upsertBody).
				WithCipher(ez).
				BuildMutable()
			if err != nil {
				return nil, err
			}
			messages = append(messages, msg)
		}
	}

	return messages, nil
}

// repackUpsertDataWithPartitionKeyForStreamingService repacks upsert data with partition key support
func repackUpsertDataWithPartitionKeyForStreamingService(
	ctx context.Context,
	channelNames []string,
	insertMsg *msgstream.InsertMsg,
	deleteMsg *msgstream.DeleteMsg,
	result *milvuspb.MutationResult,
	partitionKeys *schemapb.FieldData,
	idAllocator allocator.Interface,
	ts uint64,
	dbName string,
	ez *message.CipherConfig,
) ([]message.MutableMessage, error) {
	// Hash insert data by primary keys
	channel2InsertRowOffsets := assignChannelsByPK(result.IDs, channelNames, insertMsg)

	// Get default partitions in partition key mode
	partitionNames, err := getDefaultPartitionsInPartitionKeyMode(ctx, dbName, insertMsg.CollectionName)
	if err != nil {
		log.Ctx(ctx).Warn("get default partition names failed in partition key mode",
			zap.String("collectionName", insertMsg.CollectionName),
			zap.Error(err))
		return nil, err
	}

	// Get partition IDs
	partitionIDs := make(map[string]int64, 0)
	for _, partitionName := range partitionNames {
		partitionID, err := globalMetaCache.GetPartitionID(ctx, dbName, insertMsg.CollectionName, partitionName)
		if err != nil {
			log.Ctx(ctx).Warn("get partition id failed",
				zap.String("collectionName", insertMsg.CollectionName),
				zap.String("partitionName", partitionName),
				zap.Error(err))
			return nil, err
		}
		partitionIDs[partitionName] = partitionID
	}

	// Hash partition keys to partitions
	hashValues, err := typeutil.HashKey2Partitions(partitionKeys, partitionNames)
	if err != nil {
		log.Ctx(ctx).Warn("hash partition keys to partitions failed",
			zap.String("collectionName", insertMsg.CollectionName),
			zap.Error(err))
		return nil, err
	}

	// Hash delete data by primary keys
	deleteHashValues := typeutil.HashPK2Channels(deleteMsg.PrimaryKeys, channelNames)
	channel2DeletePKs := make(map[string]*schemapb.IDs)
	for idx, hashValue := range deleteHashValues {
		channelName := channelNames[hashValue]
		if channel2DeletePKs[channelName] == nil {
			channel2DeletePKs[channelName] = &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: make([]int64, 0)},
				},
			}
			if deleteMsg.PrimaryKeys.GetStrId() != nil {
				channel2DeletePKs[channelName] = &schemapb.IDs{
					IdField: &schemapb.IDs_StrId{
						StrId: &schemapb.StringArray{Data: make([]string, 0)},
					},
				}
			}
		}

		if intPKs := deleteMsg.PrimaryKeys.GetIntId(); intPKs != nil {
			channel2DeletePKs[channelName].GetIntId().Data = append(
				channel2DeletePKs[channelName].GetIntId().Data,
				intPKs.Data[idx],
			)
		} else if strPKs := deleteMsg.PrimaryKeys.GetStrId(); strPKs != nil {
			channel2DeletePKs[channelName].GetStrId().Data = append(
				channel2DeletePKs[channelName].GetStrId().Data,
				strPKs.Data[idx],
			)
		}
	}

	messages := make([]message.MutableMessage, 0)
	for channel, insertRowOffsets := range channel2InsertRowOffsets {
		// Group insert row offsets by partition
		partition2RowOffsets := make(map[string][]int)
		for _, idx := range insertRowOffsets {
			partitionName := partitionNames[hashValues[idx]]
			if _, ok := partition2RowOffsets[partitionName]; !ok {
				partition2RowOffsets[partitionName] = []int{}
			}
			partition2RowOffsets[partitionName] = append(partition2RowOffsets[partitionName], idx)
		}

		// Sort partition names by their IDs for deterministic ordering
		sortedPartitionNames := make([]string, 0, len(partition2RowOffsets))
		for partitionName := range partition2RowOffsets {
			sortedPartitionNames = append(sortedPartitionNames, partitionName)
		}
		sort.Slice(sortedPartitionNames, func(i, j int) bool {
			return partitionIDs[sortedPartitionNames[i]] < partitionIDs[sortedPartitionNames[j]]
		})

		// Merge all rows from all partitions in partition order (contiguous ranges)
		allRowOffsets := make([]int, 0)
		partitionAssignments := make([]*message.PartitionSegmentAssignment, 0, len(sortedPartitionNames))

		for _, partitionName := range sortedPartitionNames {
			rows := partition2RowOffsets[partitionName]
			allRowOffsets = append(allRowOffsets, rows...)

			partitionAssignments = append(partitionAssignments, &message.PartitionSegmentAssignment{
				PartitionId: partitionIDs[partitionName],
				Rows:        uint64(len(rows)),
				BinarySize:  0,
			})
		}

		// Create single merged InsertRequest with all rows from all partitions
		mergedInsertMsgs, err := genInsertMsgsByPartition(ctx, 0, 0, "", allRowOffsets, channel, insertMsg)
		if err != nil {
			return nil, err
		}

		// Get delete data for this channel
		deletePKs := channel2DeletePKs[channel]
		if deletePKs == nil {
			deletePKs = &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: make([]int64, 0)}}}
		}

		// Generate delete messages for this channel (using first partition's info)
		firstPartitionName := sortedPartitionNames[0]
		deleteMsgsForChannel, _, err := repackDeleteMsgByHash(
			ctx, deletePKs, []string{channel}, idAllocator, ts,
			deleteMsg.CollectionID, deleteMsg.CollectionName,
			partitionIDs[firstPartitionName], firstPartitionName, dbName,
		)
		if err != nil {
			return nil, err
		}

		// Combine merged insert and delete into upsert messages
		for idx, insertMsgItem := range mergedInsertMsgs {
			insertRequest := insertMsgItem.(*msgstream.InsertMsg).InsertRequest

			var deleteRequest *msgpb.DeleteRequest
			if len(deleteMsgsForChannel) > 0 {
				deleteIdx := idx
				if deleteIdx >= len(deleteMsgsForChannel[0]) {
					deleteIdx = len(deleteMsgsForChannel[0]) - 1
				}
				if deleteIdx >= 0 && deleteIdx < len(deleteMsgsForChannel[0]) {
					deleteRequest = deleteMsgsForChannel[0][deleteIdx].DeleteRequest
				}
			}

			if deleteRequest == nil {
				// Initialize with correct PK type based on deleteMsg
				var primaryKeys *schemapb.IDs
				if deleteMsg.PrimaryKeys.GetStrId() != nil {
					primaryKeys = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: make([]string, 0)}}}
				} else {
					primaryKeys = &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: make([]int64, 0)}}}
				}
				deleteRequest = &msgpb.DeleteRequest{
					Base:           insertRequest.Base,
					CollectionID:   deleteMsg.CollectionID,
					PartitionID:    partitionIDs[firstPartitionName],
					CollectionName: deleteMsg.CollectionName,
					PartitionName:  firstPartitionName,
					DbName:         dbName,
					ShardName:      channel,
					PrimaryKeys:    primaryKeys,
				}
			}

			upsertBody := &message.UpsertMessageBody{
				InsertRequest: insertRequest,
				DeleteRequest: deleteRequest,
			}

			deleteRows := uint64(0)
			if deleteRequest.PrimaryKeys.GetIntId() != nil {
				deleteRows = uint64(len(deleteRequest.PrimaryKeys.GetIntId().Data))
			} else if deleteRequest.PrimaryKeys.GetStrId() != nil {
				deleteRows = uint64(len(deleteRequest.PrimaryKeys.GetStrId().Data))
			}

			// Build multi-partition upsert message
			msg, err := message.NewUpsertMessageBuilderV2().
				WithVChannel(channel).
				WithHeader(&message.UpsertMessageHeader{
					CollectionId: insertMsg.CollectionID,
					Partitions:   partitionAssignments, // Multiple partitions
					DeleteRows:   deleteRows,
				}).
				WithBody(upsertBody).
				WithCipher(ez).
				BuildMutable()
			if err != nil {
				return nil, err
			}
			messages = append(messages, msg)
		}
	}

	return messages, nil
}
