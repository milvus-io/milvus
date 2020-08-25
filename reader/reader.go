package reader

import "../pulsar/schema"

func milvusInsertMock(collectionName string, partitionTag string, entityIds *[]int64, timestamps *[]int64, dataChunk [][]*schema.FieldValue) ResultEntityIds {
	return ResultEntityIds{}
}

func milvusDeleteMock(collectionName string, entityIds *[]int64, timestamps *[]int64) ResultEntityIds {
	return ResultEntityIds{}
}

func milvusSearchMock(collectionName string, queryString string, timestamps *[]int64, vectorRecord *[]schema.VectorRecord) ResultEntityIds {
	return ResultEntityIds{}
}

type dataChunkSchema struct {
	FieldName string
	DataType schema.DataType
	Dim int
}

func insert(insertMessages []*schema.InsertMsg) schema.Status {
	var collectionName = insertMessages[0].CollectionName
	var partitionTag = insertMessages[0].PartitionTag
	var clientId = insertMessages[0].ClientId

	// TODO: prevent Memory copy
	var entityIds []int64
	var timestamps []int64
	var vectorRecords [][]*schema.FieldValue
	for _, msg := range insertMessages {
		entityIds = append(entityIds, msg.EntityId)
		timestamps = append(timestamps, msg.Timestamp)
		vectorRecords = append(vectorRecords, msg.Fields)
	}

	var result = milvusInsertMock(collectionName, partitionTag, &entityIds, &timestamps, vectorRecords)
	return publishResult(&result, clientId)
}

func delete(deleteMessages []*schema.DeleteMsg) schema.Status {
	var collectionName = deleteMessages[0].CollectionName
	var clientId = deleteMessages[0].ClientId

	// TODO: prevent Memory copy
	var entityIds []int64
	var timestamps []int64
	for _, msg := range deleteMessages {
		entityIds = append(entityIds, msg.EntityId)
		timestamps = append(timestamps, msg.Timestamp)
	}

	var result = milvusDeleteMock(collectionName, &entityIds, &timestamps)
	return publishResult(&result, clientId)
}

func search(searchMessages []*schema.SearchMsg) schema.Status {
	var collectionName = searchMessages[0].CollectionName
	var clientId int64 = searchMessages[0].ClientId
	var queryString = searchMessages[0].VectorParam.Json

	// TODO: prevent Memory copy
	var records []schema.VectorRecord
	var timestamps []int64
	for _, msg := range searchMessages {
		records = append(records, *msg.VectorParam.RowRecord)
		timestamps = append(timestamps, msg.Timestamp)
	}

	var result = milvusSearchMock(collectionName, queryString, &timestamps, &records)
	return publishResult(&result, clientId)
}
