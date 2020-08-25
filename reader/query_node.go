package reader

import "C"
import (
	"context"
)

type QueryNodeTimeSync struct {
	deleteTimeSync uint64
	insertTimeSync uint64
	searchTimeSync uint64
}

type QueryNode struct {
	Collections               []*Collection
	queryNodeTimeSync         *QueryNodeTimeSync
}

func NewQueryNode(ctx context.Context, timeSync uint64) *QueryNode {
	ctx = context.Background()
	queryNodeTimeSync := &QueryNodeTimeSync {
		deleteTimeSync: timeSync,
		insertTimeSync: timeSync,
		searchTimeSync: timeSync,
	}

	return &QueryNode{
		Collections:           nil,
		queryNodeTimeSync:     queryNodeTimeSync,
	}
}

func (node *QueryNode) AddNewCollection(collectionName string, schema CollectionSchema) error {
	var collection, err = NewCollection(collectionName, schema)
	node.Collections = append(node.Collections, collection)
	return err
}
