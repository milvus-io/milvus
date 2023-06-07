package rootcoord

import (
	"fmt"

	"golang.org/x/exp/maps"

	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type nameDb struct {
	db2Name2ID         map[string]map[string]UniqueID // database -> collection name -> collection id
	totalCollectionNum int
	totalPartitionNum  int
}

func (n *nameDb) exist(dbName string) bool {
	_, ok := n.db2Name2ID[dbName]
	return ok
}

func (n *nameDb) empty(dbName string) bool {
	db, ok := n.db2Name2ID[dbName]
	return ok && len(db) == 0
}

func (n *nameDb) createDbIfNotExist(dbName string) {
	if !n.exist(dbName) {
		n.db2Name2ID[dbName] = make(map[string]UniqueID)
	}
}

func (n *nameDb) dropDb(dbName string) {
	delete(n.db2Name2ID, dbName)
}

func (n *nameDb) insert(dbName string, collectionName string, collectionID UniqueID) {
	n.createDbIfNotExist(dbName)
	n.db2Name2ID[dbName][collectionName] = collectionID
}

func (n *nameDb) get(dbName string, collectionName string) (collectionID UniqueID, ok bool) {
	if !n.exist(dbName) {
		return 0, false
	}
	collectionID, ok = n.db2Name2ID[dbName][collectionName]
	return collectionID, ok
}

func (n *nameDb) listDB() []string {
	dbs := make([]string, 0, len(n.db2Name2ID))
	for db := range n.db2Name2ID {
		dbs = append(dbs, db)
	}
	return dbs
}

func (n *nameDb) listCollectionID(dbName string) ([]typeutil.UniqueID, error) {
	name2ID, ok := n.db2Name2ID[dbName]
	if !ok {
		return nil, fmt.Errorf("database not exist: %s", dbName)
	}
	return maps.Values(name2ID), nil
}

func (n *nameDb) removeIf(selector func(db string, collection string, id UniqueID) bool) {
	type union struct {
		db         string
		collection string
		id         UniqueID
	}

	matches := make([]union, 0, len(n.db2Name2ID))
	for dbName, db := range n.db2Name2ID {
		for collection, id := range db {
			if selector(dbName, collection, id) {
				matches = append(matches, union{
					db:         dbName,
					collection: collection,
					id:         id,
				})
			}
		}
	}

	for _, match := range matches {
		delete(n.db2Name2ID[match.db], match.collection)
	}
}

func (n *nameDb) remove(db, collection string) {
	if n.exist(db) {
		delete(n.db2Name2ID[db], collection)
	}
}

func (n *nameDb) iterate(exitOnFalse func(db string, collection string, id UniqueID) bool) {
	for dbName, db := range n.db2Name2ID {
		for collection, id := range db {
			if !exitOnFalse(dbName, collection, id) {
				return
			}
		}
	}
}

func newNameDb() *nameDb {
	return &nameDb{
		db2Name2ID: make(map[string]map[string]typeutil.UniqueID),
	}
}
