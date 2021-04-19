package datanode

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newReplica() collectionReplica {
	collections := make([]*Collection, 0)
	segments := make([]*Segment, 0)

	var replica collectionReplica = &collectionReplicaImpl{
		collections: collections,
		segments:    segments,
	}
	return replica
}

func initTestReplicaMeta(t *testing.T, replica collectionReplica, collectionName string, collectionID UniqueID, segmentID UniqueID) {
	Factory := &MetaFactory{}
	collectionMeta := Factory.CollectionMetaFactory(collectionID, collectionName)

	var err = replica.addCollection(collectionMeta.ID, collectionMeta.Schema)
	require.NoError(t, err)

	collection, err := replica.getCollectionByName(collectionName)
	require.NoError(t, err)
	assert.Equal(t, collection.Name(), collectionName)
	assert.Equal(t, collection.ID(), collectionID)
	assert.Equal(t, replica.getCollectionNum(), 1)

}

//----------------------------------------------------------------------------------------------------- collection
func TestCollectionReplica_getCollectionNum(t *testing.T) {
	replica := newReplica()
	initTestReplicaMeta(t, replica, "collection0", 0, 0)
	assert.Equal(t, replica.getCollectionNum(), 1)
}

func TestCollectionReplica_addCollection(t *testing.T) {
	replica := newReplica()
	initTestReplicaMeta(t, replica, "collection0", 0, 0)
}

func TestCollectionReplica_removeCollection(t *testing.T) {
	replica := newReplica()
	initTestReplicaMeta(t, replica, "collection0", 0, 0)
	assert.Equal(t, replica.getCollectionNum(), 1)

	err := replica.removeCollection(0)
	assert.NoError(t, err)
	assert.Equal(t, replica.getCollectionNum(), 0)
}

func TestCollectionReplica_getCollectionByID(t *testing.T) {
	replica := newReplica()
	collectionName := "collection0"
	collectionID := UniqueID(0)
	initTestReplicaMeta(t, replica, collectionName, collectionID, 0)
	targetCollection, err := replica.getCollectionByID(collectionID)
	assert.NoError(t, err)
	assert.NotNil(t, targetCollection)
	assert.Equal(t, targetCollection.Name(), collectionName)
	assert.Equal(t, targetCollection.ID(), collectionID)
}

func TestCollectionReplica_getCollectionByName(t *testing.T) {
	replica := newReplica()
	collectionName := "collection0"
	collectionID := UniqueID(0)
	initTestReplicaMeta(t, replica, collectionName, collectionID, 0)

	targetCollection, err := replica.getCollectionByName(collectionName)
	assert.NoError(t, err)
	assert.NotNil(t, targetCollection)
	assert.Equal(t, targetCollection.Name(), collectionName)
	assert.Equal(t, targetCollection.ID(), collectionID)

}

func TestCollectionReplica_hasCollection(t *testing.T) {
	replica := newReplica()
	collectionName := "collection0"
	collectionID := UniqueID(0)
	initTestReplicaMeta(t, replica, collectionName, collectionID, 0)

	hasCollection := replica.hasCollection(collectionID)
	assert.Equal(t, hasCollection, true)
	hasCollection = replica.hasCollection(UniqueID(1))
	assert.Equal(t, hasCollection, false)

}

func TestCollectionReplica_freeAll(t *testing.T) {
	replica := newReplica()
	collectionName := "collection0"
	collectionID := UniqueID(0)
	initTestReplicaMeta(t, replica, collectionName, collectionID, 0)

}
