package writenode

import (
	"errors"
	"strconv"
)

type ddBuffer struct {
	collectionBuffer map[UniqueID]interface{}
	partitionBuffer  map[UniqueID]interface{}
}

func (d *ddBuffer) addCollection(collectionID UniqueID) error {
	if _, ok := d.collectionBuffer[collectionID]; !ok {
		return errors.New("collection " + strconv.FormatInt(collectionID, 10) + " is already exists")
	}

	d.collectionBuffer[collectionID] = nil
	return nil
}

func (d *ddBuffer) removeCollection(collectionID UniqueID) error {
	if _, ok := d.collectionBuffer[collectionID]; !ok {
		return errors.New("cannot found collection " + strconv.FormatInt(collectionID, 10))
	}

	delete(d.collectionBuffer, collectionID)
	return nil
}

func (d *ddBuffer) addPartition(partitionID UniqueID) error {
	if _, ok := d.partitionBuffer[partitionID]; !ok {
		return errors.New("partition " + strconv.FormatInt(partitionID, 10) + " is already exists")
	}

	d.partitionBuffer[partitionID] = nil
	return nil
}

func (d *ddBuffer) removePartition(partitionID UniqueID) error {
	if _, ok := d.partitionBuffer[partitionID]; !ok {
		return errors.New("cannot found partition " + strconv.FormatInt(partitionID, 10))
	}

	delete(d.partitionBuffer, partitionID)
	return nil
}
