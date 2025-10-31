package model

import (
	pb "github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type FileResource struct {
	ID   int64
	Name string
	Path string
}

func (resource *FileResource) Marshal() *pb.FileResourceInfo {
	if resource == nil {
		return nil
	}

	return &pb.FileResourceInfo{
		ResourceId: resource.ID,
		Name:       resource.Name,
		Path:       resource.Path,
	}
}

func UnmarshalFileResourceInfo(resource *pb.FileResourceInfo) *FileResource {
	if resource == nil {
		return nil
	}

	return &FileResource{
		ID:   resource.ResourceId,
		Name: resource.Name,
		Path: resource.Path,
	}
}
