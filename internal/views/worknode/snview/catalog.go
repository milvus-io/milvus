package snview

import "github.com/milvus-io/milvus/pkg/v3/proto/viewpb"

// StreamingNodeCatalog defines the persistence interface for SN query views,
// implemented by the streaming node's catalog layer.
type StreamingNodeCatalog interface {
	// SaveQueryView persists or deletes a query view based on its state.
	// The persistence key is derived internally from view.Meta.
	//
	//   - Meta.State == Up → save/overwrite recovery info.
	//   - Meta.State == Down, Unrecoverable, or Dropped → delete recovery info.
	SaveQueryView(view *viewpb.QueryViewOfShard) error
}
