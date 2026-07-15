package snview

import (
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func reportUnrecoverable(views []handler.ApplyView) {
	for _, view := range views {
		if view.OnReport == nil {
			continue
		}
		pb := view.View.IntoProto()
		pb.Meta.State = viewpb.QueryViewState(qviews.QueryViewStateUnrecoverable)
		view.OnReport(qviews.NewQueryViewAtWorkNodeFromProto(pb))
	}
}
