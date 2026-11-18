package loadmgr

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

// AdvanceMinimumDataVersion publishes a data visibility requirement without
// changing load intent. Repeated or older requirements reuse the current config.
// An unloaded collection stays unloaded; its next Load uses committed DataViews.
func (s *LoadConfigStore) AdvanceMinimumDataVersion(ctx context.Context, collectionID int64, minimum qviews.DataVersion) error {
	s.collectionLocks.Lock(collectionID)
	defer s.collectionLocks.Unlock(collectionID)
	if err := ctx.Err(); err != nil {
		return err
	}
	cfg := s.GetConfig(collectionID)
	if cfg == nil || cfg.MinimumDataVersion.GTE(minimum) {
		return nil
	}
	cfg = cfg.Clone()
	cfg.MinimumDataVersion = minimum
	return s.putLocked(ctx, cfg)
}
