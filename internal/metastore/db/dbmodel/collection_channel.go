package dbmodel

import (
	"time"

	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type CollectionChannel struct {
	ID                  int64              `gorm:"id"`
	TenantID            string             `gorm:"tenant_id"`
	CollectionID        int64              `gorm:"collection_id"`
	VirtualChannelName  string             `gorm:"virtual_channel_name"`
	PhysicalChannelName string             `gorm:"physical_channel_name"`
	Removed             bool               `gorm:"removed"`
	Ts                  typeutil.Timestamp `gorm:"ts"`
	IsDeleted           bool               `gorm:"is_deleted"`
	CreatedAt           time.Time          `gorm:"created_at"`
	UpdatedAt           time.Time          `gorm:"updated_at"`
}

func (v CollectionChannel) TableName() string {
	return "collection_channels"
}

//go:generate mockery --name=ICollChannelDb
type ICollChannelDb interface {
	GetByCollectionID(tenantID string, collectionID typeutil.UniqueID, ts typeutil.Timestamp) ([]*CollectionChannel, error)
	Insert(in []*CollectionChannel) error
}

func ExtractChannelNames(channels []*CollectionChannel) ([]string, []string) {
	vchans := make([]string, 0, len(channels))
	pchans := make([]string, 0, len(channels))
	for _, ch := range channels {
		vchans = append(vchans, ch.VirtualChannelName)
		pchans = append(pchans, ch.PhysicalChannelName)
	}
	return vchans, pchans
}
