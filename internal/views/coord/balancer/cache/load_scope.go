package cache

// LoadedSize and ShardLoadedSize describe desired membership, including data
// not placed yet. They never include partitions outside LoadConfig.
func (c *CollectionEntry) LoadedSize() int64 {
	if c.loadsAllPartitions {
		return c.data.LogicalSize
	}
	return c.loadedRows
}

func (c *CollectionEntry) ShardLoadedSize(vchannel string) int64 {
	if c.loadsAllPartitions {
		if shard := c.data.Shard(vchannel); shard != nil {
			return shard.LogicalSize
		}
		return 0
	}
	return c.loadedShardRows[vchannel]
}

// LoadsAllPartitions applies only to this entry's current DataView. Retained
// views may contain other partitions and must still use LoadsPartition.
func (c *CollectionEntry) LoadsAllPartitions() bool { return c.loadsAllPartitions }

func (c *CollectionEntry) LoadsPartition(id int64) bool {
	_, loaded := c.loadedPartitions[id]
	return loaded
}

// refreshLoadRows runs only on config/DataView publication. Summaries share the
// full immutable DataView; neither segment lists nor filtered views are copied.
func (c *CollectionEntry) refreshLoadRows() {
	c.loadedRows = 0
	c.loadsAllPartitions = false
	c.loadedPartitions = nil
	c.loadedShardRows = nil
	if c.config == nil {
		return
	}
	c.loadedPartitions = make(map[int64]struct{}, len(c.config.PartitionIDs))
	for _, id := range c.config.PartitionIDs {
		c.loadedPartitions[id] = struct{}{}
	}
	if c.data == nil {
		return
	}
	if c.coversDataView() {
		c.loadsAllPartitions = true
		return
	}
	c.loadedShardRows = make(map[string]int64, len(c.data.Shards))
	for _, shard := range c.data.Shards {
		var rows int64
		for _, partition := range shard.Partitions {
			if c.LoadsPartition(partition.PartitionID) {
				rows += partition.LogicalSize
			}
		}
		c.loadedShardRows[shard.VChannel] = rows
		c.loadedRows += rows
	}
}

func (c *CollectionEntry) coversDataView() bool {
	for _, shard := range c.data.Shards {
		for _, partition := range shard.Partitions {
			if !c.LoadsPartition(partition.PartitionID) {
				return false
			}
		}
	}
	return true
}
