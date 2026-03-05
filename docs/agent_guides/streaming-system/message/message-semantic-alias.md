# Alias Messages

Messages managing collection aliases. All are CChannel-only broadcasts serialized at the database level.

All broadcast messages implicitly carry **SharedCluster** via the Broadcaster.

| Message | Dispatch | ExclusiveRequired | ResourceKey |
|---------|----------|-------------------|-------------|
| AlterAlias | Broadcast: CChannel | No | ExclusiveDBName |
| DropAlias | Broadcast: CChannel | No | ExclusiveDBName |

- **AlterAlias**: Creates or alters an alias to point to a collection.
- **DropAlias**: Drops an alias.

## Key Invariants

- All alias operations are serialized via ExclusiveDBName.
- ExclusiveDBName conflicts with SharedDBName (used by collection-level DDL), so alias operations block all collection DDL in the same database.
- CreateAlias uses the AlterAlias message type internally.
