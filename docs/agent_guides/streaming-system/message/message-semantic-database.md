# Database Messages

Messages managing database lifecycle. All are CChannel-only broadcasts serialized via ExclusiveDBName.

All broadcast messages implicitly carry **SharedCluster** via the Broadcaster.

| Message | Dispatch | ExclusiveRequired | ResourceKey |
|---------|----------|-------------------|-------------|
| CreateDatabase | Broadcast: CChannel | No | ExclusiveDBName |
| AlterDatabase | Broadcast: CChannel | No | ExclusiveDBName |
| DropDatabase | Broadcast: CChannel | No | ExclusiveDBName |

- **CreateDatabase**: Creates a new database.
- **AlterDatabase**: Alters database properties (may trigger load config updates for all collections in the DB).
- **DropDatabase**: Drops a database. The database must be empty (no collections).

## Key Invariants

- CreateDatabase must precede CreateCollection in this database.
- DropDatabase must come after all collections in the database are dropped.
- ExclusiveDBName conflicts with SharedDBName (used by collection DDL), preventing concurrent collection operations during database DDL.

```
CreateDatabase@tt=1 → CreateCollection(in_db)@tt=5 → ... → DropCollection@tt=15 → DropDatabase@tt=20
```
