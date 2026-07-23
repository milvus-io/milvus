# 行级 TTL（Entity-level TTL）

行级 TTL 允许同一个 Collection 中的不同 Entity 使用不同的过期时间。它适用于日志、事件、IoT、MLOps 以及多租户等数据生命周期不一致的场景。

本文描述 Milvus 3.0 代码线中的行级 TTL 行为。行级 TTL 也称为 Entity-level TTL。

## 工作原理

行级 TTL 使用一个 `TIMESTAMPTZ` 字段保存每条 Entity 的绝对过期时间，并通过 Collection 属性 `ttl_field` 指定该字段。

```text
Collection property: ttl_field = "expire_at"
Entity field:        expire_at = "2026-08-01T12:00:00Z"
```

Milvus 在 Query、Search 和 Query Iterator 中自动应用以下可见性条件：

```text
expire_at IS NULL OR expire_at > read_time
```

这意味着：

- `expire_at` 晚于当前读请求的物理时间时，Entity 可见。
- `expire_at` 等于或早于当前读请求的物理时间时，Entity 已过期且不可见。
- `expire_at` 为 `NULL` 时，Entity 永不过期。
- 过期首先影响查询可见性，底层数据文件由 Compaction 异步回收。

不要使用 `0`、`-1` 或其他特殊时间值表示“永不过期”，应明确写入 `NULL`。

## 行级 TTL 与 Collection TTL

| 对比项 | 行级 TTL | Collection TTL |
| --- | --- | --- |
| 配置方式 | `ttl_field` | `collection.ttl.seconds` |
| 时间语义 | 每条 Entity 的绝对过期时间 | 从写入时间开始计算的统一保留时长 |
| 数据类型 | `TIMESTAMPTZ` | 秒数 |
| 不过期数据 | TTL 字段写入 `NULL` | 不适用 |
| 同一 Collection 的差异化生命周期 | 支持 | 不支持 |

两种 TTL 互斥。一个 Collection 不能同时设置 `ttl_field` 和 `collection.ttl.seconds`。

## 前提条件

使用行级 TTL 时需要满足以下条件：

- Collection 中存在一个 `TIMESTAMPTZ` 字段。
- `ttl_field` 必须指向该 Collection 中真实存在的 `TIMESTAMPTZ` 字段。
- 每个 Collection 最多只能激活一个 TTL 字段。
- 如果需要支持永不过期的 Entity，应将 TTL 字段设置为 `nullable=True`。
- TTL 字段保存绝对时间，不支持为每条 Entity 直接写入“10 分钟”之类的相对时长。

## 创建使用行级 TTL 的 Collection

以下示例创建一个名为 `events_with_ttl` 的 Collection，并将 `expire_at` 设置为 TTL 字段。

```python
from pymilvus import DataType, MilvusClient

client = MilvusClient(uri="http://localhost:19530")
collection_name = "events_with_ttl"

schema = client.create_schema(
    auto_id=False,
    enable_dynamic_field=False,
)
schema.add_field(
    field_name="id",
    datatype=DataType.INT64,
    is_primary=True,
)
schema.add_field(
    field_name="expire_at",
    datatype=DataType.TIMESTAMPTZ,
    nullable=True,
)
schema.add_field(
    field_name="vector",
    datatype=DataType.FLOAT_VECTOR,
    dim=4,
)

client.create_collection(
    collection_name=collection_name,
    schema=schema,
    consistency_level="Strong",
    properties={
        "ttl_field": "expire_at",
        "timezone": "UTC",
    },
)

index_params = client.prepare_index_params()
index_params.add_index(
    field_name="vector",
    index_type="AUTOINDEX",
    metric_type="COSINE",
)
client.create_index(
    collection_name=collection_name,
    index_params=index_params,
)
client.load_collection(collection_name=collection_name)
```

`ttl_field` 的值是字段名，而不是 Field ID。

## 写入不同过期时间的数据

PyMilvus 使用 ISO 8601 字符串写入 `TIMESTAMPTZ` 字段。推荐始终携带 `Z` 或明确的 UTC offset。

```python
from datetime import datetime, timedelta, timezone

now = datetime.now(timezone.utc)
expire_in_5_minutes = (now + timedelta(minutes=5)).isoformat()
expire_in_1_day = (now + timedelta(days=1)).isoformat()

rows = [
    {
        "id": 1,
        "expire_at": expire_in_5_minutes,
        "vector": [0.1, 0.2, 0.3, 0.4],
    },
    {
        "id": 2,
        "expire_at": expire_in_1_day,
        "vector": [0.2, 0.3, 0.4, 0.5],
    },
    {
        "id": 3,
        "expire_at": None,
        "vector": [0.3, 0.4, 0.5, 0.6],
    },
]

client.insert(
    collection_name=collection_name,
    data=rows,
)
```

在这个示例中：

- Entity `1` 在 5 分钟后过期。
- Entity `2` 在 1 天后过期。
- Entity `3` 永不过期。

已经过期的时间也可以写入，但该 Entity 在写入可见后会立即被 Query 和 Search 过滤。

## 查询数据

用户不需要在 Filter 中手动添加 TTL 条件。Milvus 会自动过滤已过期 Entity。

```python
results = client.query(
    collection_name=collection_name,
    filter="",
    output_fields=["id", "expire_at"],
    consistency_level="Strong",
)
```

带业务 Filter 时，TTL 条件同样自动生效：

```python
results = client.query(
    collection_name=collection_name,
    filter="id >= 1",
    output_fields=["id", "expire_at"],
    consistency_level="Strong",
)
```

## 搜索数据

无论是否提供业务 Filter，Search 都不会返回已过期 Entity。

```python
results = client.search(
    collection_name=collection_name,
    data=[[0.1, 0.2, 0.3, 0.4]],
    anns_field="vector",
    search_params={"metric_type": "COSINE"},
    limit=10,
    output_fields=["id", "expire_at"],
    consistency_level="Strong",
)
```

Query Iterator 也使用相同的 TTL 可见性规则。

## 修改 Entity 的过期时间

使用 Upsert 写入新的 `expire_at` 值，可以延长或缩短 Entity 的生命周期。

```python
new_expire_at = (
    datetime.now(timezone.utc) + timedelta(days=7)
).isoformat()

client.upsert(
    collection_name=collection_name,
    data=[
        {
            "id": 1,
            "expire_at": new_expire_at,
            "vector": [0.4, 0.3, 0.2, 0.1],
        }
    ],
)
```

对于 Nullable TTL 字段，将其更新为 `NULL` 可以取消该 Entity 的过期时间：

```python
client.upsert(
    collection_name=collection_name,
    data=[
        {
            "id": 1,
            "expire_at": None,
            "vector": [0.4, 0.3, 0.2, 0.1],
        }
    ],
)
```

使用 `partial_update=True` 且不包含 TTL 字段时，原有 TTL 值会被保留。

## 为已有 Collection 启用行级 TTL

如果已有 Collection 使用了 Collection TTL，需要先删除 `collection.ttl.seconds`：

```python
client.drop_collection_properties(
    collection_name=collection_name,
    property_keys=["collection.ttl.seconds"],
)
```

然后添加一个 Nullable `TIMESTAMPTZ` 字段：

```python
client.add_collection_field(
    collection_name=collection_name,
    field_name="expire_at",
    data_type=DataType.TIMESTAMPTZ,
    nullable=True,
)
```

最后将新字段绑定为 TTL 字段：

```python
client.alter_collection_properties(
    collection_name=collection_name,
    properties={
        "ttl_field": "expire_at",
        "timezone": "UTC",
    },
)
```

已有数据在新字段中没有值，因此其 `expire_at` 为 `NULL`，默认永不过期。如果已有数据也需要过期，应使用 Upsert 回填 `expire_at`。

## 切换或禁用 TTL 字段

如果 Collection 中存在另一个 `TIMESTAMPTZ` 字段，可以直接修改 `ttl_field`：

```python
client.alter_collection_properties(
    collection_name=collection_name,
    properties={"ttl_field": "archive_expire_at"},
)
```

修改生效后，Milvus 使用新字段判断所有 Entity 的过期状态。

要禁用行级 TTL，删除 `ttl_field` 属性：

```python
client.drop_collection_properties(
    collection_name=collection_name,
    property_keys=["ttl_field"],
)
```

删除属性不会删除原 TTL 字段，该字段会变为普通的 `TIMESTAMPTZ` 字段。

处于激活状态的 TTL 字段不能直接从 Schema 中删除。必须先删除或切换 `ttl_field` 属性，再删除字段。

## 时区处理

TTL 字段最终以 UTC Unix microseconds 保存。以下两个值表示同一个绝对时间：

```text
2026-08-01T12:00:00Z
2026-08-01T20:00:00+08:00
```

建议：

- 优先写入带 `Z` 或 UTC offset 的 ISO 8601 时间。
- 如果写入不带时区的时间，使用 Collection 的 `timezone` 属性明确解释方式。
- 修改 Collection 或 Database 的默认时区不会改变已经写入的绝对过期时间。

## 逻辑过期与物理回收

到达 `expire_at` 后，Entity 会先从 Query、Search 和 Query Iterator 结果中消失，但对象存储中的数据不会立即删除。

Milvus 在 Sort Compaction 和 Mix Compaction 中：

1. 过滤已过期 Entity。
2. 统计 Segment 中 TTL 的过期分位点。
3. 在过期数据达到 Compaction 阈值后触发后台回收。

因此：

- 不要使用存储空间是否立即下降来判断 TTL 是否生效。
- 自动 Compaction 的执行时间受 Compaction 配置、任务队列和系统负载影响。
- 可以手动触发 Compaction，加快物理回收。

```python
compaction_id = client.compact(collection_name=collection_name)
print(f"Compaction job: {compaction_id}")
```

即使 Compaction 尚未完成，已过期 Entity 也不应出现在查询结果中。

## 限制与注意事项

- 每个 Collection 只能激活一个 TTL 字段。
- TTL 字段必须是 `TIMESTAMPTZ` 类型。
- 行级 TTL 与 `collection.ttl.seconds` 互斥。
- 行级 TTL 只接受绝对过期时间，不接受每行相对时长。
- `NULL` 是唯一推荐的“永不过期”表示方式。
- TTL 过滤是系统自动可见性规则，不能通过普通 Query 或 Search Filter 绕过。
- TTL 字段可以创建标量索引，但 TTL 功能本身不要求用户为该字段创建索引。
- 物理回收依赖 Compaction，不能保证在过期时刻立即释放存储空间。

## 常见问题

### 为什么过期后 `row_count` 没有立即下降？

TTL 到期会立即影响查询可见性，但 `row_count` 和存储占用可能要等 Compaction 完成后才更新。

### 能否让部分 Entity 永不过期？

可以。将 TTL 字段定义为 Nullable，并为这些 Entity 写入 `NULL`。

### 能否同时使用行级 TTL 和 Collection TTL？

不能。切换机制时需要先删除当前 TTL 属性，再设置另一种 TTL。

### Upsert 是否会刷新 TTL？

只有写入新的 TTL 字段值时，行级 TTL 才会改变。Partial Upsert 如果省略 TTL 字段，会保留原来的绝对过期时间。

### 是否需要在 Query 或 Search 中添加 TTL Filter？

不需要。Milvus 会自动将 TTL 条件与用户 Filter 合并；无用户 Filter 的 Search 也会过滤过期 Entity。
