# MEP: Default Value

Current state: Under Discussion

ISSUE: [[Feature]: Support Default Value #23337](https://github.com/milvus-io/milvus/issues/23337)

Keywords: Default, Insert, Upsert 

Released: v2.3.1

## Summary

Support Default Value when input data.

## Motivation

For now, Milvus don't support Default function. If the user pass in the same data under a certain field schema, the data can only be passed in repeatedly, which is not so flexible and user-friendly。

We need a way to support Default function, which is more efficient.

## Public Interfaces

Add new field `default_value` in `FieldSchema`
```proto
message FieldSchema {
  ...
  ScalarField default_value = 11; // default_value only support scalars for now
}
```

## Design Details

1. Add the default_value in the field schema as an optional field.

```proto

message FieldSchema {
  ...
  ScalarField default_value = 11; // default_value only support scalars for now
}
```

2. Will use the default_value if no data pass(the field get nil when insert and upsert).

```proto

message FieldData {
  ...
  oneof field {
    ScalarField scalars = 3;
    VectorField vectors = 4;
  }
}
```

```python
    # create collection
    nb = 3000
    fields = [
        FieldSchema(name="int64", dtype=DataType.INT64, is_primary=True),
         # restrict at most one value to be passed in as the default value
        FieldSchema(name="float", dtype=DataType.FLOAT, default_value=1.0)
    ]
    schema = CollectionSchema(
        fields=fields, description="collection")

    collection = Collection(name="hello_milvus", schema=default_schema)

    #  insert data
    collection.insert(
        [
            [i for i in range(nb)],
            # will use the default_value
            [],
        ]
    )
```
## Compatibility, Deprecation, and Migration Plan

|               Test Cases                   |          ExpectedBehavior                 |
|:-----------------------------------------: | :---------------------------------------: |
|           schema built in 2.2.x            |  can be used normally in the new version  |
 
## Test Plan

### Unit Tests

- Test for using default value in proxy


### E2E Tests
|                 Test Cases                   |         Expected Behavior                |
| :------------------------------------------: | :--------------------------------------: |
|         set illegal default value            |              report error                |
|          set legal default value             |     use default value as fields data     |
|           schema built in 2.2.x              |  can be used normally in the new version |
|          don't set default value             |                the same                  |

## Rejected Alternatives

Default value is set by column, and the writing method of [1,2,3, {default}, {default}, 4, 5] is not supported.

## References

None