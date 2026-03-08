# Illustration
Collection TTL(Time to Live ) is the expiration time attribute on the collection.  The expired data will not be queried or searched and clean up with the GC mechanism.

# Manipulation
We provide a property of collection level to config TTL: `collection.ttl.seconds`. 
There is a priority for the collection TTL configuration, attempting to get the `collection.ttl.seconds` 
from the collection properties first . If the configuration does not exist within the properties,
then read `common.entityExpiration` from milvus.yaml, the collection TTL will not be set  when the above steps fail.

## Set TTL  for a new collection
The following example is set TTL to 15 seconds:
```python
collection = Collection(name=name,vschema=schema,properties={"collection.ttl.seconds": 15})
```
## Set TTL for an old collection
The following example is modifying TTL to 1800 seconds:
```python
collection.set_properties(properties={"collection.ttl.seconds": 1800})
```

The above examples use Python SDK . If you want to know more details, please refer to example.py.