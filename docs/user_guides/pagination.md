# Illustration
Support offset and limit keywords to avoid returning all query results in a single rpc, currently the offset + limit is limited to 65536.

## Query Pagination
The following example gets 10 results:
```python
result1 = hello_milvus.query(expr="pk>100", limit=10, output_fields=["pk"])
```

The following example gets 5 results from the 5th:
```python
result2 = hello_milvus.query(expr="pk>100", offset=5, limit=5, output_fields=["pk"])
```

And the results satisfy:
```python
result2 == result1[5:] 
```

The above examples use Python SDK . If you want to know more details, please refer to hello_milvus.py in PyMilvus
repository.
