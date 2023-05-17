from minio import Minio

host = "10.102.9.193:9000"
access_key = "minioadmin"
secret_key = "minioadmin"
secure = False


client = Minio(
    host,
    access_key=access_key,
    secret_key=secret_key,
    secure=secure,
)

buckets = client.list_buckets()

for bucket in buckets:
    print(bucket.name, bucket.creation_date)
    size_cnt = 0
    objects = client.list_objects(bucket.name, prefix="file/insert_log", recursive=True)
    for obj in objects:
        print(obj.bucket_name, obj.object_name, obj.size, obj.last_modified, obj.etag)
        size_cnt += obj.size
    objects = client.list_objects(bucket.name, prefix="file/stats_log", recursive=True)
    for obj in objects:
        print(obj.bucket_name, obj.object_name, obj.size, obj.last_modified, obj.etag)
        size_cnt += obj.size
    objects = client.list_objects(bucket.name, prefix="file/delta_log", recursive=True)
    for obj in objects:
        print(obj.bucket_name, obj.object_name, obj.size, obj.last_modified, obj.etag)
        size_cnt += obj.size
    print(f"all file size for inset, stats, delta log is {size_cnt/1024/1024} MB")


