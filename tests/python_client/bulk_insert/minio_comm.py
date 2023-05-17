import os
from minio import Minio
from minio.error import S3Error
from utils.util_log import test_log as log


def copy_files_to_bucket(client, r_source, target_files, bucket_name, force=False):
    # check the bucket exist
    found = client.bucket_exists(bucket_name)
    if not found:
        log.error(f"Bucket {bucket_name} not found.")
        return

    # copy target files from root source folder
    os.chdir(r_source)
    for target_file in target_files:
        found = False
        try:
            result = client.stat_object(bucket_name, target_file)
            found = True
        except S3Error as exc:
            pass

        if force or not found:
            res = client.fput_object(bucket_name, target_file, f"{r_source}/{target_file}")
            log.info(f"copied {res.object_name} to minio")
        else:
            log.info(f"skip copy {target_file} to minio")


def copy_files_to_minio(host, r_source, files, bucket_name, access_key="minioadmin", secret_key="minioadmin",
                        secure=False, force=False):
    client = Minio(
        host,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )
    try:
        copy_files_to_bucket(client, r_source=r_source, target_files=files, bucket_name=bucket_name, force=force)
    except S3Error as exc:
        log.error("fail to copy files to minio", exc)


def get_milvus_data_storage_used(host, access_key="minioadmin", secret_key="minioadmin", secure=False):
    # check insert stats delta log file size
    client = Minio(
        host,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )
    buckets = client.list_buckets()
    milvus_bucket = None
    for bucket in buckets:
        if bucket.name == "milvus-bucket":
            milvus_bucket = [bucket]
            break
    assert len(milvus_bucket) == 1
    for bucket in milvus_bucket:
        total_size = 0
        objects = client.list_objects(bucket.name, prefix="file/insert_log", recursive=True)
        for obj in objects:
            log.debug(f"object_name {obj.object_name}, size {obj.size}")
            total_size += obj.size
        objects = client.list_objects(bucket.name, prefix="file/stats_log", recursive=True)
        for obj in objects:
            log.debug(f"object_name {obj.object_name}, size {obj.size}")
            total_size += obj.size
        objects = client.list_objects(bucket.name, prefix="file/delta_log", recursive=True)
        for obj in objects:
            log.debug(f"object_name {obj.object_name}, size {obj.size}")
            total_size += obj.size
        # log.info(f"all file size for inset, stats, delta log is {total_size/1024/1024} MB")
        total_size_in_mb = total_size/1024/1024
        return total_size_in_mb
