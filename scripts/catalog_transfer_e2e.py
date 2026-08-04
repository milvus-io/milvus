#!/usr/bin/env python3

import argparse
import os
import sys
import time


def require_pymilvus():
    try:
        from pymilvus import Collection, CollectionSchema, DataType, FieldSchema, connections, db, utility
    except Exception as exc:
        raise RuntimeError(
            "pymilvus is required for the catalog transfer E2E client checks. "
            "Install it in this Python environment before running the demo."
        ) from exc
    return Collection, CollectionSchema, DataType, FieldSchema, connections, db, utility


def connect(alias, uri):
    _, _, _, _, connections, _, _ = require_pymilvus()
    connections.connect(alias=alias, uri=uri)


def wait(args):
    _, _, _, _, connections, _, utility = require_pymilvus()
    deadline = time.time() + args.timeout
    last_error = None
    while time.time() < deadline:
        try:
            connections.connect(alias=args.alias, uri=args.uri)
            utility.list_collections(using=args.alias)
            print(f"{args.alias} ready at {args.uri}")
            return 0
        except Exception as exc:
            last_error = exc
            time.sleep(1)
    print(f"Timed out waiting for {args.uri}: {last_error}", file=sys.stderr)
    return 1


def seed(args):
    Collection, CollectionSchema, DataType, FieldSchema, connections, db, utility = require_pymilvus()
    connections.connect(alias="src", uri=args.source_uri)
    connections.connect(alias="dst", uri=args.target_uri)
    ensure_database(db, "src", args.db_name)
    if not args.skip_target_db:
        ensure_database(db, "dst", args.db_name)
    db.using_database(args.db_name, using="src")
    if not args.skip_target_db:
        db.using_database(args.db_name, using="dst")

    cleanup_aliases = ["src"]
    if not args.skip_target_db:
        cleanup_aliases.append("dst")
    for alias in cleanup_aliases:
        try:
            utility.drop_alias(args.alias_name, using=alias)
        except Exception:
            pass
        try:
            if utility.has_collection(args.collection, using=alias):
                utility.drop_collection(args.collection, using=alias)
        except Exception:
            pass

    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=False),
        FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=args.dim),
    ]
    schema = CollectionSchema(fields, description="catalog transfer demo")
    collection = Collection(
        name=args.collection,
        schema=schema,
        using="src",
        shards_num=1,
        consistency_level="Strong",
    )
    collection.create_partition(args.partition)
    collection.create_index("vector", {"index_type": "FLAT", "metric_type": "L2", "params": {}})
    utility.create_alias(args.collection, args.alias_name, using="src")
    insert_rows(collection, 0, args.rows, args.dim, args.partition)
    collection.flush()
    Collection(name=args.collection, using="src").schema
    print(f"seeded source db={args.db_name} collection={args.collection} alias={args.alias_name} rows={args.rows}")
    return 0


def ensure_database(db, alias, db_name):
    if db_name not in db.list_database(using=alias):
        db.create_database(db_name, using=alias)


def insert_rows(collection, start, count, dim, partition):
    ids = list(range(start, start + count))
    vectors = [[float((i + j) % 17) for j in range(dim)] for i in ids]
    collection.insert([ids, vectors], partition_name=partition)


def writer(args):
    Collection, _, _, _, connections, _, _ = require_pymilvus()
    connections.connect(alias="src-writer", uri=args.source_uri, db_name=args.db_name)
    with open(args.ready_file, "w", encoding="utf-8") as ready:
        ready.write("ready\n")
    count = 0
    first_error = None
    while os.path.exists(args.run_file):
        try:
            collection = Collection(name=args.collection, using="src-writer")
            insert_rows(collection, args.start_id + count, args.batch_rows, args.dim, args.partition)
            count += args.batch_rows
        except Exception as exc:
            first_error = exc
            break
        time.sleep(args.interval)
    if first_error is not None:
        with open(args.error_file, "w", encoding="utf-8") as errf:
            errf.write(str(first_error))
        print(f"writer stopped after source gate/cache rejection: {first_error}")
        return 0
    message = f"writer stopped before observing source gate/cache rejection, inserted_rows={count}"
    with open(args.error_file, "w", encoding="utf-8") as errf:
        errf.write(message)
    print(message)
    return 1


def expect_failure(label, fn):
    try:
        fn()
    except Exception as exc:
        print(f"{label} failed as expected: {exc}")
        return
    raise AssertionError(f"{label} unexpectedly succeeded")


def verify(args):
    Collection, _, _, _, connections, _, utility = require_pymilvus()
    connections.connect(alias="src-verify", uri=args.source_uri, db_name=args.db_name)
    connections.connect(alias="dst-verify", uri=args.target_uri, db_name=args.db_name)

    expect_failure(
        "source describe transferred collection",
        lambda: Collection(name=args.collection, using="src-verify").schema,
    )
    expect_failure(
        "source describe transferred alias",
        lambda: Collection(name=args.alias_name, using="src-verify").schema,
    )
    expect_failure(
        "source insert transferred collection",
        lambda: insert_rows(Collection(name=args.collection, using="src-verify"), args.start_id, 1, args.dim, args.partition),
    )

    Collection(name=args.collection, using="dst-verify").schema
    Collection(name=args.alias_name, using="dst-verify").schema
    print(f"target describe and alias cache are visible for {args.collection}")
    return 0


def main():
    parser = argparse.ArgumentParser(description="Milvus Catalog Service collection transfer E2E helper")
    sub = parser.add_subparsers(dest="command", required=True)

    wait_parser = sub.add_parser("wait")
    wait_parser.add_argument("--uri", required=True)
    wait_parser.add_argument("--alias", default="wait")
    wait_parser.add_argument("--timeout", type=int, default=180)
    wait_parser.set_defaults(func=wait)

    def add_common(p):
        p.add_argument("--source-uri", required=True)
        p.add_argument("--target-uri", required=True)
        p.add_argument("--db-name", default="default")
        p.add_argument("--collection", required=True)
        p.add_argument("--alias-name", required=True)
        p.add_argument("--partition", default="p_transfer")
        p.add_argument("--dim", type=int, default=4)
        p.add_argument("--rows", type=int, default=20)
        p.add_argument("--start-id", type=int, default=100000)

    seed_parser = sub.add_parser("seed")
    add_common(seed_parser)
    seed_parser.add_argument("--skip-target-db", action="store_true")
    seed_parser.set_defaults(func=seed)

    writer_parser = sub.add_parser("writer")
    add_common(writer_parser)
    writer_parser.add_argument("--run-file", required=True)
    writer_parser.add_argument("--ready-file", required=True)
    writer_parser.add_argument("--error-file", required=True)
    writer_parser.add_argument("--batch-rows", type=int, default=2)
    writer_parser.add_argument("--interval", type=float, default=0.2)
    writer_parser.set_defaults(func=writer)

    verify_parser = sub.add_parser("verify")
    add_common(verify_parser)
    verify_parser.set_defaults(func=verify)

    args = parser.parse_args()
    try:
        return args.func(args)
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
