"""Kubernetes support resources and pre-flight RBAC checks."""

from __future__ import annotations

import base64
from collections.abc import Mapping


def build_support_config_map(name: str, files: Mapping[str, str]) -> dict:
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "labels": {"app": "spark-milvus-backfill"},
        },
        "data": dict(files),
    }


def build_ephemeral_secret(name: str, *, access_key: str, secret_key: str, milvus_token: str) -> dict:
    if bool(access_key) != bool(secret_key):
        raise ValueError("S3 access key and secret key must both be set or both be empty")
    string_data = {}
    if access_key:
        string_data.update({"s3-access-key": access_key, "s3-secret-key": secret_key})
    if milvus_token:
        string_data["milvus-token"] = milvus_token
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": name,
            "labels": {"app": "spark-milvus-backfill"},
        },
        "type": "Opaque",
        "stringData": string_data,
    }


def decode_storage_credentials(secret_data: Mapping[str, str]) -> tuple[str, str]:
    key_pairs = (
        ("s3-access-key", "s3-secret-key"),
        ("accesskey", "secretkey"),
    )
    for access_key_name, secret_key_name in key_pairs:
        access_key = secret_data.get(access_key_name, "")
        secret_key = secret_data.get(secret_key_name, "")
        if not access_key and not secret_key:
            continue
        if not access_key or not secret_key:
            raise ValueError(
                f"Kubernetes Secret must contain both {access_key_name!r} and {secret_key_name!r}"
            )
        try:
            return (
                base64.b64decode(access_key, validate=True).decode("utf-8"),
                base64.b64decode(secret_key, validate=True).decode("utf-8"),
            )
        except (ValueError, UnicodeDecodeError) as exc:
            raise ValueError("Kubernetes Secret contains invalid storage credentials") from exc
    raise ValueError(
        "Kubernetes Secret does not contain a supported storage credential key pair"
    )


def read_storage_credentials(core_api, namespace: str, secret_name: str) -> tuple[str, str]:
    secret = core_api.read_namespaced_secret(secret_name, namespace)
    return decode_storage_credentials(getattr(secret, "data", None) or {})


def required_rbac_permissions(*, create_secret: bool, runner_mode: str = "job") -> list[tuple[str, str, str]]:
    if runner_mode == "toolbox":
        return [
            ("", "pods", "get"),
            ("", "pods", "list"),
            ("", "pods/exec", "get"),
        ]
    permissions = [
        ("batch", "jobs", "create"),
        ("batch", "jobs", "get"),
        ("batch", "jobs", "delete"),
        ("", "pods", "get"),
        ("", "pods", "list"),
        ("", "pods/log", "get"),
        ("", "configmaps", "create"),
        ("", "configmaps", "get"),
        ("", "configmaps", "delete"),
    ]
    if create_secret:
        permissions.extend(
            [
                ("", "secrets", "create"),
                ("", "secrets", "delete"),
            ]
        )
    return permissions


def assert_rbac_permissions(
    authorization_api,
    namespace: str,
    *,
    create_secret: bool,
    runner_mode: str = "job",
) -> None:
    denied = []
    for group, resource, verb in required_rbac_permissions(
        create_secret=create_secret,
        runner_mode=runner_mode,
    ):
        body = {
            "apiVersion": "authorization.k8s.io/v1",
            "kind": "SelfSubjectAccessReview",
            "spec": {
                "resourceAttributes": {
                    "namespace": namespace,
                    "group": group,
                    "resource": resource,
                    "verb": verb,
                }
            },
        }
        response = authorization_api.create_self_subject_access_review(body=body)
        if not getattr(response.status, "allowed", False):
            denied.append(f"{verb} {group or 'core'}/{resource}")
    if denied:
        raise PermissionError("Spark Backfill Kubernetes RBAC denied: " + ", ".join(denied))
