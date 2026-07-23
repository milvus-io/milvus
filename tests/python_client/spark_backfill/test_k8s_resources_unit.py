from spark_backfill.k8s_resources import (
    build_ephemeral_secret,
    build_support_config_map,
    required_rbac_permissions,
)


def test_support_config_map_contains_remote_scripts():
    body = build_support_config_map(
        "spark-support",
        {
            "contracts.py": "contracts",
            "remote_entrypoint.py": "entrypoint",
            "read_probe.py": "probe",
        },
    )

    assert body["metadata"]["name"] == "spark-support"
    assert body["metadata"]["labels"]["app"] == "spark-milvus-backfill"
    assert body["data"]["remote_entrypoint.py"] == "entrypoint"


def test_ephemeral_secret_uses_string_data_and_omits_empty_static_credentials():
    body = build_ephemeral_secret("spark-secret", access_key="", secret_key="", milvus_token="root:Milvus")

    assert body["type"] == "Opaque"
    assert body["stringData"] == {"milvus-token": "root:Milvus"}


def test_ephemeral_secret_includes_complete_static_credential_pair():
    body = build_ephemeral_secret("spark-secret", access_key="ak", secret_key="sk", milvus_token="")

    assert body["stringData"] == {"s3-access-key": "ak", "s3-secret-key": "sk"}


def test_rbac_permissions_include_secret_mutation_only_for_ephemeral_secret():
    without_secret = required_rbac_permissions(create_secret=False)
    with_secret = required_rbac_permissions(create_secret=True)

    assert ("batch", "jobs", "create") in without_secret
    assert ("", "pods/log", "get") in without_secret
    assert ("", "configmaps", "delete") in without_secret
    assert not any(resource == "secrets" for _, resource, _ in without_secret)
    assert ("", "secrets", "create") in with_secret
    assert ("", "secrets", "delete") in with_secret
