import time
from contextlib import contextmanager
from dataclasses import dataclass

import pyetcd


def _format_milvus_config_key(key):
    """Match pkg/config.formatKey for keys stored by Milvus in etcd."""
    return key.lower().replace("/", "").replace("_", "").replace(".", "")


@dataclass(frozen=True)
class EtcdConfigValue:
    value: bytes | None
    mod_revision: int | None

    @property
    def exists(self):
        return self.value is not None


class MilvusEtcdConfigController:
    """Safely update one Milvus instance's dynamic-config namespace in etcd."""

    def __init__(
        self,
        host,
        port=2379,
        root_path="by-dev",
        user=None,
        password=None,
        timeout=10,
        client=None,
    ):
        self.host = host
        self.port = int(port)
        self.root_path = root_path.strip("/")
        self._client = client or pyetcd.client(
            host=host,
            port=self.port,
            timeout=timeout,
            user=user or None,
            password=password or None,
        )
        self._owns_client = client is None
        self._owned_revisions = {}

    @property
    def endpoint(self):
        return f"{self.host}:{self.port}"

    def close(self):
        if self._owns_client:
            self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def config_path(self, key):
        prefix = f"{self.root_path}/" if self.root_path else ""
        return f"{prefix}config/{_format_milvus_config_key(key)}"

    def read_config(self, key):
        return self._read_path(self.config_path(key))

    def set_config(self, key, value, *, settle_after_write=False):
        path = self.config_path(key)
        before = self._read_path(path)
        owned_revision = self._owned_revisions.get(path)
        if owned_revision is not None and before.mod_revision != owned_revision:
            raise RuntimeError(
                f"Refusing to overwrite a concurrent Milvus config update: path={path} "
                f"owned_revision={owned_revision} current_revision={before.mod_revision}"
            )
        if before.exists:
            compare = [self._client.transactions.mod(path) == before.mod_revision]
        else:
            compare = [self._client.transactions.version(path) == 0]
        succeeded, responses = self._client.transaction(
            compare=compare,
            success=[self._client.transactions.put(path, str(value))],
            failure=[],
        )
        if not succeeded:
            raise RuntimeError(f"Milvus config changed concurrently before update: {path}")
        # pyetcd retains the PutResponse, whose header identifies this committed write.
        # Record ownership before any independent read can fail or observe a newer write.
        committed_revision = responses[0].response_put.header.revision
        if committed_revision <= 0:
            raise RuntimeError(f"Milvus config transaction returned no commit revision: {path}")
        self._owned_revisions[path] = committed_revision
        after = self._read_path(path)
        expected = str(value).encode()
        if after.value != expected or after.mod_revision != committed_revision:
            raise RuntimeError(
                f"Milvus config update was not visible after commit: path={path} expected={expected!r} actual={after.value!r}"
            )
        if settle_after_write:
            # A read-back proves the etcd write, not adoption by Milvus components.
            time.sleep(10)
        return after

    @contextmanager
    def preserve_config(self, key):
        path = self.config_path(key)
        original = self._read_path(path)
        try:
            yield self
        finally:
            self._restore_path(path, original)

    def _read_path(self, path):
        value, metadata = self._client.get(path)
        if metadata is None:
            return EtcdConfigValue(value=None, mod_revision=None)
        return EtcdConfigValue(value=value, mod_revision=metadata.mod_revision)

    def _restore_path(self, path, original):
        owned_revision = self._owned_revisions.pop(path, None)
        if owned_revision is None:
            return
        current = self._read_path(path)
        if current.mod_revision != owned_revision:
            raise RuntimeError(
                f"Refusing to overwrite a concurrent Milvus config update: path={path} "
                f"owned_revision={owned_revision} current_revision={current.mod_revision}"
            )
        if original.exists:
            operation = self._client.transactions.put(path, original.value)
        else:
            operation = self._client.transactions.delete(path)
        succeeded, _ = self._client.transaction(
            compare=[self._client.transactions.mod(path) == owned_revision],
            success=[operation],
            failure=[],
        )
        if not succeeded:
            raise RuntimeError(f"Milvus config changed concurrently before restore: {path}")
        restored = self._read_path(path)
        if restored.value != original.value:
            raise RuntimeError(
                f"Milvus config restore was not visible after commit: path={path} "
                f"expected={original.value!r} actual={restored.value!r}"
            )
