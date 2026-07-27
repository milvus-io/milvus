import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from spark_backfill.k8s_runner import SparkJobRequest
from spark_backfill.toolbox_runner import (
    ToolboxRuntimeConfig,
    ToolboxSparkRunner,
    stream_pod_exec,
)

EXIT_MARKER = "__SPARK_BACKFILL_TOOLBOX_EXIT_CODE__="


def _pod(
    name="spark-toolbox-pod", *, phase="Running", ready=True, container="spark-toolbox"
):
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name),
        status=SimpleNamespace(
            phase=phase,
            container_statuses=[SimpleNamespace(name=container, ready=ready)],
        ),
    )


class FakeCoreApi:
    def __init__(self, pods):
        self.pods = list(pods)
        self.read_calls = []
        self.list_calls = []

    def read_namespaced_pod(self, name, namespace):
        self.read_calls.append((namespace, name))
        for pod in self.pods:
            if pod.metadata.name == name:
                return pod
        raise RuntimeError(f"pod not found: {name}")

    def list_namespaced_pod(self, namespace, label_selector):
        self.list_calls.append((namespace, label_selector))
        return SimpleNamespace(items=self.pods)


class FakePodExec:
    def __init__(self, outputs):
        self.outputs = list(outputs)
        self.calls = []

    def __call__(self, core_api, *, pod_name, namespace, container, command):
        self.calls.append(
            {
                "pod_name": pod_name,
                "namespace": namespace,
                "container": container,
                "command": command,
            }
        )
        return self.outputs.pop(0)


def _runtime(tmp_path, **overrides):
    values = {
        "namespace": "default",
        "pod_name": "spark-toolbox-pod",
        "pod_label": "app=spark-milvus-toolbox",
        "container": "spark-toolbox",
        "wrapper_path": "/usr/local/bin/spark-submit-milvus",
        "workspace_path": "/workspace/spark-backfill-pytest",
        "timeout_seconds": 1800,
        "evidence_root": tmp_path,
    }
    values.update(overrides)
    return ToolboxRuntimeConfig(**values)


def _runner(tmp_path, core, pod_exec, **runtime_overrides):
    return ToolboxSparkRunner(
        core,
        _runtime(tmp_path, **runtime_overrides),
        support_files={"contracts.py": "CONTRACTS", "read_probe.py": "READ_PROBE"},
        pod_exec=pod_exec,
    )


def test_stream_pod_exec_uses_kubernetes_exec_subresource(monkeypatch):
    calls = []

    def fake_stream(method, pod_name, namespace, **kwargs):
        calls.append((method, pod_name, namespace, kwargs))
        return "output"

    core = SimpleNamespace(connect_get_namespaced_pod_exec=object())
    monkeypatch.setattr("spark_backfill.toolbox_runner.stream", fake_stream)

    output = stream_pod_exec(
        core,
        pod_name="toolbox",
        namespace="default",
        container="spark-toolbox",
        command="echo ok",
    )

    assert output == "output"
    method, pod_name, namespace, kwargs = calls[0]
    assert method is core.connect_get_namespaced_pod_exec
    assert (pod_name, namespace) == ("toolbox", "default")
    assert kwargs["container"] == "spark-toolbox"
    assert kwargs["command"] == ["bash", "-lc", "echo ok"]
    assert kwargs["stderr"] is True
    assert kwargs["stdout"] is True
    assert kwargs["tty"] is False


def test_runner_executes_backfill_in_explicit_ready_toolbox_pod(tmp_path):
    core = FakeCoreApi([_pod()])
    pod_exec = FakePodExec(
        [
            f"prepared\n{EXIT_MARKER}0\n",
            f"Backfill Summary: SUCCESS\n{EXIT_MARKER}0\n",
        ]
    )
    runner = _runner(tmp_path, core, pod_exec)

    result = runner.run(
        SparkJobRequest(
            case_id="coalesce",
            operation="backfill",
            payload={"arguments": ["--mode", "coalesce", "--batch-size", "1024"]},
        )
    )

    assert result.succeeded is True
    assert result.exit_code == 0
    assert result.pod_name == "spark-toolbox-pod"
    assert result.logs == "Backfill Summary: SUCCESS"
    assert core.read_calls == [("default", "spark-toolbox-pod")]
    assert len(pod_exec.calls) == 2
    command = pod_exec.calls[1]["command"]
    assert "/usr/local/bin/spark-submit-milvus" in command
    assert "com.zilliz.spark.connector.operations.backfill.BackfillApp" in command
    assert "--mode coalesce" in command
    assert '"$S3_ACCESS_KEY"' in command
    assert '"$S3_SECRET_KEY"' in command
    assert "<redacted>" in command
    assert "PIPESTATUS" in command
    assert (
        Path(result.evidence_dir) / "pod.log"
    ).read_text() == "Backfill Summary: SUCCESS"
    evidence = json.loads((Path(result.evidence_dir) / "command.json").read_text())
    assert evidence["runnerMode"] == "toolbox"
    assert evidence["podName"] == "spark-toolbox-pod"


def test_runner_discovers_exactly_one_ready_toolbox_pod_by_label(tmp_path):
    core = FakeCoreApi(
        [
            _pod("old", phase="Pending", ready=False),
            _pod("ready"),
        ]
    )
    pod_exec = FakePodExec([f"prepared\n{EXIT_MARKER}0\n", f"ok\n{EXIT_MARKER}0\n"])
    runner = _runner(tmp_path, core, pod_exec, pod_name="")

    result = runner.run(
        SparkJobRequest(case_id="read", operation="read", payload={"options": {}})
    )

    assert result.pod_name == "ready"
    assert core.list_calls == [("default", "app=spark-milvus-toolbox")]


@pytest.mark.parametrize("pods", [[], [_pod("one"), _pod("two")]])
def test_runner_rejects_missing_or_ambiguous_ready_toolbox_pods(tmp_path, pods):
    runner = _runner(tmp_path, FakeCoreApi(pods), FakePodExec([]), pod_name="")

    with pytest.raises(RuntimeError, match="exactly one Ready Toolbox Pod"):
        runner.run(
            SparkJobRequest(case_id="read", operation="read", payload={"options": {}})
        )


def test_runner_injects_read_probe_and_uses_pod_secret_environment(tmp_path):
    core = FakeCoreApi([_pod()])
    pod_exec = FakePodExec(
        [f"prepared\n{EXIT_MARKER}0\n", f"read result\n{EXIT_MARKER}0\n"]
    )
    runner = _runner(tmp_path, core, pod_exec)

    result = runner.run(
        SparkJobRequest(
            case_id="read",
            operation="read",
            payload={
                "options": {"milvus.uri": "http://milvus:19530"},
                "primaryKey": "id",
            },
        )
    )

    assert result.succeeded is True
    prepare_command = pod_exec.calls[0]["command"]
    assert "contracts.py" in prepare_command
    assert "read_probe.py" in prepare_command
    command = pod_exec.calls[1]["command"]
    assert "SPARK_BACKFILL_READ_SPEC_JSON" in command
    assert '"${MILVUS_TOKEN:-}"' in command
    assert '"${S3_ACCESS_KEY:-}"' in command
    assert '"${S3_SECRET_KEY:-}"' in command
    assert "/workspace/spark-backfill-pytest/read_probe.py" in command
    assert "<redacted>" in command
    assert "PIPESTATUS" in command


def test_runner_records_nonzero_exit_without_deleting_toolbox(tmp_path):
    core = FakeCoreApi([_pod()])
    pod_exec = FakePodExec(
        [f"prepared\n{EXIT_MARKER}0\n", f"duplicate primary key\n{EXIT_MARKER}7\n"]
    )
    runner = _runner(tmp_path, core, pod_exec)

    result = runner.run(
        SparkJobRequest(
            case_id="duplicate-pk", operation="backfill", payload={"arguments": []}
        )
    )

    assert result.succeeded is False
    assert result.exit_code == 7
    assert result.reason == "Error"
    assert result.logs == "duplicate primary key"
    assert (Path(result.evidence_dir) / "result.json").is_file()


def test_runner_raises_timeout_after_saving_evidence(tmp_path):
    core = FakeCoreApi([_pod()])
    pod_exec = FakePodExec(
        [f"prepared\n{EXIT_MARKER}0\n", f"terminated\n{EXIT_MARKER}124\n"]
    )
    runner = _runner(tmp_path, core, pod_exec, timeout_seconds=1)

    with pytest.raises(TimeoutError, match="timed out"):
        runner.run(
            SparkJobRequest(
                case_id="timeout", operation="backfill", payload={"arguments": []}
            )
        )

    evidence_dirs = list(tmp_path.iterdir())
    assert len(evidence_dirs) == 1
    assert (evidence_dirs[0] / "pod.log").read_text() == "terminated"
    saved = json.loads((evidence_dirs[0] / "result.json").read_text())
    assert saved["reason"] == "TimedOut"


def test_redacted_pipeline_hides_pod_secrets_and_preserves_application_exit_code():
    application = 'printf "token=%s key=%s\\n" "$MILVUS_TOKEN" "$S3_ACCESS_KEY"; exit 9'
    command = "\n".join(
        ["set -uo pipefail", *ToolboxSparkRunner._redacted_pipeline(application)]
    )
    environment = dict(
        os.environ,
        MILVUS_TOKEN="root:secret",
        S3_ACCESS_KEY="access-secret",
        S3_SECRET_KEY="",
    )

    completed = subprocess.run(
        ["bash", "-lc", command],
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 9
    assert "root:secret" not in completed.stdout
    assert "access-secret" not in completed.stdout
    assert completed.stdout == "token=<redacted> key=<redacted>\n"
