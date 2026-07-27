"""Spark Backfill runner that reuses a pre-built Toolbox Pod via Kubernetes exec."""

from __future__ import annotations

import base64
import json
import re
import shlex
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from kubernetes.stream import stream

from .contracts import BACKFILL_MAIN_CLASS
from .k8s_runner import KubernetesSparkRunner, SparkJobRequest, SparkJobResult

EXIT_MARKER = "__SPARK_BACKFILL_TOOLBOX_EXIT_CODE__="
CONNECTOR_JAR = "/opt/spark-milvus/jars/spark-connector-assembly.jar"
REQUIRED_NATIVE_LIBRARIES = (
    "/opt/spark-milvus/native/libmilvus-storage.so",
    "/opt/spark-milvus/native/libmilvus-storage-jni.so",
)


@dataclass(frozen=True)
class ToolboxRuntimeConfig:
    namespace: str
    pod_name: str
    pod_label: str
    container: str
    wrapper_path: str
    workspace_path: str
    timeout_seconds: int
    evidence_root: Path


PodExec = Callable[..., str]


def stream_pod_exec(
    core_api, *, pod_name: str, namespace: str, container: str, command: str
) -> str:
    result = stream(
        core_api.connect_get_namespaced_pod_exec,
        pod_name,
        namespace,
        container=container,
        command=["bash", "-lc", command],
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False,
    )
    return str(result or "")


class ToolboxSparkRunner:
    def __init__(
        self,
        core_api,
        runtime: ToolboxRuntimeConfig,
        *,
        support_files: Mapping[str, str],
        pod_exec: PodExec = stream_pod_exec,
    ):
        self.core_api = core_api
        self.runtime = runtime
        self.support_files = dict(support_files)
        self.pod_exec = pod_exec
        self._prepared_pods: set[str] = set()

    def run(self, request: SparkJobRequest) -> SparkJobResult:
        KubernetesSparkRunner._validate_request(request)
        pod_name = self._resolve_pod_name()
        run_name = self._new_run_name(request.case_id)
        evidence_dir = self.runtime.evidence_root / run_name
        evidence_dir.mkdir(parents=True, exist_ok=False)

        command = self._build_operation_command(request)
        self._write_json(
            evidence_dir / "command.json",
            {
                "runnerMode": "toolbox",
                "runName": run_name,
                "podName": pod_name,
                "namespace": self.runtime.namespace,
                "container": self.runtime.container,
                "operation": request.operation,
                "payload": request.payload,
                "command": command,
            },
        )

        if pod_name not in self._prepared_pods:
            prepared_logs, prepared_exit = self._execute_shell(
                pod_name, self._build_prepare_command()
            )
            if prepared_exit != 0:
                result = SparkJobResult(
                    job_name=run_name,
                    pod_name=pod_name,
                    succeeded=False,
                    exit_code=prepared_exit,
                    reason="PreparationFailed",
                    logs=prepared_logs,
                    evidence_dir=str(evidence_dir),
                )
                (evidence_dir / "pod.log").write_text(prepared_logs, encoding="utf-8")
                self._write_json(evidence_dir / "result.json", result.__dict__)
                return result
            self._prepared_pods.add(pod_name)

        logs, exit_code = self._execute_shell(pod_name, command)
        reason = (
            "Completed"
            if exit_code == 0
            else "TimedOut" if exit_code == 124 else "Error"
        )
        result = SparkJobResult(
            job_name=run_name,
            pod_name=pod_name,
            succeeded=exit_code == 0,
            exit_code=exit_code,
            reason=reason,
            logs=logs,
            evidence_dir=str(evidence_dir),
        )
        (evidence_dir / "pod.log").write_text(logs, encoding="utf-8")
        self._write_json(evidence_dir / "result.json", result.__dict__)
        if exit_code == 124:
            raise TimeoutError(
                f"Spark Toolbox execution {run_name} timed out after {self.runtime.timeout_seconds}s"
            )
        return result

    def _resolve_pod_name(self) -> str:
        if self.runtime.pod_name:
            pod = self.core_api.read_namespaced_pod(
                self.runtime.pod_name, self.runtime.namespace
            )
            if not self._is_ready(pod):
                raise RuntimeError(
                    f"Toolbox Pod {self.runtime.pod_name!r} is not Running with ready container "
                    f"{self.runtime.container!r}"
                )
            return self.runtime.pod_name

        pods = self.core_api.list_namespaced_pod(
            namespace=self.runtime.namespace,
            label_selector=self.runtime.pod_label,
        ).items
        ready = [pod for pod in pods if self._is_ready(pod)]
        if len(ready) != 1:
            names = [
                getattr(getattr(pod, "metadata", None), "name", "<unknown>")
                for pod in ready
            ]
            raise RuntimeError(
                f"Expected exactly one Ready Toolbox Pod for label {self.runtime.pod_label!r}; "
                f"found {len(ready)}: {names}"
            )
        return ready[0].metadata.name

    def _is_ready(self, pod) -> bool:
        status = getattr(pod, "status", None)
        if getattr(status, "phase", None) != "Running":
            return False
        for container_status in getattr(status, "container_statuses", None) or []:
            if container_status.name == self.runtime.container:
                return bool(container_status.ready)
        return False

    def _build_prepare_command(self) -> str:
        workspace = self.runtime.workspace_path
        commands = [
            f"test -x {shlex.quote(self.runtime.wrapper_path)}",
            f"test -f {shlex.quote(CONNECTOR_JAR)}",
            *(f"test -f {shlex.quote(path)}" for path in REQUIRED_NATIVE_LIBRARIES),
            f"mkdir -p {shlex.quote(workspace)}",
        ]
        for filename, content in sorted(self.support_files.items()):
            if PurePosixPath(filename).name != filename:
                raise ValueError(
                    f"Toolbox support filename must be a basename: {filename!r}"
                )
            encoded = base64.b64encode(content.encode("utf-8")).decode("ascii")
            destination = f"{workspace}/{filename}"
            commands.append(
                f"printf %s {shlex.quote(encoded)} | base64 -d > {shlex.quote(destination)}"
            )
        return "set -euo pipefail\n" + "\n".join(commands)

    def _build_operation_command(self, request: SparkJobRequest) -> str:
        if request.operation == "backfill":
            arguments = [str(value) for value in request.payload.get("arguments", [])]
            application = shlex.join(
                [
                    self.runtime.wrapper_path,
                    "--class",
                    BACKFILL_MAIN_CLASS,
                    CONNECTOR_JAR,
                    *arguments,
                ]
            )
            return "\n".join(
                [
                    "set -uo pipefail",
                    "storage_args=()",
                    'if [[ -n "${S3_ACCESS_KEY:-}" || -n "${S3_SECRET_KEY:-}" ]]; then',
                    '  if [[ -z "${S3_ACCESS_KEY:-}" || -z "${S3_SECRET_KEY:-}" ]]; then',
                    '    echo "Toolbox S3_ACCESS_KEY and S3_SECRET_KEY must both be present"',
                    "    exit 3",
                    "  fi",
                    '  storage_args=(--s3-access-key "$S3_ACCESS_KEY" --s3-secret-key "$S3_SECRET_KEY")',
                    "else",
                    "  storage_args=(--use-iam)",
                    "fi",
                    *self._redacted_pipeline(f'{application} "${{storage_args[@]}}"'),
                ]
            )

        spec = json.dumps(request.payload, separators=(",", ":"))
        read_probe = f"{self.runtime.workspace_path}/read_probe.py"
        return "\n".join(
            [
                "set -uo pipefail",
                f"export SPARK_BACKFILL_READ_SPEC_JSON={shlex.quote(spec)}",
                'export SPARK_BACKFILL_MILVUS_TOKEN="${MILVUS_TOKEN:-}"',
                'export SPARK_BACKFILL_S3_ACCESS_KEY="${S3_ACCESS_KEY:-}"',
                'export SPARK_BACKFILL_S3_SECRET_KEY="${S3_SECRET_KEY:-}"',
                *self._redacted_pipeline(
                    shlex.join([self.runtime.wrapper_path, read_probe])
                ),
            ]
        )

    @staticmethod
    def _redacted_pipeline(application: str) -> list[str]:
        redactor = "\n".join(
            [
                "import os, sys",
                'secrets = [os.getenv("MILVUS_TOKEN", ""), os.getenv("S3_ACCESS_KEY", ""), os.getenv("S3_SECRET_KEY", "")]',
                "for line in sys.stdin:",
                "    for secret in secrets:",
                "        if secret:",
                '            line = line.replace(secret, "<redacted>")',
                "    sys.stdout.write(line)",
                "    sys.stdout.flush()",
            ]
        )
        return [
            "set +e",
            f"{{ {application}; }} 2>&1 | python3 -c {shlex.quote(redactor)}",
            'rc="${PIPESTATUS[0]}"',
            'exit "$rc"',
        ]

    def _execute_shell(self, pod_name: str, command: str) -> tuple[str, int | None]:
        wrapped = "\n".join(
            [
                "set +e",
                (
                    f"timeout --signal=TERM --kill-after=10s {int(self.runtime.timeout_seconds)}s "
                    f"bash -lc {shlex.quote(command)} 2>&1"
                ),
                "rc=$?",
                f"printf '\\n{EXIT_MARKER}%s\\n' \"$rc\"",
                "exit 0",
            ]
        )
        output = self.pod_exec(
            self.core_api,
            pod_name=pod_name,
            namespace=self.runtime.namespace,
            container=self.runtime.container,
            command=wrapped,
        )
        return self._parse_output(str(output or ""))

    @staticmethod
    def _parse_output(output: str) -> tuple[str, int | None]:
        matches = list(re.finditer(rf"(?m)^{re.escape(EXIT_MARKER)}(\d+)$", output))
        if not matches:
            return output.rstrip(), None
        match = matches[-1]
        logs = (output[: match.start()] + output[match.end() :]).strip()
        return logs, int(match.group(1))

    @staticmethod
    def _new_run_name(case_id: str) -> str:
        label = KubernetesSparkRunner._label(case_id)[:35].strip("-") or "case"
        return f"spark-toolbox-{label}-{uuid.uuid4().hex[:8]}"

    @staticmethod
    def _write_json(path: Path, value: Any) -> None:
        path.write_text(
            json.dumps(value, indent=2, sort_keys=True, default=str), encoding="utf-8"
        )
