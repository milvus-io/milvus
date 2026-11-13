"""Exercise Paimon selection and isolation through real pytest and shell entry points."""

import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CLIENT = ROOT / "tests/python_client"
ORDINARY = "milvus_client/test_milvus_client_external_table_paimon.py"
SPECIAL = "milvus_client/test_milvus_client_external_table_paimon_l3.py"


def invoke(paths, *arguments, collect=True):
    env = dict(os.environ)
    env.pop("PAIMON_L3_PROFILE", None)
    argv = [sys.executable, "-m", "pytest", *paths, "-o", "addopts=-p no:locust", "-q", *arguments]
    if collect:
        argv.append("--collect-only")
    return subprocess.run(argv, cwd=CLIENT, env=env, text=True, capture_output=True, timeout=90, check=False)


def nodeids(result):
    assert result.returncode == 0, result.stdout + result.stderr
    return [line.strip() for line in result.stdout.splitlines() if line.startswith("milvus_client/") and "::" in line]


def test_tcus_file_filter_includes_ordinary_cases_and_excludes_specialist():
    # Actual TCUS PR runner discovers source files by this CaseLabel expression
    # before pytest-tagging sees individual items. Literal strings bypass it.
    pattern = re.compile(r"CaseLabel\.(?:L0|L1)\b")
    selected = [
        str(path.relative_to(CLIENT))
        for path in CLIENT.rglob("test_*.py")
        if ".venv" not in path.parts and pattern.search(path.read_text(errors="ignore"))
    ]
    assert ORDINARY in selected
    assert SPECIAL not in selected
    assert not any(path.startswith("paimon_l3/") for path in selected)


@pytest.mark.parametrize("levels,count", [(("L0",), 2), (("L1",), 24), (("L0", "L1"), 26), (("L2",), 131)])
def test_standard_pytest_tag_selection(levels, count):
    result = invoke([ORDINARY, SPECIAL], "--tags", *levels)
    selected = nodeids(result)
    assert len(selected) == count
    assert all(line.startswith(ORDINARY + "::") for line in selected)


def test_l3_option_is_registered_for_standard_milvus_client_path(tmp_path):
    result = invoke([SPECIAL], "--tags", "L3", "--paimon-l3-profile", str(tmp_path / "ci profile.json"), "-n", "0")
    selected = nodeids(result)
    assert len(selected) == 3 and all(line.startswith(SPECIAL + "::") for line in selected)


def test_l3_rejects_parallel_execution_before_any_fixture(tmp_path):
    result = invoke([SPECIAL], "--tags", "L3", "--paimon-l3-profile", str(tmp_path / "ci.json"), "-n", "2")
    assert result.returncode == 4
    assert "requires pytest -n 0" in result.stderr


def test_l3_rejects_mixed_workload_before_any_fixture(tmp_path):
    result = invoke(
        [ORDINARY, SPECIAL], "--tags", "L2", "L3", "--paimon-l3-profile", str(tmp_path / "ci.json"), "-n", "0"
    )
    assert result.returncode == 4, result.stdout + result.stderr
    assert "dedicated CI job" in result.stderr


def test_l3_missing_profile_fails_without_contacting_milvus_or_minio():
    result = invoke([SPECIAL], "--tags", "L3", "--uri", "http://127.0.0.1:1", "-n", "0", collect=False)
    assert result.returncode == 4, result.stdout + result.stderr
    assert "requires --paimon-l3-profile" in result.stderr


def test_serial_ci_entry_preserves_standard_connection_and_profile_arguments(tmp_path):
    fake = tmp_path / "python"
    fake.write_text("#!/usr/bin/env python3\nimport json,sys\nprint(json.dumps(sys.argv[1:]))\n")
    fake.chmod(0o755)
    env = dict(os.environ, PATH=str(tmp_path) + os.pathsep + os.environ["PATH"])
    profile = str(tmp_path / "ci profile.json")
    result = subprocess.run(
        [
            "bash",
            str(ROOT / "tests/scripts/ci_paimon_external_l3.sh"),
            "--host",
            "selected-milvus.tests",
            "--minio_host",
            "selected-minio.tests",
            "--paimon-l3-profile",
            profile,
            "-n",
            "6",
        ],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        timeout=15,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    argv = json.loads(result.stdout)
    assert argv[:2] == ["-m", "pytest"]
    argv = argv[2:]
    assert argv[:6] == [
        "--host",
        "selected-milvus.tests",
        "--minio_host",
        "selected-minio.tests",
        "--paimon-l3-profile",
        profile,
    ]
    assert argv[-1] == SPECIAL
    workers = [argv[index + 1] for index, value in enumerate(argv) if value == "-n"]
    assert workers == ["6", "0"]  # The dedicated stage owns serial execution.
    assert argv[argv.index("--tags") + 1] == "L3"
    assert argv[argv.index("-m") + 1] == "paimon_refresh_serial"
