"""Generate one validated Paimon fixture per pytest run, shared by local xdist workers."""

import fcntl
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from common.paimon_fixture import FixtureBundle

GENERATOR = Path(__file__).resolve().parents[1] / "scripts/paimon/build_fixtures.py"
GENERATION_TIMEOUT = 900


def _generate(candidate, log):
    command = [sys.executable, str(GENERATOR), "--output", str(candidate)]
    process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    try:
        code = process.wait(timeout=GENERATION_TIMEOUT)
        if code:
            raise subprocess.CalledProcessError(code, command)
    finally:
        # Terminate javac/Java descendants too, including on timeout or interruption.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()


def generate_fixture_bundle(directory):
    """Only dependencies are cached; directory belongs to a fresh pytest run UID."""
    directory.mkdir(parents=True, exist_ok=True)
    status_path = directory / "status.json"
    bundle_path = directory / "fixtures.zip"
    log_path = directory / "generation.log"

    def status(value):
        temporary = status_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(value), encoding="utf8")
        temporary.replace(status_path)

    with (directory / ".generation.lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if status_path.exists():
            previous = json.loads(status_path.read_text())
            if previous["state"] != "ready":
                raise RuntimeError(f"Paimon generation failed or was interrupted: {previous}; log: {log_path}")
            with bundle_path.open("rb") as stream:
                if hashlib.file_digest(stream, "sha256").hexdigest() != previous["sha256"]:
                    raise ValueError("This run's generated Paimon bundle was modified after publication")
            print(
                f"[paimon] sharing current-run fixture {directory.name}, generation_seconds={previous['seconds']:.2f}",
                flush=True,
            )
            return FixtureBundle(bundle_path)

        started = time.monotonic()
        status({"state": "generating"})
        candidate = directory / "candidate.zip"
        print(f"[paimon] generating fresh tables and reader GT for {directory.name}", flush=True)
        try:
            with log_path.open("w", encoding="utf8") as log:
                _generate(candidate, log)
            bundle = FixtureBundle(candidate)
            with candidate.open("rb") as stream:
                digest = hashlib.file_digest(stream, "sha256").hexdigest()
            candidate.replace(bundle_path)
            bundle.path = bundle_path
            seconds = time.monotonic() - started
            status({"state": "ready", "sha256": digest, "seconds": seconds})
            print(
                f"[paimon] generated and validated current-run fixture in {seconds:.2f}s; log: {log_path}", flush=True
            )
            return bundle
        except Exception as error:
            status({"state": "failed", "error": str(error), "seconds": time.monotonic() - started})
            tail = log_path.read_text(errors="replace")[-6000:] if log_path.exists() else ""
            raise RuntimeError(f"Paimon runtime generation failed; log: {log_path}\n{tail}") from error
