"""Server evidence helpers; no Milvus or SDK dependency."""
import json
import re
import subprocess
import tempfile
from contextlib import contextmanager, ExitStack
from pathlib import Path


def hybrid_branch_evidence(spans, starts):
    """Find actual branch RPCs by schedule ancestry, not a fixed RPC count."""
    by_id = {}
    for span in spans:
        identity = span['SpanContext']['SpanID']
        if identity in by_id:
            assert by_id[identity] == span, f'Conflicting span exports: {identity}'
        by_id[identity] = span
    expected = {identity for _, identity in starts}
    schedules = {key: span for key, span in by_id.items() if span['Name'] == 'schedule'}
    assert len(expected) == 2 and set(schedules) == expected, (expected, set(schedules))
    views = {key: span for key, span in by_id.items()
             if span['Name'].endswith(('/SearchOnView', '/SearchOnViewStream'))}
    owners, covered = {}, set()
    for branch in expected:
        parent = schedules[branch].get('Parent', {}).get('SpanID')
        visited = {branch}
        nearest = None
        while parent in by_id:
            assert parent not in visited, f'Span ancestry cycle: {parent}'
            visited.add(parent)
            if parent in views:
                covered.add(parent)
                if nearest is None:
                    nearest = parent
            parent = by_id[parent].get('Parent', {}).get('SpanID')
        assert nearest is not None, f'No Search RPC ancestor for schedule {branch}'
        owners.setdefault(nearest, set()).add(branch)
    assert set(views) == covered, f'Unrelated Search RPCs: {set(views) - covered}'
    for owner, children in owners.items():
        status = views[owner]['Status']
        assert status['Code'] == 'Error' and re.search(r'cancel(?:l)?ed', status['Description'], re.I), status
    return {key: views[key] for key in owners}, expected


def segment_identity(line):
    segment = re.search(r'\[segmentID=(\d+)\]', line)
    span = re.search(r'\[spanID=([0-9a-f]+)\]', line)
    return (segment[1], span[1]) if segment and span else None


@contextmanager
def live_server_logs(sources, *, local, kube, since):
    """Follow each source once; polling never waits on repeated kubectl logs calls."""
    with tempfile.TemporaryDirectory(prefix='rr-evidence-') as directory, ExitStack() as stack:
        streams = {}
        for component, pods in sources.items():
            streams[component] = []
            for index, pod in enumerate(pods):
                path = Path(directory) / f'{component}-{index}.log'
                error = path.with_suffix('.stderr')
                output = stack.enter_context(path.open('w'))
                stderr = stack.enter_context(error.open('w'))
                command = (['docker', 'logs', '--follow', '--since', since, pod] if local else
                           [*kube, 'logs', '--follow', pod, '-c', component, '--since-time=' + since])
                process = subprocess.Popen(command, stdout=output,
                                           stderr=subprocess.STDOUT if local else stderr, text=True)
                def stop(process=process):
                    if process.poll() is None:
                        process.terminate()
                        try:
                            process.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait(timeout=5)
                stack.callback(stop)
                streams[component].append((pod, path, error, process))

        def read(component):
            lines = []
            for pod, path, error, process in streams[component]:
                assert process.poll() is None, f'Log follower exited: source={pod} rc={process.returncode}; evidence unavailable'
                content = path.read_text(errors='replace')
                # Do not parse a JSON span until the exporter finishes the line.
                complete = content[:content.rfind('\n') + 1]
                lines.extend(f'[{pod}] {line}' for line in complete.splitlines())
            return lines
        yield read
