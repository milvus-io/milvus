"""Focused T12/T08/T13/T14 evidence validation on an immutable existing image."""
import json
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import xml.etree.ElementTree as ET

from ci_functional_regression import pytest_phase, authorization_config
from running_requests_topology import ProxyAuthorizationTopology


def main():
    env = dict(os.environ)
    artifacts = Path(env['RR_ARTIFACTS']).resolve()
    artifacts.mkdir(parents=True, exist_ok=True)
    topology = ProxyAuthorizationTopology(shared=True)
    results = {}
    with TemporaryDirectory(prefix='rr-followup-') as work, topology.endpoints(require_auth_config=False) as endpoints:
        (env['PROXY_A_URI'], a), (env['PROXY_B_URI'], b) = endpoints
        env.update(PROXY_A_ID=str(a), PROXY_B_ID=str(b), MILVUS_L0_BASELINE=str(Path(work) / 'baseline.npz'))
        subprocess.run([sys.executable, 'setup_running_requests_l0.py'], cwd=Path(__file__).parent, env=env, check=True)
        trace = dict(env, MILVUS_T12_HYBRID_TRACE='1', MILVUS_L0_ROWS='80000', MILVUS_L1_HYBRID_NQ='4096')
        selections = {
            't12-followup': ['test_running_requests_t12_internal_tasks.py::test_t12_cancel_hybrid_with_trace'] + [
                f'test_running_requests_t10_t12_transport_parity.py::test_t12_transport_cancel_hybrid_with_trace[{language}]'
                for language in ('java', 'go', 'rest')],
            't13-followup': ['test_running_requests_t13_extended.py::test_t13_struct_cancel[struct-query]'] + [
                f'test_running_requests_t13_extended_management.py::test_t13_manage_struct_array[{language}-query]'
                for language in ('java', 'go', 'rest')],
            't14-followup': ['test_running_requests_t14_long_execution.py::test_t14_compute_cancellation_log_evidence'] + [
                f'test_running_requests_t14_transport_parity.py::test_t14_transport_compute_log[{language}]'
                for language in ('java', 'go', 'rest')],
        }
        selected = set(env.get('RR_FOLLOWUP_PHASES', ','.join([*selections, 'auth-followup'])).split(','))
        assert selected <= set(selections) | {'auth-followup'} and selected, selected
        for phase, selection in selections.items():
            if phase not in selected:
                continue
            results[phase] = pytest_phase(artifacts, phase, trace if phase == 't12-followup' else env, *selection)
            cases = list(ET.parse(artifacts / f'{phase}.xml').iter('testcase'))
            if len(cases) != 4 or any(case.find('skipped') is not None for case in cases):
                results[phase] = 1
        if 'auth-followup' in selected:
            name, key = authorization_config(topology)
            env.update(MILVUS_L1_AUTH_CONFIGMAP=name, MILVUS_L1_AUTH_CONFIG_KEY=key)
            results['auth-followup'] = pytest_phase(artifacts, 'auth-followup', env,
                'test_running_requests_t08_rbac.py::test_t08_auth_disabled_smoke',
                'test_running_requests_t08_rbac.py::test_t08_auth_disabled_smoke_native')
            cases = list(ET.parse(artifacts / 'auth-followup.xml').iter('testcase'))
            if len(cases) != 3 or any(case.find('skipped') is not None for case in cases):
                results['auth-followup'] = 1
    (artifacts / 'exit-codes.json').write_text(json.dumps(results, indent=2))
    return int(any(results.values()))


if __name__ == '__main__':
    raise SystemExit(main())
