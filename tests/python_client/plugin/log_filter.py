"""
Pytest plugin for conditional logging based on test outcome.

This plugin captures logs per test in memory buffers and only persists
detailed logs for failed tests:
- PASSED tests: Only metadata saved (nodeid, duration, timestamp)
- FAILED tests: Full logs + error traceback + metadata

Generates two report formats:
- test_report.json: AI-friendly structured format with complete test data
- test_report.html: Human-readable HTML report with color-coded logs
"""

import os
import logging
import json
from datetime import datetime

import pytest


class LogBuffer:
    """Captures logs per test in memory"""

    def __init__(self, test_nodeid):
        self.test_nodeid = test_nodeid
        self.records = []  # List of LogRecord objects
        self.start_time = None
        self.end_time = None

    def emit(self, record):
        """Add a log record to the buffer - must be thread-safe"""
        # Make a copy of the record to avoid mutation issues
        try:
            self.records.append(record)
        except Exception:
            pass  # Silently ignore errors during capture

    def get_summary(self, report):
        """Extract test name, duration, outcome"""
        duration = report.duration if hasattr(report, 'duration') else 0
        status = report.outcome.upper()
        return f"[{status}] {self.test_nodeid} ({duration:.2f}s)\n"

    def get_full_logs(self, formatter):
        """Format all captured records"""
        formatted_logs = []
        for record in self.records:
            try:
                formatted_logs.append(formatter.format(record))
            except Exception:
                pass  # Skip records that can't be formatted
        return "\n".join(formatted_logs) if formatted_logs else "(No logs captured)"

    def get_structured_logs(self):
        """Get logs structured by level for JSON output"""
        structured = {
            'debug': [],
            'info': [],
            'warning': [],
            'error': [],
            'critical': []
        }

        for record in self.records:
            try:
                level = record.levelname.lower()
                if level in structured:
                    structured[level].append({
                        'message': record.getMessage(),
                        'timestamp': datetime.fromtimestamp(record.created).isoformat(),
                        'location': f"{record.filename}:{record.lineno}",
                        'logger': record.name
                    })
            except Exception:
                pass

        return structured


class ConditionalLogHandler(logging.Handler):
    """Routes logs to per-test buffers"""

    def __init__(self, log_config):
        super().__init__()
        self.current_test_buffer = None
        self.buffers = {}  # test_nodeid -> LogBuffer
        self.log_config = log_config
        self.formatter = logging.Formatter(
            "[%(asctime)s - %(levelname)s - %(name)s]: %(message)s (%(filename)s:%(lineno)s)"
        )

        # Initialize log files
        self.report_json = os.path.join(log_config.log_path, "test_report.json")
        self.report_html = os.path.join(log_config.log_path, "test_report.html")

        # Track test statistics
        self.test_stats = {
            'passed': [],
            'failed': [],
            'skipped': [],
            'start_time': None,
            'end_time': None
        }

        # Worker mode support
        self.worker_id = ''
        self.is_worker = False

    def emit(self, record):
        """Capture log record into current test buffer - NO LOCKS to avoid deadlock"""
        # CRITICAL: No locks here to avoid deadlock with logging system
        try:
            if self.current_test_buffer:
                self.current_test_buffer.emit(record)
        except Exception:
            # Must silently ignore errors to avoid breaking logging system
            pass

    def start_test(self, item):
        """Create new buffer for this test"""
        buffer = LogBuffer(item.nodeid)
        buffer.start_time = datetime.now()
        self.buffers[item.nodeid] = buffer
        self.current_test_buffer = buffer

    def end_test(self, item, report):
        """Process test result and write logs"""
        buffer = self.buffers.get(item.nodeid)
        if buffer:
            buffer.end_time = datetime.now()

            # Track statistics with structured data
            try:
                # Extract test location info
                test_parts = item.nodeid.split("::")
                file_path = test_parts[0] if test_parts else "unknown"
                test_class = test_parts[1] if len(test_parts) > 2 else None
                test_function = test_parts[-1] if test_parts else "unknown"

                if report.passed:
                    self.test_stats['passed'].append({
                        'nodeid': item.nodeid,
                        'file': file_path,
                        'class': test_class,
                        'function': test_function,
                        'duration': report.duration,
                        'timestamp': datetime.now().isoformat()
                    })
                elif report.failed:
                    # Extract error information
                    error_info = self._extract_error_info(report)

                    self.test_stats['failed'].append({
                        'nodeid': item.nodeid,
                        'file': file_path,
                        'class': test_class,
                        'function': test_function,
                        'duration': report.duration,
                        'timestamp': datetime.now().isoformat(),
                        'error': error_info,
                        'logs': buffer.get_structured_logs()
                    })
                elif report.skipped:
                    self.test_stats['skipped'].append({
                        'nodeid': item.nodeid,
                        'file': file_path,
                        'class': test_class,
                        'function': test_function,
                        'duration': report.duration,
                        'timestamp': datetime.now().isoformat(),
                        'reason': str(report.longrepr[2]) if hasattr(report, 'longrepr') and len(report.longrepr) > 2 else 'Unknown'
                    })
            except Exception:
                pass  # Silently ignore write errors

            # Clear buffer to free memory
            if item.nodeid in self.buffers:
                del self.buffers[item.nodeid]

            # Clear current buffer if it matches
            if self.current_test_buffer is buffer:
                self.current_test_buffer = None

    def _extract_error_info(self, report):
        """Extract structured error information from test report"""
        error_info = {
            'type': 'Unknown',
            'message': '',
            'traceback': '',
            'line_number': None,
            'file': None
        }

        try:
            if hasattr(report, 'longreprtext'):
                error_info['traceback'] = report.longreprtext

                # Try to extract error type and message
                lines = report.longreprtext.split('\n')
                for line in lines:
                    if line.startswith('E       '):
                        error_info['message'] = line[8:].strip()
                        # Try to extract error type
                        if ':' in error_info['message']:
                            parts = error_info['message'].split(':', 1)
                            error_info['type'] = parts[0].strip()
                        break

                # Try to extract file and line number from traceback
                for line in lines:
                    if '.py:' in line and '>' in line:
                        parts = line.split('.py:')
                        if len(parts) >= 2:
                            error_info['file'] = parts[0].split()[-1] + '.py'
                            line_num = parts[1].split()[0]
                            try:
                                error_info['line_number'] = int(line_num)
                            except:
                                pass
                        break

            elif hasattr(report, 'longrepr'):
                error_info['traceback'] = str(report.longrepr)
                error_info['message'] = str(report.longrepr)

        except Exception:
            pass

        return error_info

    def _save_worker_data(self, filepath):
        """Save worker's test data to file for later merging"""
        try:
            # All data is already serializable (no report/buffer objects)
            serializable_stats = {
                'passed': self.test_stats['passed'],
                'skipped': self.test_stats['skipped'],
                'failed': self.test_stats['failed'],
                'start_time': self.test_stats['start_time'].isoformat() if self.test_stats['start_time'] else None,
                'end_time': self.test_stats['end_time'].isoformat() if self.test_stats['end_time'] else None
            }

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(serializable_stats, f)
        except Exception as e:
            print(f"Error saving worker data: {e}")

    def _merge_worker_data(self):
        """Merge data from all worker processes"""
        try:
            import glob

            # Find all worker data files
            worker_files = glob.glob(os.path.join(self.log_config.log_path, ".worker_*_data.json"))

            for worker_file in worker_files:
                try:
                    with open(worker_file, 'r', encoding='utf-8') as f:
                        worker_data = json.load(f)

                    # Merge data
                    self.test_stats['passed'].extend(worker_data.get('passed', []))
                    self.test_stats['skipped'].extend(worker_data.get('skipped', []))
                    self.test_stats['failed'].extend(worker_data.get('failed', []))

                    # Delete worker file after merging
                    os.remove(worker_file)
                except Exception as e:
                    print(f"Error merging worker file {worker_file}: {e}")

        except Exception as e:
            print(f"Error in merge_worker_data: {e}")

    def generate_json_report(self):
        """Generate AI-friendly JSON report"""
        try:
            total = len(self.test_stats['passed']) + len(self.test_stats['failed']) + len(self.test_stats['skipped'])
            total_duration = sum(t['duration'] for t in self.test_stats['passed']) + \
                           sum(t['duration'] for t in self.test_stats['failed']) + \
                           sum(t['duration'] for t in self.test_stats['skipped'])

            # Build JSON structure optimized for AI analysis
            report_data = {
                'metadata': {
                    'generated_at': datetime.now().isoformat(),
                    'log_location': self.log_config.log_path,
                    'total_tests': total,
                    'total_duration_seconds': round(total_duration, 2),
                    'start_time': self.test_stats['start_time'].isoformat() if self.test_stats['start_time'] else None,
                    'end_time': self.test_stats['end_time'].isoformat() if self.test_stats['end_time'] else None
                },
                'summary': {
                    'passed': {
                        'count': len(self.test_stats['passed']),
                        'percentage': round(len(self.test_stats['passed']) / total * 100, 1) if total > 0 else 0
                    },
                    'failed': {
                        'count': len(self.test_stats['failed']),
                        'percentage': round(len(self.test_stats['failed']) / total * 100, 1) if total > 0 else 0
                    },
                    'skipped': {
                        'count': len(self.test_stats['skipped']),
                        'percentage': round(len(self.test_stats['skipped']) / total * 100, 1) if total > 0 else 0
                    }
                },
                'tests': {
                    'passed': [
                        {
                            'id': t['nodeid'],
                            'file': t['file'],
                            'class': t['class'],
                            'function': t['function'],
                            'duration': round(t['duration'], 3),
                            'timestamp': t['timestamp']
                        }
                        for t in self.test_stats['passed']
                    ],
                    'failed': [
                        {
                            'id': t['nodeid'],
                            'file': t['file'],
                            'class': t['class'],
                            'function': t['function'],
                            'duration': round(t['duration'], 3),
                            'timestamp': t['timestamp'],
                            'error': {
                                'type': t['error']['type'],
                                'message': t['error']['message'],
                                'file': t['error']['file'],
                                'line': t['error']['line_number'],
                                'traceback': t['error']['traceback']
                            },
                            'logs': {
                                'debug': t['logs']['debug'],
                                'info': t['logs']['info'],
                                'warning': t['logs']['warning'],
                                'error': t['logs']['error'],
                                'critical': t['logs']['critical']
                            }
                        }
                        for t in self.test_stats['failed']
                    ],
                    'skipped': [
                        {
                            'id': t['nodeid'],
                            'file': t['file'],
                            'class': t['class'],
                            'function': t['function'],
                            'duration': round(t['duration'], 3),
                            'timestamp': t['timestamp'],
                            'reason': t['reason']
                        }
                        for t in self.test_stats['skipped']
                    ]
                }
            }

            # Write JSON report with pretty printing
            with open(self.report_json, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, indent=2, ensure_ascii=False)

        except Exception as e:
            print(f"Error generating JSON report: {e}")

    def generate_html_report(self):
        """Generate HTML test report for humans"""
        try:
            total = len(self.test_stats['passed']) + len(self.test_stats['failed']) + len(self.test_stats['skipped'])
            total_duration = sum(t['duration'] for t in self.test_stats['passed']) + \
                           sum(t['duration'] for t in self.test_stats['failed']) + \
                           sum(t['duration'] for t in self.test_stats['skipped'])

            passed_pct = (len(self.test_stats['passed'])/total*100) if total > 0 else 0
            failed_pct = (len(self.test_stats['failed'])/total*100) if total > 0 else 0
            skipped_pct = (len(self.test_stats['skipped'])/total*100) if total > 0 else 0

            with open(self.report_html, 'w', encoding='utf-8') as f:
                # HTML header
                f.write("""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Milvus Python Test Report</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background: #f5f5f5;
        }
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 8px;
            margin-bottom: 20px;
        }
        .summary {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }
        .summary-card {
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .summary-card h3 { margin: 0 0 10px 0; color: #666; font-size: 14px; }
        .summary-card .value { font-size: 32px; font-weight: bold; }
        .passed { color: #10b981; }
        .failed { color: #ef4444; }
        .skipped { color: #f59e0b; }
        .test-section {
            background: white;
            padding: 20px;
            border-radius: 8px;
            margin: 20px 0;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .test-section h2 {
            margin-top: 0;
            border-bottom: 2px solid #e5e7eb;
            padding-bottom: 10px;
        }
        .test-item {
            padding: 10px;
            margin: 5px 0;
            border-left: 4px solid;
            background: #f9fafb;
        }
        .test-item.passed { border-color: #10b981; }
        .test-item.failed { border-color: #ef4444; }
        .test-item.skipped { border-color: #f59e0b; }
        .test-name { font-family: monospace; font-size: 13px; }
        .duration { color: #6b7280; font-size: 12px; }
        .error-box {
            background: #fee2e2;
            border: 1px solid #ef4444;
            padding: 15px;
            margin: 10px 0;
            border-radius: 4px;
            font-family: monospace;
            font-size: 12px;
            white-space: pre-wrap;
        }
        .logs-box {
            background: #f3f4f6;
            border: 1px solid #d1d5db;
            padding: 15px;
            margin: 10px 0;
            border-radius: 4px;
            font-family: monospace;
            font-size: 11px;
            max-height: 400px;
            overflow-y: auto;
        }
        .log-entry { margin: 3px 0; }
        .log-debug { color: #6b7280; }
        .log-info { color: #3b82f6; }
        .log-warning { color: #f59e0b; }
        .log-error { color: #ef4444; }
        .log-critical { color: #dc2626; font-weight: bold; }
        details { margin: 10px 0; }
        summary { cursor: pointer; font-weight: 600; padding: 10px; background: #f9fafb; }
        summary:hover { background: #f3f4f6; }
    </style>
</head>
<body>
    <div class="header">
        <h1>Milvus Python Test Report</h1>
        <p>Generated at: """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + f"""</p>
        <p>Log location: {self.log_config.log_path}</p>
    </div>

    <div class="summary">
        <div class="summary-card">
            <h3>Total Tests</h3>
            <div class="value">{total}</div>
        </div>
        <div class="summary-card">
            <h3>Passed</h3>
            <div class="value passed">{len(self.test_stats['passed'])}</div>
            <div class="duration">{passed_pct:.1f}%</div>
        </div>
        <div class="summary-card">
            <h3>Failed</h3>
            <div class="value failed">{len(self.test_stats['failed'])}</div>
            <div class="duration">{failed_pct:.1f}%</div>
        </div>
        <div class="summary-card">
            <h3>Skipped</h3>
            <div class="value skipped">{len(self.test_stats['skipped'])}</div>
            <div class="duration">{skipped_pct:.1f}%</div>
        </div>
        <div class="summary-card">
            <h3>Duration</h3>
            <div class="value">{total_duration:.2f}s</div>
        </div>
    </div>
""")

                # Failed tests section
                if self.test_stats['failed']:
                    f.write('    <div class="test-section">\n')
                    f.write('        <h2>❌ Failed Tests</h2>\n')
                    for test_info in self.test_stats['failed']:
                        f.write(f'        <details open>\n')
                        f.write(f'            <summary class="test-item failed">\n')
                        f.write(f'                <span class="test-name">{test_info["nodeid"]}</span>\n')
                        f.write(f'                <span class="duration"> ({test_info["duration"]:.2f}s)</span>\n')
                        f.write(f'            </summary>\n')

                        # Error information - use data from test_info['error']
                        error_info = test_info.get('error', {})
                        if error_info:
                            f.write('            <div class="error-box">\n')
                            error_type = error_info.get('type', 'Unknown')
                            error_msg = error_info.get('message', '')
                            error_traceback = error_info.get('traceback', '')

                            # Format error display
                            if error_traceback:
                                error_text = error_traceback.replace('<', '&lt;').replace('>', '&gt;')
                            else:
                                error_text = f"{error_type}: {error_msg}".replace('<', '&lt;').replace('>', '&gt;')

                            f.write(f'{error_text}\n')
                            f.write('            </div>\n')

                        # Captured logs - use data from test_info['logs']
                        logs = test_info.get('logs', {})
                        has_logs = any(logs.get(level, []) for level in ['debug', 'info', 'warning', 'error', 'critical'])

                        if has_logs:
                            f.write('            <div class="logs-box">\n')
                            f.write('                <strong>Captured Logs:</strong>\n')

                            for level in ['debug', 'info', 'warning', 'error', 'critical']:
                                for log_entry in logs.get(level, []):
                                    msg = log_entry['message'].replace('<', '&lt;').replace('>', '&gt;')
                                    ts = log_entry['timestamp'].split('T')[1].split('.')[0]
                                    loc = log_entry['location']
                                    f.write(f'                <div class="log-entry log-{level}">')
                                    f.write(f'[{ts} - {level.upper()}] {msg} ({loc})</div>\n')

                            f.write('            </div>\n')

                        f.write('        </details>\n')
                    f.write('    </div>\n')

                # Passed tests section
                if self.test_stats['passed']:
                    f.write('    <div class="test-section">\n')
                    f.write('        <h2>✅ Passed Tests</h2>\n')
                    for test in self.test_stats['passed']:
                        f.write(f'        <div class="test-item passed">\n')
                        f.write(f'            <span class="test-name">{test["nodeid"]}</span>\n')
                        f.write(f'            <span class="duration"> ({test["duration"]:.2f}s)</span>\n')
                        f.write(f'        </div>\n')
                    f.write('    </div>\n')

                # Skipped tests section
                if self.test_stats['skipped']:
                    f.write('    <div class="test-section">\n')
                    f.write('        <h2>⊘ Skipped Tests</h2>\n')
                    for test in self.test_stats['skipped']:
                        f.write(f'        <div class="test-item skipped">\n')
                        f.write(f'            <span class="test-name">{test["nodeid"]}</span>\n')
                        f.write(f'            <span class="duration"> ({test["duration"]:.2f}s)</span>\n')
                        if 'reason' in test:
                            reason = test['reason'].replace('<', '&lt;').replace('>', '&gt;')
                            f.write(f'            <div style="margin-top:5px;color:#6b7280;font-size:12px;">Reason: {reason}</div>\n')
                        f.write(f'        </div>\n')
                    f.write('    </div>\n')

                f.write("""
</body>
</html>
""")

        except Exception as e:
            print(f"Error generating HTML report: {e}")

    def generate_report(self):
        """Generate unified test report (JSON and HTML)"""
        # Generate JSON report for AI
        self.generate_json_report()

        # Generate HTML report for humans
        self.generate_html_report()


# Global handler instance
_conditional_handler = None


def pytest_configure(config):
    """Initialize the conditional log handler"""
    global _conditional_handler

    # Only initialize if not already done
    if _conditional_handler is None:
        try:
            from config.log_config import log_config
            _conditional_handler = ConditionalLogHandler(log_config)
            _conditional_handler.setLevel(logging.DEBUG)

            # Record start time
            _conditional_handler.test_stats['start_time'] = datetime.now()

            # Detect if running in xdist worker mode
            worker_id = os.environ.get('PYTEST_XDIST_WORKER', '')
            _conditional_handler.worker_id = worker_id
            _conditional_handler.is_worker = bool(worker_id)

            # Pre-create log files
            for log_file in [_conditional_handler.report_json,
                           _conditional_handler.report_html]:
                if not os.path.exists(log_file):
                    try:
                        with open(log_file, 'w', encoding='utf-8') as f:
                            pass  # Create empty file
                    except Exception:
                        pass  # Ignore errors

            # Add handler to root logger
            root_logger = logging.getLogger()
            root_logger.addHandler(_conditional_handler)
        except Exception:
            # If plugin initialization fails, don't break pytest
            pass


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_setup(item):
    """Hook before test setup"""
    global _conditional_handler
    if _conditional_handler:
        try:
            _conditional_handler.start_test(item)
        except Exception:
            pass  # Don't break test execution
    yield


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Hook after each test phase (setup, call, teardown)"""
    outcome = yield
    report = outcome.get_result()

    # Only process after the main test execution (call phase)
    if report.when == "call":
        global _conditional_handler
        if _conditional_handler:
            try:
                _conditional_handler.end_test(item, report)
            except Exception:
                pass  # Don't break test execution


def pytest_sessionfinish(session, exitstatus):
    """Hook at the end of test session"""
    global _conditional_handler
    if _conditional_handler:
        try:
            # Record end time
            _conditional_handler.test_stats['end_time'] = datetime.now()

            if _conditional_handler.is_worker:
                # Worker mode: save data to temporary file
                worker_data_file = os.path.join(
                    _conditional_handler.log_config.log_path,
                    f".worker_{_conditional_handler.worker_id}_data.json"
                )
                _conditional_handler._save_worker_data(worker_data_file)
            else:
                # Main process or single process mode
                # Collect data from workers if any
                _conditional_handler._merge_worker_data()

                # Generate unified report
                _conditional_handler.generate_report()

            # Clear any remaining references
            _conditional_handler.current_test_buffer = None
        except Exception as e:
            print(f"Error in pytest_sessionfinish: {e}")
