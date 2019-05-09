#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

import platform
import re
from itertools import product

from .util import merge_dict
from .test import TestEntry

# Those keys are passed to execution as is.
# Note that there are keys other than these, namely:
# delay: After server is started, client start is delayed for the value
# (seconds).
# timeout: Test timeout after client is started (seconds).
# platforms: Supported platforms. Should match platform.system() value.
# protocols: list of supported protocols
# transports: list of supported transports
# sockets: list of supported sockets
#
# protocols and transports entries can be colon separated "spec:impl" pair
# (e.g. binary:accel) where test is run for any matching "spec" while actual
# argument passed to test executable is "impl".
# Otherwise "spec" is equivalent to "spec:spec" pair.
# (e.g. "binary" is equivalent to "binary:binary" in tests.json)
#
VALID_JSON_KEYS = [
    'name',  # name of the library, typically a language name
    'workdir',  # work directory where command is executed
    'command',  # test command
    'extra_args',  # args appended to command after other args are appended
    'remote_args',  # args added to the other side of the program
    'join_args',  # whether args should be passed as single concatenated string
    'env',  # additional environmental variable
]

DEFAULT_MAX_DELAY = 5
DEFAULT_TIMEOUT = 5


def _collect_testlibs(config, server_match, client_match=[None]):
    """Collects server/client configurations from library configurations"""
    def expand_libs(config):
        for lib in config:
            sv = lib.pop('server', None)
            cl = lib.pop('client', None)
            yield lib, sv, cl

    def yield_testlibs(base_configs, configs, match):
        for base, conf in zip(base_configs, configs):
            if conf:
                if not match or base['name'] in match:
                    platforms = conf.get('platforms') or base.get('platforms')
                    if not platforms or platform.system() in platforms:
                        yield merge_dict(base, conf)

    libs, svs, cls = zip(*expand_libs(config))
    servers = list(yield_testlibs(libs, svs, server_match))
    clients = list(yield_testlibs(libs, cls, client_match))
    return servers, clients


def collect_features(config, match):
    res = list(map(re.compile, match))
    return list(filter(lambda c: any(map(lambda r: r.search(c['name']), res)), config))


def _do_collect_tests(servers, clients):
    def intersection(key, o1, o2):
        """intersection of two collections.
        collections are replaced with sets the first time"""
        def cached_set(o, key):
            v = o[key]
            if not isinstance(v, set):
                v = set(v)
                o[key] = v
            return v
        return cached_set(o1, key) & cached_set(o2, key)

    def intersect_with_spec(key, o1, o2):
        # store as set of (spec, impl) tuple
        def cached_set(o):
            def to_spec_impl_tuples(values):
                for v in values:
                    spec, _, impl = v.partition(':')
                    yield spec, impl or spec
            v = o[key]
            if not isinstance(v, set):
                v = set(to_spec_impl_tuples(set(v)))
                o[key] = v
            return v
        for spec1, impl1 in cached_set(o1):
            for spec2, impl2 in cached_set(o2):
                if spec1 == spec2:
                    name = impl1 if impl1 == impl2 else '%s-%s' % (impl1, impl2)
                    yield name, impl1, impl2

    def maybe_max(key, o1, o2, default):
        """maximum of two if present, otherwise defult value"""
        v1 = o1.get(key)
        v2 = o2.get(key)
        return max(v1, v2) if v1 and v2 else v1 or v2 or default

    def filter_with_validkeys(o):
        ret = {}
        for key in VALID_JSON_KEYS:
            if key in o:
                ret[key] = o[key]
        return ret

    def merge_metadata(o, **ret):
        for key in VALID_JSON_KEYS:
            if key in o:
                ret[key] = o[key]
        return ret

    for sv, cl in product(servers, clients):
        for proto, proto1, proto2 in intersect_with_spec('protocols', sv, cl):
            for trans, trans1, trans2 in intersect_with_spec('transports', sv, cl):
                for sock in intersection('sockets', sv, cl):
                    yield {
                        'server': merge_metadata(sv, **{'protocol': proto1, 'transport': trans1}),
                        'client': merge_metadata(cl, **{'protocol': proto2, 'transport': trans2}),
                        'delay': maybe_max('delay', sv, cl, DEFAULT_MAX_DELAY),
                        'timeout': maybe_max('timeout', sv, cl, DEFAULT_TIMEOUT),
                        'protocol': proto,
                        'transport': trans,
                        'socket': sock
                    }


def _filter_entries(tests, regex):
    if regex:
        return filter(lambda t: re.search(regex, TestEntry.get_name(**t)), tests)
    return tests


def collect_cross_tests(tests_dict, server_match, client_match, regex):
    sv, cl = _collect_testlibs(tests_dict, server_match, client_match)
    return list(_filter_entries(_do_collect_tests(sv, cl), regex))


def collect_feature_tests(tests_dict, features_dict, server_match, feature_match, regex):
    sv, _ = _collect_testlibs(tests_dict, server_match)
    ft = collect_features(features_dict, feature_match)
    return list(_filter_entries(_do_collect_tests(sv, ft), regex))
