from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import targets_cfg

def pretty_list(lst, indent=8):
    if lst is None or len(lst) == 0:
        return ""

    if len(lst) == 1:
        return "\"%s\"" % lst[0]

    separator = "\",\n%s\"" % (" " * indent)
    res = separator.join(sorted(lst))
    res = "\n" + (" " * indent) + "\"" + res + "\",\n" + (" " * (indent - 4))
    return res


class TARGETSBuilder:
    def __init__(self, path):
        self.path = path
        self.targets_file = open(path, 'w')
        self.targets_file.write(targets_cfg.rocksdb_target_header)
        self.total_lib = 0
        self.total_bin = 0
        self.total_test = 0
        self.tests_cfg = ""

    def __del__(self):
        self.targets_file.close()

    def add_library(self, name, srcs, deps=None, headers=None):
        headers_attr_prefix = ""
        if headers is None:
            headers_attr_prefix = "auto_"
            headers = "AutoHeaders.RECURSIVE_GLOB"
        self.targets_file.write(targets_cfg.library_template.format(
            name=name,
            srcs=pretty_list(srcs),
            headers_attr_prefix=headers_attr_prefix,
            headers=headers,
            deps=pretty_list(deps)))
        self.total_lib = self.total_lib + 1

    def add_binary(self, name, srcs, deps=None):
        self.targets_file.write(targets_cfg.binary_template % (
            name,
            pretty_list(srcs),
            pretty_list(deps)))
        self.total_bin = self.total_bin + 1

    def register_test(self, test_name, src, is_parallel):
        exec_mode = "serial"
        if is_parallel:
            exec_mode = "parallel"
        self.tests_cfg += targets_cfg.test_cfg_template % (
            test_name,
            str(src),
            str(exec_mode))

        self.total_test = self.total_test + 1

    def flush_tests(self):
        self.targets_file.write(targets_cfg.unittests_template % self.tests_cfg)
        self.tests_cfg = ""
