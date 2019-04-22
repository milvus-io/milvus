#!/usr/local/fbcode/gcc-4.9-glibc-2.20-fb/bin/python2.7

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import argparse
import commands
import subprocess
import sys
import re
import os
import time


#
# Simple logger
#

class Log:

    def __init__(self, filename):
        self.filename = filename
        self.f = open(self.filename, 'w+', 0)

    def caption(self, str):
        line = "\n##### %s #####\n" % str
        if self.f:
            self.f.write("%s \n" % line)
        else:
            print(line)

    def error(self, str):
        data = "\n\n##### ERROR ##### %s" % str
        if self.f:
            self.f.write("%s \n" % data)
        else:
            print(data)

    def log(self, str):
        if self.f:
            self.f.write("%s \n" % str)
        else:
            print(str)

#
# Shell Environment
#


class Env(object):

    def __init__(self, logfile, tests):
        self.tests = tests
        self.log = Log(logfile)

    def shell(self, cmd, path=os.getcwd()):
        if path:
            os.chdir(path)

        self.log.log("==== shell session ===========================")
        self.log.log("%s> %s" % (path, cmd))
        status = subprocess.call("cd %s; %s" % (path, cmd), shell=True,
                                 stdout=self.log.f, stderr=self.log.f)
        self.log.log("status = %s" % status)
        self.log.log("============================================== \n\n")
        return status

    def GetOutput(self, cmd, path=os.getcwd()):
        if path:
            os.chdir(path)

        self.log.log("==== shell session ===========================")
        self.log.log("%s> %s" % (path, cmd))
        status, out = commands.getstatusoutput(cmd)
        self.log.log("status = %s" % status)
        self.log.log("out = %s" % out)
        self.log.log("============================================== \n\n")
        return status, out

#
# Pre-commit checker
#


class PreCommitChecker(Env):

    def __init__(self, args):
        Env.__init__(self, args.logfile, args.tests)
        self.ignore_failure = args.ignore_failure

    #
    #   Get commands for a given job from the determinator file
    #
    def get_commands(self, test):
        status, out = self.GetOutput(
            "RATIO=1 build_tools/rocksdb-lego-determinator %s" % test, ".")
        return status, out

    #
    # Run a specific CI job
    #
    def run_test(self, test):
        self.log.caption("Running test %s locally" % test)

        # get commands for the CI job determinator
        status, cmds = self.get_commands(test)
        if status != 0:
            self.log.error("Error getting commands for test %s" % test)
            return False

        # Parse the JSON to extract the commands to run
        cmds = re.findall("'shell':'([^\']*)'", cmds)

        if len(cmds) == 0:
            self.log.log("No commands found")
            return False

        # Run commands
        for cmd in cmds:
            # Replace J=<..> with the local environment variable
            if "J" in os.environ:
                cmd = cmd.replace("J=1", "J=%s" % os.environ["J"])
                cmd = cmd.replace("make ", "make -j%s " % os.environ["J"])
            # Run the command
            status = self.shell(cmd, ".")
            if status != 0:
                self.log.error("Error running command %s for test %s"
                               % (cmd, test))
                return False

        return True

    #
    # Run specified CI jobs
    #
    def run_tests(self):
        if not self.tests:
            self.log.error("Invalid args. Please provide tests")
            return False

        self.print_separator()
        self.print_row("TEST", "RESULT")
        self.print_separator()

        result = True
        for test in self.tests:
            start_time = time.time()
            self.print_test(test)
            result = self.run_test(test)
            elapsed_min = (time.time() - start_time) / 60
            if not result:
                self.log.error("Error running test %s" % test)
                self.print_result("FAIL (%dm)" % elapsed_min)
                if not self.ignore_failure:
                    return False
                result = False
            else:
                self.print_result("PASS (%dm)" % elapsed_min)

        self.print_separator()
        return result

    #
    # Print a line
    #
    def print_separator(self):
        print("".ljust(60, "-"))

    #
    # Print two colums
    #
    def print_row(self, c0, c1):
        print("%s%s" % (c0.ljust(40), c1.ljust(20)))

    def print_test(self, test):
        print(test.ljust(40), end="")
        sys.stdout.flush()

    def print_result(self, result):
        print(result.ljust(20))

#
# Main
#
parser = argparse.ArgumentParser(description='RocksDB pre-commit checker.')

# --log <logfile>
parser.add_argument('--logfile', default='/tmp/precommit-check.log',
                    help='Log file. Default is /tmp/precommit-check.log')
# --ignore_failure
parser.add_argument('--ignore_failure', action='store_true', default=False,
                    help='Stop when an error occurs')
# <test ....>
parser.add_argument('tests', nargs='+',
                    help='CI test(s) to run. e.g: unit punit asan tsan ubsan')

args = parser.parse_args()
checker = PreCommitChecker(args)

print("Please follow log %s" % checker.log.filename)

if not checker.run_tests():
    print("Error running tests. Please check log file %s"
          % checker.log.filename)
    sys.exit(1)

sys.exit(0)
