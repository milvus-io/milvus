"""
This module keeps commonly used components.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import subprocess
import os
import time

class ColorString:
    """ Generate colorful strings on terminal """
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'

    @staticmethod
    def _make_color_str(text, color):
        return "".join([color, text.encode('utf-8'), ColorString.ENDC])

    @staticmethod
    def ok(text):
        if ColorString.is_disabled:
            return text
        return ColorString._make_color_str(text, ColorString.GREEN)

    @staticmethod
    def info(text):
        if ColorString.is_disabled:
            return text
        return ColorString._make_color_str(text, ColorString.BLUE)

    @staticmethod
    def header(text):
        if ColorString.is_disabled:
            return text
        return ColorString._make_color_str(text, ColorString.HEADER)

    @staticmethod
    def error(text):
        if ColorString.is_disabled:
            return text
        return ColorString._make_color_str(text, ColorString.FAIL)

    @staticmethod
    def warning(text):
        if ColorString.is_disabled:
            return text
        return ColorString._make_color_str(text, ColorString.WARNING)

    is_disabled = False


def run_shell_command(shell_cmd, cmd_dir=None):
    """ Run a single shell command.
        @returns a tuple of shell command return code, stdout, stderr """

    if cmd_dir is not None and not os.path.exists(cmd_dir):
        run_shell_command("mkdir -p %s" % cmd_dir)

    start = time.time()
    print("\t>>> Running: " + shell_cmd)
    p = subprocess.Popen(shell_cmd,
                         shell=True,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         cwd=cmd_dir)
    stdout, stderr = p.communicate()
    end = time.time()

    # Report time if we spent more than 5 minutes executing a command
    execution_time = end - start
    if execution_time > (60 * 5):
        mins = (execution_time / 60)
        secs = (execution_time % 60)
        print("\t>time spent: %d minutes %d seconds" % (mins, secs))


    return p.returncode, stdout, stderr


def run_shell_commands(shell_cmds, cmd_dir=None, verbose=False):
    """ Execute a sequence of shell commands, which is equivalent to
        running `cmd1 && cmd2 && cmd3`
        @returns boolean indication if all commands succeeds.
    """

    if cmd_dir:
        print("\t=== Set current working directory => %s" % cmd_dir)

    for shell_cmd in shell_cmds:
        ret_code, stdout, stderr = run_shell_command(shell_cmd, cmd_dir)
        if stdout:
            if verbose or ret_code != 0:
                print(ColorString.info("stdout: \n"), stdout)
        if stderr:
            # contents in stderr is not necessarily to be error messages.
            if verbose or ret_code != 0:
                print(ColorString.error("stderr: \n"), stderr)
        if ret_code != 0:
            return False

    return True
