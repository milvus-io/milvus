#!/usr/bin/env python3
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
from __future__ import print_function
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest


class TestStalenessCheck(unittest.TestCase):

    CURRENT_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
    THRIFT_EXECUTABLE_PATH = None
    SINGLE_THRIFT_FILE_PATH = os.path.join(CURRENT_DIR_PATH, "Single.thrift")
    INCLUDING_THRIFT_FILE_PATH = os.path.join(CURRENT_DIR_PATH, "Including.thrift")
    INCLUDED_THRIFT_FILE_PATH = os.path.join(CURRENT_DIR_PATH, "Included.thrift")

    def test_staleness_check_of_single_thrift_file_without_changed_output(self):
        temp_dir = tempfile.mkdtemp(dir=TestStalenessCheck.CURRENT_DIR_PATH)

        command = [TestStalenessCheck.THRIFT_EXECUTABLE_PATH, "-gen", "cpp", "-o", temp_dir]
        command += [TestStalenessCheck.SINGLE_THRIFT_FILE_PATH]
        subprocess.call(command)

        used_file_path = os.path.join(temp_dir, "gen-cpp", "Single_constants.cpp")

        first_modification_time = os.path.getmtime(os.path.join(used_file_path))

        time.sleep(0.1)

        subprocess.call(command)

        second_modification_time = os.path.getmtime(used_file_path)

        self.assertEqual(second_modification_time, first_modification_time)

        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_staleness_check_of_single_thrift_file_with_changed_output(self):
        temp_dir = tempfile.mkdtemp(dir=TestStalenessCheck.CURRENT_DIR_PATH)

        command = [TestStalenessCheck.THRIFT_EXECUTABLE_PATH, "-gen", "cpp", "-o", temp_dir]
        command += [TestStalenessCheck.SINGLE_THRIFT_FILE_PATH]
        subprocess.call(command)

        used_file_path = os.path.join(temp_dir, "gen-cpp", "Single_constants.cpp")

        first_modification_time = os.path.getmtime(os.path.join(used_file_path))
        used_file = open(used_file_path, "r")
        first_contents = used_file.read()
        used_file.close()

        used_file = open(used_file_path, "a")
        used_file.write("\n/* This is a comment */\n")
        used_file.close()

        time.sleep(0.1)

        subprocess.call(command)

        second_modification_time = os.path.getmtime(used_file_path)
        used_file = open(used_file_path, "r")
        second_contents = used_file.read()
        used_file.close()

        self.assertGreater(second_modification_time, first_modification_time)
        self.assertEqual(first_contents, second_contents)

        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_staleness_check_of_included_file(self):
        temp_dir = tempfile.mkdtemp(dir=TestStalenessCheck.CURRENT_DIR_PATH)

        temp_included_file_path = os.path.join(temp_dir, "Included.thrift")
        temp_including_file_path = os.path.join(temp_dir, "Including.thrift")

        shutil.copy2(TestStalenessCheck.INCLUDED_THRIFT_FILE_PATH, temp_included_file_path)
        shutil.copy2(TestStalenessCheck.INCLUDING_THRIFT_FILE_PATH, temp_including_file_path)

        command = [TestStalenessCheck.THRIFT_EXECUTABLE_PATH, "-gen", "cpp", "-recurse", "-o", temp_dir]
        command += [temp_including_file_path]

        subprocess.call(command)

        included_constants_cpp_file_path = os.path.join(temp_dir, "gen-cpp", "Included_constants.cpp")
        including_constants_cpp_file_path = os.path.join(temp_dir, "gen-cpp", "Including_constants.cpp")

        included_constants_cpp_first_modification_time = os.path.getmtime(included_constants_cpp_file_path)
        including_constants_cpp_first_modification_time = os.path.getmtime(including_constants_cpp_file_path)

        temp_included_file = open(temp_included_file_path, "a")
        temp_included_file.write("\nconst i32 an_integer = 42\n")
        temp_included_file.close()

        time.sleep(0.1)

        subprocess.call(command)

        included_constants_cpp_second_modification_time = os.path.getmtime(included_constants_cpp_file_path)
        including_constants_cpp_second_modification_time = os.path.getmtime(including_constants_cpp_file_path)

        self.assertGreater(
            included_constants_cpp_second_modification_time, included_constants_cpp_first_modification_time)
        self.assertEqual(
            including_constants_cpp_first_modification_time, including_constants_cpp_second_modification_time)

        shutil.rmtree(temp_dir, ignore_errors=True)


def suite():
    suite = unittest.TestSuite()
    loader = unittest.TestLoader()
    suite.addTest(loader.loadTestsFromTestCase(TestStalenessCheck))
    return suite


if __name__ == "__main__":
    # The path of Thrift compiler  is  passed as an argument to the test script.
    # Remove it to not confuse the unit testing framework
    TestStalenessCheck.THRIFT_EXECUTABLE_PATH = sys.argv[-1]
    del sys.argv[-1]
    unittest.main(defaultTest="suite", testRunner=unittest.TextTestRunner(verbosity=2))
