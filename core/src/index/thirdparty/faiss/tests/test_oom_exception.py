# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

#! /usr/bin/env python2

import sys
import faiss
import unittest
import resource

class TestOOMException(unittest.TestCase):

    def test_outrageous_alloc(self):
        # Disable test on OSX.
        if sys.platform == "darwin":
            return

        # https://github.com/facebookresearch/faiss/issues/758
        soft_as, hard_as = resource.getrlimit(resource.RLIMIT_AS)
        # make sure that allocing more than 10G will fail
        resource.setrlimit(resource.RLIMIT_AS, (10 * 1024 * 1024, hard_as))
        try:
            x = faiss.IntVector()
            try:
                x.resize(10**11)   # 400 G of RAM
            except MemoryError:
                pass               # good, that's what we expect
            else:
                assert False, "should raise exception"
        finally:
            resource.setrlimit(resource.RLIMIT_AS, (soft_as, hard_as))


if __name__ == '__main__':
    unittest.main()
